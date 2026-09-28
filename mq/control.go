package mq

import (
	"errors"
	"time"
)

// KeyState 描述一个（消费组, 路由键）的当前运行态。
type KeyState string

const (
	// StateDrained 表示该键没有未完成消息（全部已确认或已死信）。
	StateDrained KeyState = "drained"
	// StatePaused 表示该键被暂停投递（暂停期间不投递新消息与到期重试）。
	StatePaused KeyState = "paused"
	// StateProcessing 表示队头消息正在处理器中（在途，等待裁决）。
	StateProcessing KeyState = "processing"
	// StateWaitingRetry 表示队头消息处于延期重试等待中，尚未到期。
	StateWaitingRetry KeyState = "waiting_retry"
	// StateReady 表示队头消息已可投递，正在等待消费者额度或归属评估。
	StateReady KeyState = "ready"
	// StateNoConsumer 表示该键所在消费组当前没有可用消费者。
	StateNoConsumer KeyState = "no_consumer"
)

// WaitReason 区分“没往下走”的具体原因。
type WaitReason string

const (
	// ReasonNone 表示没有等待（已清空或正在处理）。
	ReasonNone WaitReason = "none"
	// ReasonPaused 表示因显式暂停而等待。
	ReasonPaused WaitReason = "paused"
	// ReasonRetryDelay 表示队头消息处于延期重试，等待到期。
	ReasonRetryDelay WaitReason = "retry_delay"
	// ReasonInFlight 表示队头消息在途，等待处理器裁决。
	ReasonInFlight WaitReason = "in_flight"
	// ReasonNoConsumer 表示消费组内没有可用消费者。
	ReasonNoConsumer WaitReason = "no_consumer"
)

// KeySnapshot 是一个（消费组, 路由键）在读取时刻的运行态快照。
// 快照是只读副本，读取不会打断正在进行的消费。
type KeySnapshot struct {
	Group      GroupName
	Key        RoutingKey
	Exists     bool       // 该 (group, key) 是否有任何消息记录
	Paused     bool       // 是否处于暂停状态
	State      KeyState   // 当前运行态
	WaitReason WaitReason // 未推进的原因（State 可推进时为 ReasonNone）

	// 最早未完成（未确认且未死信）消息的信息；无未完成消息时 HeadSeq 为 0。
	HeadID        MessageID
	HeadSeq       int64
	HeadAttempts  int       // 已发生的投递尝试次数
	HeadNotBefore time.Time // 预计下次可投递时间（零值表示立即可投递）

	Backlog int // 队头之后尚未完成的消息条数（不含队头）
}

// Pause 暂停指定消费组对某路由键的投递：新消息与到期重试都继续排队，
// 不交给处理器；已在途的消息允许跑完并落盘裁决。暂停状态持久化，
// 跨重启保持，直到显式 Resume。重复暂停是幂等的。
func (b *Broker) Pause(group GroupName, key RoutingKey) error {
	if group == "" || key == "" {
		return errors.New("mq: group and key are required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return errors.New("mq: broker closed")
	}
	g := b.ensureGroup(group)
	if g.paused[key] {
		return nil // 幂等：已暂停
	}
	ev := pauseEvent{Group: group, Key: key, Paused: true}
	if _, err := b.w.Append(kindPause, mustJSON(&ev)); err != nil {
		return err
	}
	g.paused[key] = true
	b.log.Printf("PAUSE group=%s key=%s", group, key)
	// 唤醒已存在的 runner 重新评估（取消重试计时、进入暂停等待）；
	// 不为此创建新 runner——暂停期间没有可投递的工作。
	if r := g.runners[key]; r != nil {
		r.cond.Signal()
	}
	return nil
}

// Resume 恢复指定消费组对某路由键的投递：从该键最早未完成的消息继续，
// 严格保持键内顺序。重复恢复是幂等的。
func (b *Broker) Resume(group GroupName, key RoutingKey) error {
	if group == "" || key == "" {
		return errors.New("mq: group and key are required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return errors.New("mq: broker closed")
	}
	g := b.groups[group]
	if g == nil || !g.paused[key] {
		return nil // 幂等：未暂停
	}
	ev := pauseEvent{Group: group, Key: key, Paused: false}
	if _, err := b.w.Append(kindPause, mustJSON(&ev)); err != nil {
		return err
	}
	delete(g.paused, key)
	b.log.Printf("RESUME group=%s key=%s", group, key)
	// 唤醒（必要时创建）runner，从最早未完成消息继续投递。
	b.wakeLocked(g, key)
	return nil
}

// KeyStatus 返回指定（消费组, 路由键）的运行态快照。
// 未完成的定义是“未确认且未死信”；快照绝不会把未成功消息显示为已处理。
// 读取只在持锁瞬间拷贝状态，不会打断正在进行的消费。
//
// 语义边界：快照反映的是 broker 的已提交状态，而不是业务处理器的进度。
// 处理器返回（业务生效）与 ACK 经 WAL fsync 落盘之间存在窗口，该窗口内
// 队头状态仍为 processing；只有 ACK/死信落盘、内存状态进入终态后才会
// 报告 drained。需要“已可靠完成”判据的调用方应轮询 State==StateDrained，
// 而不是以处理器返回来判定。
func (b *Broker) KeyStatus(group GroupName, key RoutingKey) KeySnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.keyStatusLocked(group, key, time.Now())
}

// keyStatusLocked 是 KeyStatus 的持锁版本，now 由调用方在临界区内给定，
// 保证“到期判定”与状态拷贝基于同一时刻。调用方必须持有 b.mu。
func (b *Broker) keyStatusLocked(group GroupName, key RoutingKey, now time.Time) KeySnapshot {
	snap := KeySnapshot{Group: group, Key: key}
	g := b.groups[group]
	if g == nil {
		return snap
	}
	snap.Paused = g.paused[key]
	msgs, known := g.keyMsgs[key]
	if !known {
		// 从未有消息记录；若该键被暂停，仍报告暂停态。
		if snap.Paused {
			snap.State = StatePaused
			snap.WaitReason = ReasonPaused
		}
		return snap
	}
	// 注意：全部终态后队列可能被裁减为空切片，但键仍然存在，
	// 此时应报告 drained 而非“不存在”。
	snap.Exists = true

	// 队头 = 最早未完成（未确认且未死信）的消息。
	var head *msgState
	backlog := 0
	for _, ms := range msgs {
		if ms.status == stAcked || ms.status == stDead {
			continue
		}
		if head == nil {
			head = ms
		} else {
			backlog++
		}
	}
	if head == nil {
		if snap.Paused {
			// 暂停且无未完成消息：暂停是更 actionable 的状态。
			snap.State = StatePaused
			snap.WaitReason = ReasonPaused
		} else {
			snap.State = StateDrained
			snap.WaitReason = ReasonNone
		}
		return snap
	}
	snap.HeadID = head.id
	snap.HeadSeq = head.seq
	snap.HeadAttempts = head.attempts
	snap.Backlog = backlog

	switch {
	case snap.Paused:
		// 暂停优先报告：这是“没往下走”的直接原因。
		snap.State = StatePaused
		snap.WaitReason = ReasonPaused
		if head.status == stPending && head.notBefore.After(now) {
			snap.HeadNotBefore = head.notBefore
		}
	case head.status == stInflight:
		snap.State = StateProcessing
		snap.WaitReason = ReasonInFlight
	case head.status == stPending && head.notBefore.After(now):
		snap.State = StateWaitingRetry
		snap.WaitReason = ReasonRetryDelay
		snap.HeadNotBefore = head.notBefore
	case g.ownerOf(key) == nil:
		snap.State = StateNoConsumer
		snap.WaitReason = ReasonNoConsumer
	default:
		snap.State = StateReady
		snap.WaitReason = ReasonNone
	}
	return snap
}
