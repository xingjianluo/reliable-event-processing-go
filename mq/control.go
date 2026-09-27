package mq

import (
	"errors"
	"time"
)

// KeyState 描述一个（消费组, 路由键）的当前运行态。
type KeyState string

const (
	// StateConsuming 正常消费：有未完成消息且立即可投递（或正在等待唤醒）。
	StateConsuming KeyState = "consuming"
	// StatePaused 已暂停：新消息与到期重试都排队等待，不交给处理器。
	StatePaused KeyState = "paused"
	// StateInflight 处理中：队头消息已投递给处理器、等待裁决。
	StateInflight KeyState = "inflight"
	// StateRetryWait 等待重试：队头消息处于延期重试，尚未到期。
	StateRetryWait KeyState = "retry_wait"
	// StateDrained 已清空：该键没有未完成的消息。
	StateDrained KeyState = "drained"
)

// WaitReason 区分“没往下走”的具体原因。
type WaitReason string

const (
	// ReasonNone 无等待：正在正常流转或已清空。
	ReasonNone WaitReason = "none"
	// ReasonPaused 因显式暂停而等待。
	ReasonPaused WaitReason = "paused"
	// ReasonRetryBackoff 因队头消息延期重试未到期而等待。
	ReasonRetryBackoff WaitReason = "retry_backoff"
	// ReasonInFlight 因队头消息正在处理中而等待。
	ReasonInFlight WaitReason = "in_flight"
	// ReasonNoConsumer 因消费组内没有可用消费者而等待。
	ReasonNoConsumer WaitReason = "no_consumer"
)

// HeadInfo 描述最早一条未完成消息的排障信息。
type HeadInfo struct {
	ID MessageID
	// Seq 是该消息在路由键内的序号。
	Seq int64
	// Attempts 是已发生的投递尝试次数。
	Attempts int
	// NextDeliveryAt 是预计下次可投递时间（重试到期时刻）；
	// 零值表示立即可投递（或未安排重试）。
	NextDeliveryAt time.Time
	// LastError 是最近一次失败描述（无则为空）。
	LastError string
}

// KeyStatus 是一个（消费组, 路由键）的运行态快照。
// 快照只包含尚未成功的消息；已确认/死信的消息不会计入积压。
type KeyStatus struct {
	Group GroupName
	Key   RoutingKey
	// Paused 表示该键当前处于显式暂停状态（跨重启保留）。
	Paused bool
	// State 是当前运行态。
	State KeyState
	// Reason 是当前等待原因（State 的辅助判定）。
	Reason WaitReason
	// Head 是最早未完成消息的信息；无未完成消息时为 nil。
	Head *HeadInfo
	// Backlog 是该键尚未成功（pending/inflight）的消息条数。
	Backlog int
}

// Pause 暂停指定（消费组, 路由键）的投递：新消息与到期重试继续排队，
// 不交给处理器；已在途的消息允许跑完并落盘裁决。暂停状态持久化，
// 重启后仍保持暂停，直到显式 Resume。重复暂停是幂等的。
func (b *Broker) Pause(group GroupName, key RoutingKey) error {
	return b.setPaused(group, key, true)
}

// Resume 恢复指定（消费组, 路由键）的投递：从该键最早未完成的消息
// 继续按序处理。重复恢复是幂等的。
func (b *Broker) Resume(group GroupName, key RoutingKey) error {
	return b.setPaused(group, key, false)
}

// setPaused 持久化并应用暂停/恢复状态，然后唤醒对应消费循环重新评估。
func (b *Broker) setPaused(group GroupName, key RoutingKey, paused bool) error {
	if group == "" || key == "" {
		return errors.New("mq: group and key are required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return errors.New("mq: broker closed")
	}
	g := b.ensureGroup(group)
	if g.paused[key] == paused {
		return nil // 幂等：状态未变化时不重复落盘
	}
	ev := pauseEvent{Group: group, Key: key, Paused: paused}
	if _, err := b.w.Append(kindPause, mustJSON(&ev)); err != nil {
		return err
	}
	g.paused[key] = paused
	if paused {
		b.log.Printf("PAUSE group=%s key=%s", group, key)
	} else {
		b.log.Printf("RESUME group=%s key=%s", group, key)
	}
	// 唤醒该键的消费循环：恢复时立即续投最早未完成消息；
	// 暂停时让循环停止安排重试计时。
	b.wakeLocked(g, key)
	return nil
}

// KeyStatus 读取指定（消费组, 路由键）的运行态快照。
// 读取只短暂持锁，不打断正在进行的消费。
// 快照只统计尚未成功的消息：已确认/死信的消息不会计入积压，
// 也不会出现在 Head 中。
func (b *Broker) KeyStatus(group GroupName, key RoutingKey) KeyStatus {
	b.mu.Lock()
	defer b.mu.Unlock()
	st := KeyStatus{Group: group, Key: key, State: StateDrained, Reason: ReasonNone}
	g := b.groups[group]
	if g == nil {
		return st
	}
	st.Paused = g.paused[key]
	now := time.Now()
	var head *msgState
	for _, ms := range g.keyMsgs[key] {
		if ms.status == stAcked || ms.status == stDead {
			continue
		}
		st.Backlog++
		if head == nil {
			head = ms
		}
	}
	if head == nil {
		return st // 已清空
	}
	st.Head = &HeadInfo{
		ID:        head.id,
		Seq:       head.seq,
		Attempts:  head.attempts,
		LastError: head.errMsg,
	}
	if head.notBefore.After(now) {
		st.Head.NextDeliveryAt = head.notBefore
	}
	switch {
	case st.Paused:
		st.State = StatePaused
		st.Reason = ReasonPaused
	case head.status == stInflight:
		st.State = StateInflight
		st.Reason = ReasonInFlight
	case head.notBefore.After(now):
		st.State = StateRetryWait
		st.Reason = ReasonRetryBackoff
	case g.ownerOf(key) == nil:
		// 有立即可投递消息，但组内没有可用消费者。
		st.State = StateConsuming
		st.Reason = ReasonNoConsumer
	default:
		st.State = StateConsuming
		st.Reason = ReasonNone
	}
	return st
}
