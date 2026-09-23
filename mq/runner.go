package mq

import (
	"context"
	"time"
)

// runner 负责单个（消费组, 路由键）的严格有序消费循环。
// 同一时刻该路由键最多只有一条消息处于 inflight，因此同键严格有序；
// 不同路由键拥有独立 runner，可并行消费。
//
// 归属（owner）只决定“下一条”消息由哪个消费者处理器执行；
// 已经在途的消息允许原处理器跑完并落盘裁决，避免无谓重投。
//
// 唤醒模型：每 runner 一个容量 1 的 wake channel + pendingWake 标志，
// 保证任意多次状态变更至少有一个待处理唤醒（不丢信号、也不溢出）。
type runner struct {
	b   *Broker
	g   *groupState
	key string

	wake        chan struct{}
	pendingWake bool

	inflight        *msgState
	decisionCh      chan Decision
	cancelDelivery  context.CancelFunc
	sem             chan struct{}
	handlerConsumer string
}

func (b *Broker) ensureRunnerLocked(g *groupState, key string) {
	if _, ok := g.runners[key]; ok {
		return
	}
	r := &runner{
		b:    b,
		g:    g,
		key:  key,
		wake: make(chan struct{}, 1),
	}
	g.runners[key] = r
	b.wg.Add(1)
	go r.run()
}

// wakeLocked 标记一次待处理唤醒并非阻塞发送信号。调用方持有 b.mu。
func (b *Broker) wakeLocked(g *groupState, key string) {
	r := g.runners[key]
	if r == nil {
		b.ensureRunnerLocked(g, key)
		r = g.runners[key]
	}
	r.signalLocked()
}

func (r *runner) signalLocked() {
	if r.pendingWake {
		return
	}
	r.pendingWake = true
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *runner) logf(format string, args ...any) {
	r.b.log.Printf("group=%s key=%s "+format, append([]any{r.g.name, r.key}, args...)...)
}

func (r *runner) run() {
	defer r.b.wg.Done()
	var timer *time.Timer
	var timerStop chan struct{} // 非 nil 表示当前有重试计时 goroutine
	var timerFired bool
	cancelTimer := func() {
		if timerStop != nil {
			close(timerStop)
			timerStop = nil
		}
		if timer != nil {
			timer.Stop()
			timer = nil
		}
		timerFired = false
	}

	for {
		r.b.mu.Lock()
		if r.b.closed && r.inflight == nil {
			cancelTimer()
			r.b.mu.Unlock()
			r.logf("RUNNER stop (no in-flight)")
			return
		}

		// 消费掉进入本轮前的待处理唤醒标志（决策到达会重新置位）。
		r.pendingWake = false
		select {
		case <-r.wake:
		default:
		}

		if r.inflight == nil && !r.b.closed {
			r.tryDeliverLocked(&timer, &timerStop, &timerFired)
		} else {
			cancelTimer()
		}

		decCh := r.decisionCh
		force := r.b.baseCtx.Done()
		wake := r.wake
		r.b.mu.Unlock()

		if decCh != nil {
			// 在途：等待处理器裁决或强制取消。
			select {
			case dec := <-decCh:
				r.handleDecision(dec)
			case <-force:
				r.revokeInflight("shutdown-timeout")
				r.logf("RUNNER stop (forced); in-flight reset pending for redelivery")
				return
			}
			continue
		}

		// 空闲：等待状态变更唤醒（新消息、归属变化、重试到期、关闭）。
		select {
		case <-wake:
		case <-force:
			r.logf("RUNNER stop (forced)")
			return
		}
	}
}

// armRetryTimer 启动一个一次性 goroutine 管理重试计时。
// 该 goroutine 是 timer.C 的唯一读取者；到期后在持锁状态下唤醒主循环，
// 因此主循环从不直接消费 timer.C，避免与本 goroutine 竞争同一个 tick。
func (r *runner) armRetryTimer(d time.Duration,
	timer **time.Timer, timerStop *chan struct{}, timerFired *bool) {
	stop := make(chan struct{})
	t := time.NewTimer(d)
	*timer = t
	*timerStop = stop
	*timerFired = false
	go func() {
		select {
		case <-t.C:
			r.b.mu.Lock()
			defer r.b.mu.Unlock()
			if *timerFired {
				return
			}
			*timerFired = true
			// 定时到期必须保证唤醒即使在主循环“刚清标志尚未阻塞”的
			// 窗口里也不丢：置标志并入队 wake。
			r.pendingWake = true
			select {
			case r.wake <- struct{}{}:
			default:
			}
		case <-stop:
		case <-r.b.baseCtx.Done():
		}
	}()
}

// tryDeliverLocked 尝试投递下一条可处理消息；若只能等待重试则安排计时器。
func (r *runner) tryDeliverLocked(
	timer **time.Timer, timerStop *chan struct{}, timerFired *bool,
) {
	// 若已有计时器且尚未触发，则维持当前等待；触发过则清理后重算。
	if *timer != nil {
		if !*timerFired {
			return
		}
		if *timerStop != nil {
			close(*timerStop)
			*timerStop = nil
		}
		(*timer).Stop()
		*timer = nil
		*timerFired = false
	}

	owner := r.g.ownerOf(r.key)
	if owner == nil {
		return
	}
	msgs := r.g.keyMsgs[r.key]
	now := time.Now()
	var next *msgState
	var earliest time.Time
	for _, ms := range msgs {
		if ms.status == stAcked || ms.status == stDead {
			continue
		}
		if ms.status == stInflight {
			return
		}
		if ms.notBefore.After(now) {
			if earliest.IsZero() || ms.notBefore.Before(earliest) {
				earliest = ms.notBefore
			}
			continue
		}
		next = ms
		break
	}
	if next != nil {
		// 尝试占用该消费者的并发额度（非阻塞，满了则等待下次唤醒）。
		select {
		case owner.sem <- struct{}{}:
		default:
			return
		}
		sm := r.b.messages[next.id]
		next.status = stInflight
		next.attempts++
		r.inflight = next
		r.sem = owner.sem
		r.handlerConsumer = owner.id
		ctx, cancel := context.WithCancel(r.b.baseCtx)
		ch := make(chan Decision, 1)
		r.decisionCh = ch
		r.cancelDelivery = cancel

		d := &Delivery{
			id:         next.id,
			key:        next.key,
			group:      r.g.name,
			seq:        next.seq,
			attempt:    next.attempts,
			payload:    sm.payload,
			decisionCh: ch,
			ctx:        ctx,
		}
		r.logf("DELIVER id=%s seq=%d attempt=%d status: pending->inflight consumer=%s",
			next.id, next.seq, next.attempts, owner.id)

		h := owner.handler
		go func() {
			dec := h(ctx, d)
			d.complete(dec)
		}()
		return
	}
	if !earliest.IsZero() {
		d := time.Until(earliest)
		if d < 0 {
			d = 0
		}
		r.armRetryTimer(d, timer, timerStop, timerFired)
		return
	}
}

// handleDecision 处理处理器返回的裁决。
func (r *runner) handleDecision(dec Decision) {
	r.b.mu.Lock()
	defer r.b.mu.Unlock()

	ms := r.inflight
	if ms == nil {
		return
	}
	r.inflight = nil
	r.decisionCh = nil
	if r.sem != nil {
		<-r.sem
		r.sem = nil
	}
	r.handlerConsumer = ""
	if r.cancelDelivery != nil {
		r.cancelDelivery()
		r.cancelDelivery = nil
	}

	if ms.status != stInflight {
		r.logf("DECISION-IGNORE id=%s seq=%d status=%s (stale)", ms.id, ms.seq, ms.status)
		r.signalLocked()
		return
	}

	if dec.Ack {
		ev := groupEvent{Group: r.g.name, ID: ms.id, Key: ms.key, Seq: ms.seq}
		if _, err := r.b.w.Append(kindAcked, mustJSON(&ev)); err != nil {
			r.logf("ACK-PERSIST-FAIL id=%s err=%v -> pending redelivery", ms.id, err)
			ms.status = stPending
			r.signalLocked()
			return
		}
		ms.status = stAcked
		r.logf("ACK id=%s seq=%d attempt=%d status: inflight->acked",
			ms.id, ms.seq, ms.attempts)
		r.compactLocked()
		r.signalLocked()
		return
	}

	ms.errMsg = errText(dec.Err)
	if dec.Unrecoverable || ms.attempts >= r.b.cfg.MaxAttempts {
		reason := ReasonRetryExhausted
		if dec.Unrecoverable {
			reason = ReasonUnrecoverable
		}
		ev := groupEvent{
			Group: r.g.name, ID: ms.id, Key: ms.key, Seq: ms.seq,
			Attempt: ms.attempts, Reason: string(reason), ErrMsg: ms.errMsg,
		}
		if _, err := r.b.w.Append(kindDead, mustJSON(&ev)); err != nil {
			r.logf("DEAD-PERSIST-FAIL id=%s err=%v -> pending redelivery", ms.id, err)
			ms.status = stPending
			r.signalLocked()
			return
		}
		ms.status = stDead
		ms.reason = reason
		ms.deadAt = time.Now()
		cp := *ms
		r.g.dead = append(r.g.dead, &cp)
		r.logf("DEAD id=%s seq=%d attempt=%d reason=%s err=%q status: inflight->dead",
			ms.id, ms.seq, ms.attempts, reason, ms.errMsg)
		r.compactLocked()
		r.signalLocked()
		return
	}

	delay := dec.RequeueDelay
	if delay <= 0 {
		delay = r.b.cfg.RetryDelay
		if r.b.cfg.RetryBackoff > 1 {
			for i := 1; i < ms.attempts; i++ {
				delay = time.Duration(float64(delay) * r.b.cfg.RetryBackoff)
			}
		}
	}
	if delay > r.b.cfg.MaxRetryDelay {
		delay = r.b.cfg.MaxRetryDelay
	}
	notBefore := time.Now().Add(delay)
	ev := groupEvent{
		Group: r.g.name, ID: ms.id, Key: ms.key, Seq: ms.seq,
		Attempt: ms.attempts, ErrMsg: ms.errMsg, NotBefore: notBefore.UnixNano(),
	}
	if _, err := r.b.w.Append(kindNacked, mustJSON(&ev)); err != nil {
		r.logf("NACK-PERSIST-FAIL id=%s err=%v -> retry immediately", ms.id, err)
		ms.status = stPending
		r.signalLocked()
		return
	}
	ms.status = stPending
	ms.notBefore = notBefore
	r.logf("NACK id=%s seq=%d attempt=%d retryIn=%s err=%q status: inflight->pending",
		ms.id, ms.seq, ms.attempts, delay, ms.errMsg)
	r.signalLocked()
}

// revokeInflight 在关闭超时等场景把在途消息收回为 pending（不落盘确认）。
func (r *runner) revokeInflight(cause string) {
	r.b.mu.Lock()
	defer r.b.mu.Unlock()
	ms := r.inflight
	if ms == nil {
		return
	}
	if ms.status == stInflight {
		ms.status = stPending
	}
	r.inflight = nil
	if r.cancelDelivery != nil {
		r.cancelDelivery()
		r.cancelDelivery = nil
	}
	r.decisionCh = nil
	if r.sem != nil {
		<-r.sem
		r.sem = nil
	}
	r.handlerConsumer = ""
	r.logf("REVOKE id=%s seq=%d cause=%s -> pending (at-least-once redelivery)",
		ms.id, ms.seq, cause)
}

// compactLocked 裁剪路由键队列开头连续的终态消息，控制内存增长。
func (r *runner) compactLocked() {
	msgs := r.g.keyMsgs[r.key]
	i := 0
	for i < len(msgs) && (msgs[i].status == stAcked || msgs[i].status == stDead) {
		i++
	}
	if i > 0 {
		r.g.keyMsgs[r.key] = append(msgs[i:i:i], msgs[i:]...)
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// stopTimer 停止定时器并排空其 channel 中可能残留的到期值。
func stopTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}
