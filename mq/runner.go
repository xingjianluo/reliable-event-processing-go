package mq

import (
	"context"
	"sync"
	"time"
)

// runner 负责单个（消费组, 路由键）的严格有序消费循环。
// 同一时刻该路由键最多只有一条消息处于 inflight，因此同键严格有序；
// 不同路由键拥有独立 runner，可并行消费。
//
// 归属（owner）只决定“下一条”消息由哪个消费者处理器执行；
// 已经在途的消息允许原处理器跑完并落盘裁决，避免无谓重投。
//
// 唤醒模型：使用以 b.mu 为锁的 sync.Cond。所有状态变更与 signal
// 都在持有 b.mu 时完成，runner 在持锁状态下调用 cond.Wait（原子地
// 释放锁并进入等待）。因此“发布/缩容/重试到期”等事件不可能在
// runner 检查状态与进入等待之间丢失。
type runner struct {
	b   *Broker
	g   *groupState
	key string

	cond *sync.Cond

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
		cond: sync.NewCond(&b.mu),
	}
	g.runners[key] = r
	b.wg.Add(1)
	go r.run()
}

// wakeLocked 唤醒等待中的 runner。调用方必须持有 b.mu。
func (b *Broker) wakeLocked(g *groupState, key string) {
	r := g.runners[key]
	if r == nil {
		b.ensureRunnerLocked(g, key)
		r = g.runners[key]
	}
	r.cond.Signal()
}

func (r *runner) logf(format string, args ...any) {
	r.b.log.Printf("group=%s key=%s "+format, append([]any{r.g.name, r.key}, args...)...)
}

func (r *runner) run() {
	defer r.b.wg.Done()
	var timer *time.Timer
	var timerCancel chan struct{} // close 以取消计时 goroutine

	cancelTimer := func() {
		if timerCancel != nil {
			close(timerCancel)
			timerCancel = nil
		}
		if timer != nil {
			timer.Stop()
			timer = nil
		}
	}

	for {
		r.b.mu.Lock()

		// 关闭且无在途：退出。
		if r.b.closed && r.inflight == nil {
			cancelTimer()
			r.b.mu.Unlock()
			r.logf("RUNNER stop (no in-flight)")
			return
		}

		if r.inflight == nil && !r.b.closed {
			r.tryDeliverLocked(&timer, &timerCancel)
		} else {
			cancelTimer()
		}

		if r.inflight != nil {
			// 在途：在锁内等待处理器裁决（decisionCh 由处理 goroutine
			// 在持锁状态变更前写入；这里用一个解锁窗口等待它）。
			decCh := r.decisionCh
			force := r.b.baseCtx.Done()
			r.b.mu.Unlock()
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

		// 空闲：在锁内等待状态变化或关闭。重试计时 goroutine 会在到期时
		// 持锁 Broadcast；发布/缩容/关闭同样在持锁时 Signal/Broadcast。
		// cond.Wait 原子地释放锁并等待，因此信号不可能在“检查状态之后、
		// 进入等待之前”丢失。
		for !r.b.closed && !r.hasWorkLocked() {
			r.cond.Wait()
		}
		r.b.mu.Unlock()
	}
}

// hasWorkLocked 判断是否应结束等待重新评估：关闭、有立即可投递消息、
// 或已安排的重试计时已经到期。
func (r *runner) hasWorkLocked() bool {
	if r.b.closed {
		return true
	}
	if owner := r.g.ownerOf(r.key); owner == nil {
		return false
	}
	now := time.Now()
	for _, ms := range r.g.keyMsgs[r.key] {
		if ms.status == stAcked || ms.status == stDead {
			continue
		}
		if ms.status == stInflight {
			return true
		}
		if !ms.notBefore.After(now) {
			return true
		}
	}
	return false
}

// armRetryTimer 安排一个一次性 goroutine 在 d 后唤醒 runner。
func (r *runner) armRetryTimer(d time.Duration, timer **time.Timer, timerCancel *chan struct{}) {
	cancel := make(chan struct{})
	t := time.NewTimer(d)
	*timer = t
	*timerCancel = cancel
	go func() {
		select {
		case <-t.C:
			r.b.mu.Lock()
			r.cond.Broadcast()
			r.b.mu.Unlock()
		case <-cancel:
		case <-r.b.baseCtx.Done():
		}
	}()
}

// tryDeliverLocked 尝试投递下一条消息，或安排重试计时。
func (r *runner) tryDeliverLocked(timer **time.Timer, timerCancel *chan struct{}) {
	// 清理上一轮计时器（若有）。
	if *timerCancel != nil {
		close(*timerCancel)
		*timerCancel = nil
	}
	if *timer != nil {
		(*timer).Stop()
		*timer = nil
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
		r.armRetryTimer(d, timer, timerCancel)
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
		r.cond.Signal()
		return
	}

	if dec.Ack {
		ev := groupEvent{Group: r.g.name, ID: ms.id, Key: ms.key, Seq: ms.seq}
		if _, err := r.b.w.Append(kindAcked, mustJSON(&ev)); err != nil {
			r.logf("ACK-PERSIST-FAIL id=%s err=%v -> pending redelivery", ms.id, err)
			ms.status = stPending
			r.cond.Signal()
			return
		}
		ms.status = stAcked
		r.logf("ACK id=%s seq=%d attempt=%d status: inflight->acked",
			ms.id, ms.seq, ms.attempts)
		r.compactLocked()
		r.cond.Signal()
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
			r.cond.Signal()
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
		r.cond.Signal()
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
		r.cond.Signal()
		return
	}
	ms.status = stPending
	ms.notBefore = notBefore
	r.logf("NACK id=%s seq=%d attempt=%d retryIn=%s err=%q status: inflight->pending",
		ms.id, ms.seq, ms.attempts, delay, ms.errMsg)
	r.cond.Signal()
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
