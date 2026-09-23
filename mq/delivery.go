package mq

import (
	"context"
	"sync"
	"time"
)

// Delivery 是投递到处理器的消息句柄。
// Ack/Nack 可显式调用；处理器返回的 Decision 也可完成裁决。
// 重复或过期（已被重投）的 Ack/Nack 调用会被安全忽略。
type Delivery struct {
	id         string
	key        string
	group      string
	seq        int64
	attempt    int
	payload    []byte
	decisionCh chan Decision
	ctx        context.Context

	mu      sync.Mutex
	decided bool
}

// ID 返回消息标识。
func (d *Delivery) ID() string { return d.id }

// RoutingKey 返回路由键。
func (d *Delivery) RoutingKey() string { return d.key }

// Attempt 返回本次投递的尝试次数（首次为 1）。
func (d *Delivery) Attempt() int { return d.attempt }

// Seq 返回消息在路由键内的序号（从 1 开始）。
func (d *Delivery) Seq() int64 { return d.seq }

// Payload 返回消息负载。
func (d *Delivery) Payload() []byte { return d.payload }

// Context 返回本次投递的上下文（关闭时取消）。
func (d *Delivery) Context() context.Context { return d.ctx }

// Ack 显式确认。
func (d *Delivery) Ack() { d.complete(AckDecision()) }

// Nack 显式拒绝。
func (d *Delivery) Nack(opts NackOptions) {
	d.complete(Decision{
		Ack:           false,
		RequeueDelay:  opts.Delay,
		Unrecoverable: opts.Unrecoverable,
		Err:           opts.Err,
	})
}

// complete 保证一次投递只接受第一个裁决（后续重复调用忽略）。
func (d *Delivery) complete(dec Decision) {
	d.mu.Lock()
	if d.decided {
		d.mu.Unlock()
		return
	}
	d.decided = true
	d.mu.Unlock()
	select {
	case d.decisionCh <- dec:
	case <-d.ctx.Done():
	}
}

// NackOptions 是显式拒绝的选项。
type NackOptions struct {
	Delay         time.Duration
	Unrecoverable bool
	Err           error
}
