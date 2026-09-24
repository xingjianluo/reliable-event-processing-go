package localmq

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
)

// settleFunc is supplied by the broker; it persists one delivery outcome.
// Returning nil means this call was the first (winning) settlement.
type settleFunc func(group, id string, a settleAction) error

// Consumer is one named member of a consumer group. Route keys are hashed
// across the live members of a group; a consumer processes only keys mapped to
// it. The broker guarantees a key maps to exactly one consumer at any instant
// and never has more than one unacknowledged delivery per key.
type Consumer struct {
	name        string
	group       string
	concurrency int
	handle      HandlerFunc
	logger      interface {
		Info(msg string, args ...any)
		Error(msg string, args ...any)
	}

	sem chan struct{} // concurrency slots

	// inFlight tracks handler goroutines currently running for this consumer,
	// so Unsubscribe/Close can wait for exactly its own in-flight work.
	inFlight waitGroup
}

type waitGroup struct {
	mu     sync.Mutex
	n      int
	zeroCh chan struct{}
}

func (w *waitGroup) add(n int) {
	w.mu.Lock()
	w.n += n
	w.mu.Unlock()
}

func (w *waitGroup) done() {
	w.mu.Lock()
	w.n--
	if w.n == 0 && w.zeroCh != nil {
		close(w.zeroCh)
		w.zeroCh = nil
	}
	w.mu.Unlock()
}

// wait blocks until no handlers are running or ctx expires.
func (w *waitGroup) wait(ctx context.Context) error {
	w.mu.Lock()
	if w.n == 0 {
		w.mu.Unlock()
		return nil
	}
	if w.zeroCh == nil {
		w.zeroCh = make(chan struct{})
	}
	ch := w.zeroCh
	w.mu.Unlock()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ConsumerOption configures a Consumer.
type ConsumerOption func(*Consumer)

// WithConcurrency sets how many deliveries this consumer processes in
// parallel. Default 1 gives strict handler-execution ordering per route key.
// Values > 1 allow parallel processing of different route keys mapped to the
// same consumer; per-key ordering is still preserved because the broker never
// schedules the next message of a key before the previous one is settled.
func WithConcurrency(n int) ConsumerOption {
	return func(c *Consumer) {
		if n > 0 {
			c.concurrency = n
		}
	}
}

func newConsumer(group, name string, h HandlerFunc, logger lineLogger, opts ...ConsumerOption) *Consumer {
	c := &Consumer{
		name:        name,
		group:       group,
		concurrency: 1,
		handle:      h,
		logger:      logger,
	}
	for _, o := range opts {
		o(c)
	}
	c.sem = make(chan struct{}, c.concurrency)
	return c
}

type lineLogger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

// acquire blocks until a handler slot is free or ctx is canceled.
func (c *Consumer) acquire(ctx context.Context) bool {
	select {
	case c.sem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *Consumer) release() { <-c.sem }

// tryAcquire takes a handler slot without blocking.
func (c *Consumer) tryAcquire() bool {
	select {
	case c.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

// process runs one delivery to completion and settles it exactly once.
//
// Settlement precedence:
//  1. An explicit Ack/Nack/Reject call inside the handler wins if it happens
//     first; whatever the handler later returns is ignored.
//  2. A nil return after no explicit settlement means Ack.
//  3. A non-nil return after no explicit settlement means Nack (backoff).
//  4. A recovered panic means immediate dead-letter (ReasonPanic).
func (c *Consumer) process(ctx context.Context, d *Delivery, settle settleFunc, done func()) {
	defer done()
	defer c.release()

	d.Attempt = attemptOf(d)
	started := time.Now()
	c.logger.Info("deliver",
		"msg_id", d.Message.ID, "route", d.Message.RouteKey,
		"group", d.Group, "consumer", c.name,
		"attempt", d.Attempt, "state", "delivered")

	var action settleAction
	var explicit bool
	d.settle = func(a settleAction) error {
		err := settle(c.group, d.Message.ID, a)
		if err != nil {
			c.logger.Info("settle rejected",
				"msg_id", d.Message.ID, "route", d.Message.RouteKey,
				"attempt", d.Attempt, "state", "settle_rejected", "err", err.Error())
			return err
		}
		explicit = true
		action = a
		logSettlement(c.logger, d, c.name, a, started)
		return nil
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				if !explicit {
					action = settleAction{kind: settlePanic,
						reason: fmt.Sprintf("panic: %v", r)}
					// Persist the panic outcome (first settlement wins).
					if err := settle(c.group, d.Message.ID, action); err != nil {
						c.logger.Error("panic settle failed",
							"msg_id", d.Message.ID, "err", err.Error())
						return
					}
					logSettlement(c.logger, d, c.name, action, started)
					c.logger.Error("handler panic recovered",
						"msg_id", d.Message.ID, "route", d.Message.RouteKey,
						"attempt", d.Attempt, "panic", fmt.Sprintf("%v", r),
						"stack", string(debug.Stack()))
				}
			}
		}()
		if err := c.handle(ctx, d); err != nil && !explicit {
			action = settleAction{kind: settleNack, reason: err.Error()}
			if serr := settle(c.group, d.Message.ID, action); serr != nil {
				c.logger.Error("auto-nack failed",
					"msg_id", d.Message.ID, "route", d.Message.RouteKey,
					"err", serr.Error(), "cause", err.Error())
			} else {
				logSettlement(c.logger, d, c.name, action, started)
			}
		} else if err == nil && !explicit {
			action = settleAction{kind: settleAck}
			if serr := settle(c.group, d.Message.ID, action); serr != nil {
				c.logger.Error("auto-ack failed",
					"msg_id", d.Message.ID, "route", d.Message.RouteKey,
					"err", serr.Error())
			} else {
				logSettlement(c.logger, d, c.name, action, started)
			}
		}
	}()
}

func attemptOf(d *Delivery) int {
	if d.Attempt > 0 {
		return d.Attempt
	}
	return 1
}

func logSettlement(log lineLogger, d *Delivery, consumer string, a settleAction, started time.Time) {
	state := "acked"
	switch a.kind {
	case settleNack:
		state = "nacked"
	case settleReject:
		state = "rejected"
	case settlePanic:
		state = "panic_dead"
	case settleAck:
		state = "acked"
	}
	args := []any{
		"msg_id", d.Message.ID, "route", d.Message.RouteKey,
		"group", d.Group, "consumer", consumer,
		"attempt", d.Attempt, "state", state,
		"took_ms", time.Since(started).Milliseconds(),
	}
	if a.reason != "" {
		args = append(args, "reason", a.reason)
	}
	log.Info("settle", args...)
}
