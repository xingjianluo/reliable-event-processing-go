package mq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// 缩容时被移除消费者的处理器卡死：超时后消息被收回并重投给新归属，
// 不悬挂、不丢失、不重复生效。
func TestConsumerRemovalStuckHandler(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.ShutdownTimeout = 200 * time.Millisecond
	b, _ := OpenBroker(cfg)
	defer b.Close()

	eff := newEffects()
	var stuckCancelled atomic.Bool

	// 统一处理器：仅 c1 的第一次投递卡住（模拟缩容时在途处理器卡死）。
	handler := func(consumer string) Handler {
		return func(ctx context.Context, d *Delivery) Decision {
			if consumer == "c1" && d.Attempt() == 1 {
				<-ctx.Done()
				stuckCancelled.Store(true)
				return AckDecision() // 已被收回，此裁决不会落盘
			}
			eff.mark(d.ID())
			return AckDecision()
		}
	}

	c1, _ := b.Subscribe("g", "c1", handler("c1"))
	res, _ := b.Publish(context.Background(), Message{Key: "stuck"}, PublishOptions{})
	waitFor(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		r := b.groups["g"].runners["stuck"]
		return r != nil && r.inflight != nil
	}, time.Second, "message in-flight on c1")

	// 扩容 c2、c3；确保 "stuck" 在 c1 退出后仍有其他消费者可立即接手。
	c2, _ := b.Subscribe("g", "c2", handler("c2"))
	c3, _ := b.Subscribe("g", "c3", handler("c2")) // 同样的成功处理器
	defer c3.Close()

	done := make(chan struct{})
	go func() { _ = c1.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("c1.Close blocked despite timeout")
	}

	waitFor(t, func() bool { return eff.count(res.ID) == 1 }, 3*time.Second,
		"revoked message redelivered exactly once to c2")
	if !stuckCancelled.Load() {
		t.Fatal("stuck handler context was not cancelled")
	}
	time.Sleep(30 * time.Millisecond)
	if n := eff.count(res.ID); n != 1 {
		t.Fatalf("message applied %d times, want 1", n)
	}
	c2.Close()
}
