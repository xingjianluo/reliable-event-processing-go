package mq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// 处理器永久卡住：Close 在 ShutdownTimeout 后强制返回，不静默确认；
// 重启后该消息作为未确认被重新投递（at-least-once）。
func TestShutdownTimeoutForceCancel(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.ShutdownTimeout = 200 * time.Millisecond
	b, _ := OpenBroker(cfg)

	var entered atomic.Int32
	var released atomic.Int32
	c, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		entered.Add(1)
		<-ctx.Done() // 模拟卡死，直到关闭强制取消
		released.Add(1)
		return AckDecision() // 该返回不会落盘（runner 已 revoke）
	})
	res, _ := b.Publish(context.Background(), Message{Key: "k"}, PublishOptions{})
	waitFor(t, func() bool { return entered.Load() == 1 }, time.Second, "entered handler")

	start := time.Now()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("close took too long: %s", d)
	}
	waitFor(t, func() bool { return released.Load() == 1 }, time.Second, "stuck handler released by cancel")

	// 重启：该消息必须作为未确认重投。
	eff := newEffects()
	b2, _ := OpenBroker(cfg)
	c2, _ := b2.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		eff.mark(d.ID())
		return AckDecision()
	})
	waitFor(t, func() bool { return eff.total() == 1 }, 2*time.Second, "redelivered after forced shutdown")
	if eff.count(res.ID) != 1 {
		t.Fatalf("unacked message not redelivered exactly once: %d", eff.count(res.ID))
	}
	c2.Close()
	b2.Close()
}

// 乱序确认：先处理的消息晚确认，后到的消息不得越过前一条；
// 最终仍严格按序，且不重复不丢失。
func TestHeadOfLineOrdering(t *testing.T) {
	dir := t.TempDir()
	b, _ := OpenBroker(testConfig(t, dir))
	defer b.Close()

	block := make(chan struct{})
	first := atomic.Bool{}
	c, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		if d.Seq() == 1 && !first.Swap(true) {
			<-block // 第一条挂住，验证后续同键消息被排在其后
			return AckDecision()
		}
		return AckDecision()
	})
	defer c.Close()

	for i := 0; i < 5; i++ {
		b.Publish(context.Background(), Message{Key: "k"}, PublishOptions{})
	}
	time.Sleep(100 * time.Millisecond)
	// 此时只有 seq=1 在途；seq 2..5 不得被投递（队头阻塞 = 严格顺序）。
	b.mu.Lock()
	runner := b.groups["g"].runners["k"]
	inflightID := ""
	if runner.inflight != nil {
		inflightID = runner.inflight.id
	}
	delivered := runner.inflight.attempts
	b.mu.Unlock()
	if delivered != 1 || inflightID == "" {
		t.Fatalf("expected exactly seq1 inflight, attempts=%d id=%s", delivered, inflightID)
	}
	close(block)
	// 全部确认。
	waitFor(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.groups["g"].keyMsgs["k"]) == 0
	}, 2*time.Second, "all processed in order after unblock")
}
