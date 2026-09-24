package mq

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// 消费组扩容/缩容：消息既不重复成功也不丢失。
func TestConsumerScaleOutAndIn(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.RetryDelay = 5 * time.Millisecond
	b, _ := OpenBroker(cfg)
	defer b.Close()

	eff := newEffects()
	// 慢处理器，让扩容时存在在途消息。
	makeHandler := func() Handler {
		return func(ctx context.Context, d *Delivery) Decision {
			time.Sleep(8 * time.Millisecond)
			eff.mark(d.ID()) // 业务幂等生效
			return AckDecision()
		}
	}
	c1, _ := b.Subscribe("g", "c1", makeHandler())

	// 先用一个消费者发布部分消息。
	for i := 0; i < 30; i++ {
		b.Publish(context.Background(), Message{Key: keyFor(i)}, PublishOptions{})
	}
	// 处理进行中扩容到 3 个消费者。
	time.Sleep(40 * time.Millisecond)
	c2, _ := b.Subscribe("g", "c2", makeHandler())
	c3, _ := b.Subscribe("g", "c3", makeHandler())
	// 再发布部分消息（此时 3 个消费者）。
	for i := 30; i < 60; i++ {
		b.Publish(context.Background(), Message{Key: keyFor(i)}, PublishOptions{})
	}
	// 缩容：移除两个消费者。
	time.Sleep(40 * time.Millisecond)
	c2.Close()
	c3.Close()
	for i := 60; i < 90; i++ {
		b.Publish(context.Background(), Message{Key: keyFor(i)}, PublishOptions{})
	}

	waitFor(t, func() bool { return eff.total() == 90 }, 5*time.Second, "all 90 processed across scale changes")
	// 等待框架确认全部落盘后再检查状态（业务先生效、ACK 后落盘）。
	waitGroupDrained(t, b, "g", 5*time.Second)
	if eff.total() != 90 {
		t.Fatalf("total effects=%d want 90", eff.total())
	}
	b.mu.Lock()
	g := b.groups["g"]
	for id, n := range eff.success {
		if n != 1 {
			b.mu.Unlock()
			t.Fatalf("message %s applied %d times, want exactly 1", id, n)
		}
		if ms := g.byID[id]; ms != nil && ms.status != stAcked {
			b.mu.Unlock()
			t.Fatalf("message %s status=%s want acked", id, ms.status)
		}
	}
	b.mu.Unlock()
	_ = c1
}

// 不同消费组彼此独立：同一消息在两个组各成功一次。
func TestIndependentGroups(t *testing.T) {
	dir := t.TempDir()
	b, _ := OpenBroker(testConfig(t, dir))
	defer b.Close()

	g1 := newEffects()
	g2 := newEffects()
	c1, _ := b.Subscribe("group-A", "a", func(ctx context.Context, d *Delivery) Decision {
		g1.mark(d.ID())
		return AckDecision()
	})
	c2, _ := b.Subscribe("group-B", "b", func(ctx context.Context, d *Delivery) Decision {
		g2.mark(d.ID())
		return AckDecision()
	})
	defer c1.Close()
	defer c2.Close()

	for i := 0; i < 10; i++ {
		b.Publish(context.Background(), Message{Key: "shared"}, PublishOptions{})
	}
	waitFor(t, func() bool { return g1.total() == 10 && g2.total() == 10 }, 2*time.Second, "both groups")
}

// 优雅关闭：在途消息必须得到处理，不静默丢失。
func TestGracefulShutdownInFlight(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	b, _ := OpenBroker(cfg)

	var done atomic.Int32
	c, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		time.Sleep(50 * time.Millisecond) // 关闭时有在途消息
		done.Add(1)
		return AckDecision()
	})
	res, _ := b.Publish(context.Background(), Message{Key: "k"}, PublishOptions{})

	time.Sleep(20 * time.Millisecond) // 确保消息已在处理中
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if done.Load() != 1 {
		t.Fatalf("in-flight handled=%d want 1 (no silent loss)", done.Load())
	}
	// 重新打开确认状态已持久化（不重投）。
	var redelivered atomic.Int32
	b2, _ := OpenBroker(cfg)
	c2, _ := b2.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		redelivered.Add(1)
		return AckDecision()
	})
	time.Sleep(80 * time.Millisecond)
	if redelivered.Load() != 0 {
		t.Fatalf("acked in-flight redelivered=%d", redelivered.Load())
	}
	b2.mu.Lock()
	st := b2.groups["g"].byID[res.ID].status
	b2.mu.Unlock()
	if st != stAcked {
		t.Fatalf("status after restart=%s want acked", st)
	}
	c2.Close()
	b2.Close()
}

func keyFor(i int) string {
	return fmt.Sprintf("key-%d", i%6)
}
