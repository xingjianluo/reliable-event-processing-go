package mq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// 重启后：已确认消息绝不重投，位点之前的消息不会被再次提交成功。
func TestRestartAckedNotRedelivered(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)

	// 第一段：处理 10 条，全部确认。
	b, _ := OpenBroker(cfg)
	c, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		return AckDecision()
	})
	var acked []string
	for i := 0; i < 10; i++ {
		res, _ := b.Publish(context.Background(), Message{Key: "k"}, PublishOptions{})
		acked = append(acked, res.ID)
	}
	waitFor(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		// 队列已被压缩为空即表示全部 acked（终态被裁剪）。
		return len(b.groups["g"].keyMsgs["k"]) == 0
	}, 2*time.Second, "all acked before crash")
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// 第二段：重新打开并订阅。业务侧记录重启后的成功次数。
	var redelivered atomic.Int32
	b2, _ := OpenBroker(cfg)
	c2, _ := b2.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		redelivered.Add(1)
		return AckDecision()
	})
	// 再发 2 条新消息，确保恢复后仍可正常工作。
	for i := 0; i < 2; i++ {
		b2.Publish(context.Background(), Message{Key: "k"}, PublishOptions{})
	}
	waitFor(t, func() bool { return redelivered.Load() == 2 }, 2*time.Second, "only 2 new delivered")
	time.Sleep(50 * time.Millisecond)
	if n := redelivered.Load(); n != 2 {
		t.Fatalf("redelivered after restart=%d, want exactly 2 (acked ones must not redeliver)", n)
	}
	c2.Close()
	b2.Close()
}

// 崩溃在“确认前/处理中”：未确认消息重启后必须重新投递（at-least-once）。
func TestRestartInflightRedelivered(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)

	// 第一段：处理器处理两个不同路由键的消息时卡住不返回
	// （同键严格串行，所以用两个键来制造 2 条在途消息，模拟处理中崩溃）。
	var entered atomic.Int32
	release := make(chan struct{})
	b, _ := OpenBroker(cfg)
	c, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		if d.RoutingKey() == "stuck1" || d.RoutingKey() == "stuck2" {
			entered.Add(1)
			<-release // 挂住，模拟进程在处理消息时被杀死
			return AckDecision()
		}
		return AckDecision()
	})
	var ids []string
	// 先发一条普通消息并确认，用于验证“已确认不重投”。
	okRes, _ := b.Publish(context.Background(), Message{Key: "ok"}, PublishOptions{})
	waitFor(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.groups["g"].byID[okRes.ID].status == stAcked
	}, time.Second, "ok message acked before crash")
	ids = append(ids, okRes.ID)
	// 再发两条会挂住的消息，制造“处理中”在途状态。
	for _, k := range []string{"stuck1", "stuck2"} {
		res, _ := b.Publish(context.Background(), Message{Key: k}, PublishOptions{})
		ids = append(ids, res.ID)
	}
	waitFor(t, func() bool { return entered.Load() == 2 }, time.Second, "2 in-flight")

	// 模拟硬崩溃：不等待在途、不优雅落盘，直接终止消费循环并关闭 WAL。
	// 这 2 条的确认从未落盘；fsync 过的发布事件保留。
	c.closed = true // 避免 c.Close 优雅等待
	if err := b.crashForTest(); err != nil {
		t.Fatal(err)
	}

	// 第二段：重启，2 条未确认消息必须重投；"ok" 崩溃前已确认，不得重投。
	eff := newEffects()
	b2, _ := OpenBroker(cfg)
	c2, _ := b2.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		eff.mark(d.ID())
		return AckDecision()
	})
	waitFor(t, func() bool { return eff.total() == 2 }, 2*time.Second, "2 unacked redelivered after crash")
	time.Sleep(50 * time.Millisecond)
	if eff.total() != 2 {
		t.Fatalf("redelivered=%d, want exactly 2 (acked must not redeliver)", eff.total())
	}
	for _, id := range ids[1:] {
		if eff.count(id) != 1 {
			t.Fatalf("id=%s applied %d times after recovery, want 1", id, eff.count(id))
		}
	}
	if eff.count(ids[0]) != 0 {
		t.Fatalf("previously acked id=%s redelivered", ids[0])
	}
	c2.Close()
	b2.Close()
}

// 拒绝后的重试状态跨重启保留：attempt 累计，最终重试耗尽进入死信。
func TestRestartRetryStatePreserved(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.MaxAttempts = 4
	// 重启前的重试窗口必须足够宽：要保证“观察到 2 次拒绝 -> 优雅关闭”
	// 确定发生在第 3 次投递之前。否则在调度严重受限时第 3 次投递可能
	// 在关闭前于旧 broker 内直接死信，重启后死信由日志重放恢复、
	// 新处理器根本不会被调用，用例就测不到“重启后 attempt 累计”。
	cfg.RetryDelay = 500 * time.Millisecond
	cfg.MaxRetryDelay = time.Second

	b, _ := OpenBroker(cfg)
	var nackedBefore atomic.Int32
	c, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		if d.Attempt() <= 2 {
			nackedBefore.Add(1)
			return RetryDecision(errors.New("t"), 500*time.Millisecond)
		}
		return DropDecision(errors.New("give up"))
	})
	res, _ := b.Publish(context.Background(), Message{Key: "k"}, PublishOptions{})
	// 等到发生两次拒绝（attempt 1、2），然后在等待重试期间优雅关闭。
	waitFor(t, func() bool { return nackedBefore.Load() == 2 }, time.Second, "2 nacks")
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启后：该消息 attempt 已为 2，不应清零；再投递（attempt 3）即进入死信。
	var attemptsAfter atomic.Int32
	b2, _ := OpenBroker(cfg)
	c2, _ := b2.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		attemptsAfter.Store(int32(d.Attempt()))
		return DropDecision(errors.New("give up2"))
	})
	waitFor(t, func() bool { return len(b2.DeadLetters("g")) == 1 }, 2*time.Second, "dead after restart")
	if a := attemptsAfter.Load(); a != 3 {
		t.Fatalf("attempt after restart=%d, want 3 (attempts must accumulate across restart)", a)
	}
	dl := b2.DeadLetters("g")[0]
	if dl.ID != res.ID {
		t.Fatalf("dead id=%s want %s", dl.ID, res.ID)
	}
	c2.Close()
	b2.Close()
}
