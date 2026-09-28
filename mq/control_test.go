package mq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// waitKeyState 轮询运行态快照直到进入任一目标状态。
// 它等待的是 broker 已提交状态（ACK/死信落盘后的终态），
// 而不是“业务处理器已返回”——后者先于 ACK fsync，直接读会踩在
// inflight->acked 的窗口上，这正是本批用例随机失败的根因。
// 轮询是正确的同步手段：drained 是单调终态，一旦观察到就不会回退，
// 因此不靠拉长固定 sleep 去碰运气。
func waitKeyState(t *testing.T, b *Broker, group, key string, want ...KeyState) KeySnapshot {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var snap KeySnapshot
	for time.Now().Before(deadline) {
		snap = b.KeyStatus(group, key)
		for _, s := range want {
			if snap.State == s {
				return snap
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting key=%s state in %v, last snapshot=%+v", key, want, snap)
	return snap
}

// assertNotDeliveredFor 是“暂停期间不投递”的负向断言：在整个观察窗口内
// 反复校验计数没有增长，而不是只 sleep 后看一眼。
// 窗口长度必须覆盖相关重试到期点（由用例给出），语义断言原样保留。
func assertNotDeliveredFor(t *testing.T, n func() int, want int, window time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		if got := n(); got != want {
			t.Fatalf("%s: processed=%d want %d (delivery happened while paused)", msg, got, want)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if got := n(); got != want {
		t.Fatalf("%s: processed=%d want %d", msg, got, want)
	}
}

// 暂停期间：该键新消息不投递，其他键不受影响；恢复后按序补齐。
func TestPauseBlocksDeliveryOtherKeysUnaffected(t *testing.T) {
	dir := t.TempDir()
	b, _ := OpenBroker(testConfig(t, dir))
	defer b.Close()

	eff := newEffects()
	var mu sync.Mutex
	order := map[string][]int64{}
	c, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		eff.mark(d.ID())
		mu.Lock()
		order[d.RoutingKey()] = append(order[d.RoutingKey()], d.Seq())
		mu.Unlock()
		return AckDecision()
	})
	defer c.Close()

	if err := b.Pause("g", "k1"); err != nil {
		t.Fatal(err)
	}
	// 重复暂停幂等。
	if err := b.Pause("g", "k1"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		b.Publish(context.Background(), Message{Key: "k1"}, PublishOptions{})
	}
	for i := 0; i < 2; i++ {
		b.Publish(context.Background(), Message{Key: "k2"}, PublishOptions{})
	}

	// 其他键正常消费。
	waitFor(t, func() bool { return eff.total() == 2 }, 2*time.Second, "k2 processed")
	// 暂停键不被投递：在覆盖数个调度周期的观察窗口内持续无新增。
	assertNotDeliveredFor(t, eff.total, 2, 80*time.Millisecond, "paused key k1")

	// 快照：暂停 + 积压。
	snap := b.KeyStatus("g", "k1")
	if !snap.Exists || !snap.Paused || snap.State != StatePaused || snap.WaitReason != ReasonPaused {
		t.Fatalf("snapshot = %+v, want paused", snap)
	}
	if snap.HeadSeq != 1 || snap.HeadAttempts != 0 || snap.Backlog != 2 {
		t.Fatalf("snapshot head/backlog wrong: %+v", snap)
	}

	// 恢复后从最早未完成消息按序补齐。
	if err := b.Resume("g", "k1"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return eff.total() == 5 }, 2*time.Second, "k1 drained after resume")
	// drained 是 ACK 落盘后的已提交终态；业务计数到 5 后仍可能有
	// 最后一个 ACK 正在 fsync，必须等快照到达 drained 再断言。
	got := waitKeyState(t, b, "g", "k1", StateDrained)
	mu.Lock()
	defer mu.Unlock()
	for i, s := range order["k1"] {
		if s != int64(i+1) {
			t.Fatalf("k1 out of order at %d: seq=%d", i, s)
		}
	}
	if got.State != StateDrained {
		t.Fatalf("after drain snapshot = %+v, want drained", got)
	}
}

// 队头处于延期重试且键被暂停：到期也不投递，后面消息不能先行完成；
// 恢复后从队头继续，严格按序。
func TestPausedKeyWithDelayedRetryHead(t *testing.T) {
	dir := t.TempDir()
	b, _ := OpenBroker(testConfig(t, dir))
	defer b.Close()

	eff := newEffects()
	var mu sync.Mutex
	var seq1Deliveries int
	var ackedSeqs []int64
	c, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		if d.Seq() == 1 {
			mu.Lock()
			seq1Deliveries++
			n := seq1Deliveries
			mu.Unlock()
			if n == 1 {
				// 首次：拒绝并延期 150ms 重试。
				return RetryDecision(errors.New("transient"), 150*time.Millisecond)
			}
		}
		eff.mark(d.ID())
		mu.Lock()
		ackedSeqs = append(ackedSeqs, d.Seq())
		mu.Unlock()
		return AckDecision()
	})
	defer c.Close()

	for i := 0; i < 3; i++ {
		b.Publish(context.Background(), Message{Key: "k"}, PublishOptions{})
	}
	// 等队头第一次投递完成裁决并进入延期重试。
	waitFor(t, func() bool {
		return b.KeyStatus("g", "k").State == StateWaitingRetry
	}, 2*time.Second, "seq1 enters delayed retry")

	// 队头等待重试期间，后面消息不得完成。
	snap := b.KeyStatus("g", "k")
	if snap.State != StateWaitingRetry || snap.WaitReason != ReasonRetryDelay {
		t.Fatalf("snapshot = %+v, want waiting_retry", snap)
	}
	if snap.HeadSeq != 1 || snap.HeadAttempts != 1 || snap.Backlog != 2 {
		t.Fatalf("snapshot head wrong: %+v", snap)
	}
	if snap.HeadNotBefore.Before(time.Now()) {
		t.Fatalf("HeadNotBefore should be in future: %+v", snap)
	}

	// 暂停该键：即使重试到期也不投递。观察窗口覆盖 150ms 重试到期点。
	if err := b.Pause("g", "k"); err != nil {
		t.Fatal(err)
	}
	assertNotDeliveredFor(t, func() int {
		mu.Lock()
		defer mu.Unlock()
		return seq1Deliveries
	}, 1, 220*time.Millisecond, "seq1 redelivered after retry due while paused")
	if eff.total() != 0 {
		t.Fatalf("later messages completed before paused head: total=%d", eff.total())
	}
	if got := b.KeyStatus("g", "k"); got.State != StatePaused || got.WaitReason != ReasonPaused {
		t.Fatalf("snapshot = %+v, want paused", got)
	}

	// 恢复：队头重投成功，后续按序完成。
	if err := b.Resume("g", "k"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return eff.total() == 3 }, 2*time.Second, "all done after resume")
	// 等队头 ACK 全部落盘后再核对顺序，避免读到裁决中的中间态。
	waitKeyState(t, b, "g", "k", StateDrained)
	mu.Lock()
	defer mu.Unlock()
	for i, s := range ackedSeqs {
		if s != int64(i+1) {
			t.Fatalf("out of order at %d: seq=%d", i, s)
		}
	}
}

// 暂停状态跨重启保留：重启后仍暂停，显式恢复后按序处理。
func TestPausePersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	b, _ := OpenBroker(testConfig(t, dir))

	if err := b.Pause("g", "k"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		b.Publish(context.Background(), Message{Key: "k"}, PublishOptions{})
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：组与暂停状态都应恢复。
	b2, err := OpenBroker(testConfig(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	eff := newEffects()
	var mu sync.Mutex
	var seqs []int64
	c, _ := b2.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		eff.mark(d.ID())
		mu.Lock()
		seqs = append(seqs, d.Seq())
		mu.Unlock()
		return AckDecision()
	})
	defer c.Close()

	// 仍暂停：订阅唤醒 runner 后，状态应稳定为 paused（它不会自行离开）。
	snap := waitKeyState(t, b2, "g", "k", StatePaused)
	if !snap.Paused || snap.HeadSeq != 1 || snap.Backlog != 2 {
		t.Fatalf("snapshot after restart = %+v, want paused head=1 backlog=2", snap)
	}
	// 负向断言：观察窗口内确无投递（暂停必须真的拦住投递）。
	assertNotDeliveredFor(t, eff.total, 0, 80*time.Millisecond, "delivery while paused after restart")

	// 显式恢复后按序处理。
	if err := b2.Resume("g", "k"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return eff.total() == 3 }, 2*time.Second, "processed after resume")
	mu.Lock()
	defer mu.Unlock()
	for i, s := range seqs {
		if s != int64(i+1) {
			t.Fatalf("out of order at %d: seq=%d", i, s)
		}
	}
}

// 快照各状态：处理中、无消费者、已清空、不存在；且读取不打断消费。
func TestKeyStatusStates(t *testing.T) {
	dir := t.TempDir()
	b, _ := OpenBroker(testConfig(t, dir))
	defer b.Close()

	// 不存在的组/键。
	if snap := b.KeyStatus("nope", "k"); snap.Exists || snap.State != "" {
		t.Fatalf("unknown group snapshot = %+v", snap)
	}

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	eff := newEffects()
	c, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		if d.Seq() == 1 {
			entered <- struct{}{}
			<-release // 阻塞第一条，制造“处理中”
		}
		eff.mark(d.ID())
		return AckDecision()
	})
	defer c.Close()

	b.Publish(context.Background(), Message{Key: "k"}, PublishOptions{})
	b.Publish(context.Background(), Message{Key: "k"}, PublishOptions{})

	<-entered
	snap := b.KeyStatus("g", "k")
	if snap.State != StateProcessing || snap.WaitReason != ReasonInFlight {
		t.Fatalf("snapshot = %+v, want processing", snap)
	}
	if snap.HeadSeq != 1 || snap.HeadAttempts != 1 || snap.Backlog != 1 {
		t.Fatalf("processing snapshot wrong: %+v", snap)
	}
	close(release)
	waitFor(t, func() bool { return eff.total() == 2 }, 2*time.Second, "business effects applied")
	// 业务生效先于 ACK 落盘：等快照到达 drained（ACK 已 fsync 的已提交终态），
	// 不能在 inflight->acked 窗口里直接断言。
	if got := waitKeyState(t, b, "g", "k", StateDrained); got.WaitReason != ReasonNone {
		t.Fatalf("drained snapshot = %+v", got)
	}

	// 无消费者：组存在但消费者已关闭，新消息排队等待。
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	b.Publish(context.Background(), Message{Key: "k"}, PublishOptions{})
	waitFor(t, func() bool {
		return b.KeyStatus("g", "k").State == StateNoConsumer
	}, 2*time.Second, "no_consumer state")
	snap = b.KeyStatus("g", "k")
	if snap.WaitReason != ReasonNoConsumer || snap.HeadSeq != 3 || snap.HeadAttempts != 0 {
		t.Fatalf("no_consumer snapshot wrong: %+v", snap)
	}
}

// 暂停与缩容叠加：暂停键的在途消息被收回后不重投，恢复后仍按序。
func TestPauseWithConsumerRemoval(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.ShutdownTimeout = 150 * time.Millisecond
	b, _ := OpenBroker(cfg)
	defer b.Close()

	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	eff := newEffects()
	c1, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		if d.Seq() == 1 {
			entered <- struct{}{}
			select {
			case <-block:
			case <-ctx.Done(): // 缩容超时收回时取消
				return RetryDecision(errors.New("revoked"), 0)
			}
		}
		eff.mark(d.ID())
		return AckDecision()
	})
	defer c1.Close()

	for i := 0; i < 3; i++ {
		b.Publish(context.Background(), Message{Key: "k"}, PublishOptions{})
	}
	<-entered
	// 暂停 + 缩容（在途消息将被收回为 pending）。
	if err := b.Pause("g", "k"); err != nil {
		t.Fatal(err)
	}
	if err := c1.Close(); err != nil {
		t.Fatal(err)
	}
	close(block)

	// 扩容新消费者：缩容收回 + 再均衡完成后，键仍暂停，不得投递。
	eff2 := newEffects()
	var mu2 sync.Mutex
	var seqs2 []int64
	c2, _ := b.Subscribe("g", "c2", func(ctx context.Context, d *Delivery) Decision {
		eff2.mark(d.ID())
		mu2.Lock()
		seqs2 = append(seqs2, d.Seq())
		mu2.Unlock()
		return AckDecision()
	})
	defer c2.Close()
	// 先等状态稳定为 paused（收回已完成），再在观察窗口内持续断言无投递。
	waitKeyState(t, b, "g", "k", StatePaused)
	assertNotDeliveredFor(t, eff2.total, 0, 80*time.Millisecond, "delivery while paused after rebalance")

	// 恢复后按序完成全部 3 条。
	if err := b.Resume("g", "k"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return eff2.total() == 3 }, 2*time.Second, "resumed after rebalance")
	// 全部生效后等 ACK 落盘的终态，再验证顺序与每条恰好一次。
	waitKeyState(t, b, "g", "k", StateDrained)
	mu2.Lock()
	defer mu2.Unlock()
	if len(seqs2) != 3 {
		t.Fatalf("seqs2=%v want 3 entries", seqs2)
	}
	for i, s := range seqs2 {
		if s != int64(i+1) {
			t.Fatalf("after rebalance out of order at %d: seq=%d", i, s)
		}
	}
}
