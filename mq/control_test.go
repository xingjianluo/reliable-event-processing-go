package mq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

var errTest = errors.New("test failure")

// 暂停后：该键新消息不投递，其他键不受影响；恢复后按序续投。
func TestPauseBlocksDeliveryAndResume(t *testing.T) {
	dir := t.TempDir()
	b, err := OpenBroker(testConfig(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	eff := newEffects()
	var mu sync.Mutex
	k1Seqs := []int64{}
	c, err := b.Subscribe("g1", "c1", func(ctx context.Context, d *Delivery) Decision {
		eff.mark(d.ID())
		if d.RoutingKey() == "k1" {
			mu.Lock()
			k1Seqs = append(k1Seqs, d.Seq())
			mu.Unlock()
		}
		return AckDecision()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := b.Pause("g1", "k1"); err != nil {
		t.Fatal(err)
	}
	// 幂等：重复暂停不报错。
	if err := b.Pause("g1", "k1"); err != nil {
		t.Fatal(err)
	}

	k1IDs := []string{}
	for i := 0; i < 3; i++ {
		res, err := b.Publish(context.Background(), Message{Key: "k1", Payload: []byte{'x'}}, PublishOptions{})
		if err != nil {
			t.Fatal(err)
		}
		k1IDs = append(k1IDs, res.ID)
	}
	k2IDs := []string{}
	for i := 0; i < 3; i++ {
		res, err := b.Publish(context.Background(), Message{Key: "k2", Payload: []byte{'y'}}, PublishOptions{})
		if err != nil {
			t.Fatal(err)
		}
		k2IDs = append(k2IDs, res.ID)
	}

	// 其他键正常消费，不被暂停键拖慢。
	waitFor(t, func() bool {
		for _, id := range k2IDs {
			if eff.count(id) != 1 {
				return false
			}
		}
		return true
	}, 3*time.Second, "k2 fully consumed while k1 paused")

	// 暂停键一条都不应投递；等一段时间确认稳定。
	time.Sleep(100 * time.Millisecond)
	for _, id := range k1IDs {
		if n := eff.count(id); n != 0 {
			t.Fatalf("paused key message %s delivered %d times, want 0", id, n)
		}
	}
	st := b.KeyStatus("g1", "k1")
	if st.State != StatePaused || st.Reason != ReasonPaused || st.Backlog != 3 {
		t.Fatalf("paused snapshot = %+v, want state=paused reason=paused backlog=3", st)
	}

	// 恢复后从最早未完成消息按序续投。
	if err := b.Resume("g1", "k1"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		for _, id := range k1IDs {
			if eff.count(id) != 1 {
				return false
			}
		}
		return true
	}, 3*time.Second, "k1 consumed after resume")
	mu.Lock()
	defer mu.Unlock()
	for i, s := range k1Seqs {
		if s != int64(i+1) {
			t.Fatalf("k1 order = %v, want 1,2,3", k1Seqs)
		}
	}
}

// 队头处于延期重试 + 暂停叠加：到期后仍不投递，后面消息不得先行；
// 恢复后队头先重试成功，后续消息才能按序完成。
func TestPauseWithDeferredRetryHead(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.MaxAttempts = 10
	b, err := OpenBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	eff := newEffects()
	var mu sync.Mutex
	var seqs []int64
	var firstID string
	c, err := b.Subscribe("g1", "c1", func(ctx context.Context, d *Delivery) Decision {
		mu.Lock()
		firstID = d.ID()
		mu.Unlock()
		if d.Seq() == 1 && d.Attempt() == 1 {
			// 队头一次失败后进入较长延期重试。
			return RetryDecision(errTest, 300*time.Millisecond)
		}
		eff.mark(d.ID())
		mu.Lock()
		seqs = append(seqs, d.Seq())
		mu.Unlock()
		return AckDecision()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ids := []string{}
	for i := 0; i < 3; i++ {
		res, err := b.Publish(context.Background(), Message{Key: "k1", Payload: []byte{'a'}}, PublishOptions{})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, res.ID)
	}

	// 等队头一次失败进入延期重试。
	waitFor(t, func() bool {
		st := b.KeyStatus("g1", "k1")
		return st.State == StateRetryWait && st.Head != nil && st.Head.Attempts == 1
	}, 3*time.Second, "head enters deferred retry")

	// 延期重试期间：后面消息不得先行完成。
	st := b.KeyStatus("g1", "k1")
	if st.Backlog != 3 {
		t.Fatalf("retry_wait backlog = %d, want 3", st.Backlog)
	}
	if st.Head.NextDeliveryAt.IsZero() {
		t.Fatalf("retry_wait head should have NextDeliveryAt set: %+v", st.Head)
	}
	if eff.count(ids[1]) != 0 || eff.count(ids[2]) != 0 {
		t.Fatalf("later messages completed while head in deferred retry")
	}

	// 叠加暂停：即使重试到期也不投递。
	if err := b.Pause("g1", "k1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond) // 远超 300ms 重试延迟
	if got := eff.total(); got != 0 {
		t.Fatalf("paused+expired retry: %d messages completed, want 0", got)
	}
	st = b.KeyStatus("g1", "k1")
	if st.State != StatePaused || st.Backlog != 3 || st.Head.Attempts != 1 {
		t.Fatalf("paused snapshot = %+v, want state=paused backlog=3 attempts=1", st)
	}

	// 恢复：队头先重试成功，后续按序完成。
	if err := b.Resume("g1", "k1"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return eff.total() == 3 }, 3*time.Second, "all consumed after resume")
	for _, id := range ids {
		if n := eff.count(id); n != 1 {
			t.Fatalf("message %s applied %d times, want 1", id, n)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for i, s := range seqs {
		if s != int64(i+1) {
			t.Fatalf("order = %v, want 1,2,3", seqs)
		}
	}
	_ = firstID
}

// 暂停状态跨重启保留：重启后仍不投递，直到显式恢复后按序处理。
func TestPausePersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	b, err := OpenBroker(testConfig(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	eff := newEffects()
	c, err := b.Subscribe("g1", "c1", func(ctx context.Context, d *Delivery) Decision {
		eff.mark(d.ID())
		return AckDecision()
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Pause("g1", "k1"); err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	for i := 0; i < 3; i++ {
		res, err := b.Publish(context.Background(), Message{Key: "k1", Payload: []byte{'z'}}, PublishOptions{})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, res.ID)
	}
	time.Sleep(50 * time.Millisecond)
	if got := eff.total(); got != 0 {
		t.Fatalf("paused key delivered %d messages before restart, want 0", got)
	}
	c.Close()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：暂停状态应保留，订阅后仍不投递。
	b2, err := OpenBroker(testConfig(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	st := b2.KeyStatus("g1", "k1")
	if !st.Paused || st.State != StatePaused || st.Backlog != 3 {
		t.Fatalf("after restart snapshot = %+v, want paused backlog=3", st)
	}
	eff2 := newEffects()
	var mu sync.Mutex
	var seqs []int64
	c2, err := b2.Subscribe("g1", "c1", func(ctx context.Context, d *Delivery) Decision {
		eff2.mark(d.ID())
		mu.Lock()
		seqs = append(seqs, d.Seq())
		mu.Unlock()
		return AckDecision()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	time.Sleep(100 * time.Millisecond)
	if got := eff2.total(); got != 0 {
		t.Fatalf("paused key delivered %d messages after restart, want 0", got)
	}

	// 显式恢复后按序处理。
	if err := b2.Resume("g1", "k1"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return eff2.total() == 3 }, 3*time.Second, "consumed after resume post-restart")
	for _, id := range ids {
		if n := eff2.count(id); n != 1 {
			t.Fatalf("message %s applied %d times after restart, want 1", id, n)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for i, s := range seqs {
		if s != int64(i+1) {
			t.Fatalf("order after restart = %v, want 1,2,3", seqs)
		}
	}
}

// 快照：各运行态、等待原因、队头信息与积压数量符合实际；
// 未成功消息不得显示为已处理。
func TestKeyStatusSnapshot(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.MaxRetryDelay = 5 * time.Second // 保留处理器声明的 500ms 重试延迟
	b, err := OpenBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	// 空键：已清空。
	st := b.KeyStatus("g1", "k1")
	if st.State != StateDrained || st.Reason != ReasonNone || st.Backlog != 0 || st.Head != nil {
		t.Fatalf("empty snapshot = %+v, want drained", st)
	}

	release := make(chan struct{})
	var mu sync.Mutex
	var failNext bool
	c, err := b.Subscribe("g1", "c1", func(ctx context.Context, d *Delivery) Decision {
		<-release // 阻塞处理器，使队头保持 inflight
		mu.Lock()
		fail := failNext
		failNext = false
		mu.Unlock()
		if fail {
			return RetryDecision(errTest, 500*time.Millisecond)
		}
		return AckDecision()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for i := 0; i < 3; i++ {
		if _, err := b.Publish(context.Background(), Message{Key: "k1", Payload: []byte{'p'}}, PublishOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	// 处理中：队头 inflight，积压 3。
	waitFor(t, func() bool {
		return b.KeyStatus("g1", "k1").State == StateInflight
	}, 3*time.Second, "state=inflight")
	st = b.KeyStatus("g1", "k1")
	if st.Reason != ReasonInFlight || st.Backlog != 3 || st.Head == nil ||
		st.Head.Seq != 1 || st.Head.Attempts != 1 {
		t.Fatalf("inflight snapshot = %+v, want reason=in_flight backlog=3 head.seq=1 attempts=1", st)
	}

	// 让队头失败进入延期重试。
	mu.Lock()
	failNext = true
	mu.Unlock()
	close(release)
	waitFor(t, func() bool {
		return b.KeyStatus("g1", "k1").State == StateRetryWait
	}, 3*time.Second, "state=retry_wait")
	st = b.KeyStatus("g1", "k1")
	if st.Reason != ReasonRetryBackoff || st.Backlog != 3 || st.Head.Attempts != 1 {
		t.Fatalf("retry_wait snapshot = %+v, want reason=retry_backoff backlog=3 attempts=1", st)
	}
	if st.Head.NextDeliveryAt.IsZero() ||
		st.Head.NextDeliveryAt.Before(time.Now().Add(200*time.Millisecond)) {
		t.Fatalf("retry_wait NextDeliveryAt = %v, want ~500ms ahead", st.Head.NextDeliveryAt)
	}
	if st.Head.LastError == "" {
		t.Fatalf("retry_wait head should carry LastError")
	}

	// 暂停优先于重试等待展示。
	if err := b.Pause("g1", "k1"); err != nil {
		t.Fatal(err)
	}
	st = b.KeyStatus("g1", "k1")
	if st.State != StatePaused || st.Reason != ReasonPaused || !st.Paused || st.Backlog != 3 {
		t.Fatalf("paused snapshot = %+v, want state=paused reason=paused backlog=3", st)
	}

	// 恢复并等待全部成功：已清空，未成功消息不得提前显示为已处理。
	if err := b.Resume("g1", "k1"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return b.KeyStatus("g1", "k1").State == StateDrained
	}, 3*time.Second, "state=drained")
	st = b.KeyStatus("g1", "k1")
	if st.Backlog != 0 || st.Head != nil || st.Reason != ReasonNone {
		t.Fatalf("drained snapshot = %+v, want backlog=0 head=nil", st)
	}
}

// 暂停不影响消费组扩缩容语义：缩容收回在途后，暂停键仍不投递；
// 扩容新消费者后恢复，消息按序完成。
func TestPauseWithRescale(t *testing.T) {
	dir := t.TempDir()
	b, err := OpenBroker(testConfig(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	eff := newEffects()
	handler := func(ctx context.Context, d *Delivery) Decision {
		eff.mark(d.ID())
		return AckDecision()
	}
	c1, err := b.Subscribe("g1", "c1", handler)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Pause("g1", "k1"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := b.Publish(context.Background(), Message{Key: "k1", Payload: []byte{'q'}}, PublishOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	// 缩容：暂停键的唯一消费者离开。
	if err := c1.Close(); err != nil {
		t.Fatal(err)
	}
	st := b.KeyStatus("g1", "k1")
	if !st.Paused || st.Backlog != 2 {
		t.Fatalf("after scale-down snapshot = %+v, want paused backlog=2", st)
	}
	// 扩容新消费者：暂停仍然生效。
	c2, err := b.Subscribe("g1", "c2", handler)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	time.Sleep(100 * time.Millisecond)
	if got := eff.total(); got != 0 {
		t.Fatalf("paused key delivered %d after rescale, want 0", got)
	}
	// 恢复后按序完成。
	if err := b.Resume("g1", "k1"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return eff.total() == 2 }, 3*time.Second, "consumed after resume post-rescale")
}
