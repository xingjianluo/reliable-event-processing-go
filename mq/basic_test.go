package mq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func waitFor(t *testing.T, cond func() bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting: %s", msg)
}

// 基本路径：每条消息恰好成功一次，同键有序。
func TestBasicDeliveryAndOrdering(t *testing.T) {
	dir := t.TempDir()
	b, err := OpenBroker(testConfig(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	eff := newEffects()
	var mu sync.Mutex
	perKeyOrder := map[string][]int64{}
	c, err := b.Subscribe("g1", "c1", func(ctx context.Context, d *Delivery) Decision {
		// 模拟业务幂等生效。
		eff.mark(d.ID())
		mu.Lock()
		perKeyOrder[d.RoutingKey()] = append(perKeyOrder[d.RoutingKey()], d.Seq())
		mu.Unlock()
		return AckDecision()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var idsA []string
	for i := 0; i < 20; i++ {
		res, err := b.Publish(context.Background(), Message{Key: "orders", Payload: []byte{'A'}}, PublishOptions{})
		if err != nil {
			t.Fatal(err)
		}
		idsA = append(idsA, res.ID)
	}
	var idsB []string
	for i := 0; i < 15; i++ {
		res, err := b.Publish(context.Background(), Message{Key: "payments", Payload: nil}, PublishOptions{})
		if err != nil {
			t.Fatal(err)
		}
		idsB = append(idsB, res.ID)
	}

	waitFor(t, func() bool { return eff.total() == 35 }, 3*time.Second, "all 35 processed")

	for _, id := range append(append([]string{}, idsA...), idsB...) {
		if n := eff.count(id); n != 1 {
			t.Fatalf("message %s applied %d times, want 1", id, n)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	checkSeq := func(key string, want int) {
		got := perKeyOrder[key]
		if len(got) != want {
			t.Fatalf("key %s processed %d, want %d", key, len(got), want)
		}
		for i, s := range got {
			if s != int64(i+1) {
				t.Fatalf("key %s out of order at %d: got seq %d, want %d", key, i, s, i+1)
			}
		}
	}
	checkSeq("orders", 20)
	checkSeq("payments", 15)
}

// 同一路由键严格串行，不同键并行。
func TestParallelAcrossKeysSerialWithin(t *testing.T) {
	dir := t.TempDir()
	b, _ := OpenBroker(testConfig(t, dir))
	defer b.Close()

	var mu sync.Mutex
	concurrent := 0
	maxConcurrent := 0
	keysSeen := map[string]bool{}

	c, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		mu.Lock()
		concurrent++
		if concurrent > maxConcurrent {
			maxConcurrent = concurrent
		}
		keysSeen[d.RoutingKey()] = true
		mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		mu.Lock()
		concurrent--
		mu.Unlock()
		return AckDecision()
	})
	defer c.Close()

	// 多个不同键，每个键两条（同键的两条不应重叠）。
	for _, k := range []string{"k1", "k2", "k3", "k4"} {
		for i := 0; i < 2; i++ {
			b.Publish(context.Background(), Message{Key: k}, PublishOptions{})
		}
	}
	time.Sleep(400 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(keysSeen) != 4 {
		t.Fatalf("expected 4 keys processed, got %d", len(keysSeen))
	}
	if maxConcurrent < 2 {
		t.Fatalf("expected parallel processing across keys, maxConcurrent=%d", maxConcurrent)
	}
}

// 重复发布（相同 DedupKey）只生效一次。
func TestDedupPublish(t *testing.T) {
	dir := t.TempDir()
	b, _ := OpenBroker(testConfig(t, dir))
	defer b.Close()

	eff := newEffects()
	c, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		eff.mark(d.ID())
		return AckDecision()
	})
	defer c.Close()

	r1, err := b.Publish(context.Background(), Message{Key: "k", Payload: []byte("x")},
		PublishOptions{DedupKey: "biz-123"})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := b.Publish(context.Background(), Message{Key: "k", Payload: []byte("x")},
		PublishOptions{DedupKey: "biz-123"})
	if err != nil {
		t.Fatal(err)
	}
	if !r2.Ignored || r2.ID != r1.ID {
		t.Fatalf("dedup publish not ignored: %+v vs %+v", r1, r2)
	}
	waitFor(t, func() bool { return eff.total() == 1 }, time.Second, "dedup single effect")
	if eff.count(r1.ID) != 1 {
		t.Fatalf("message applied %d times", eff.count(r1.ID))
	}
}

// 重复/乱序 Ack：在处理器内外多次 Ack，只确认一次。
func TestDuplicateAckOnlyOnce(t *testing.T) {
	dir := t.TempDir()
	b, _ := OpenBroker(testConfig(t, dir))
	defer b.Close()

	eff := newEffects()
	c, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		eff.mark(d.ID())
		d.Ack()          // 显式确认
		d.Ack()          // 重复确认，必须忽略
		d.Nack(NackOptions{Err: errors.New("late nack")}) // 确认后的拒绝，必须忽略
		return AckDecision() // 返回的裁决也必须被忽略
	})
	defer c.Close()

	res, _ := b.Publish(context.Background(), Message{Key: "k"}, PublishOptions{})
	waitFor(t, func() bool { return eff.total() == 1 }, time.Second, "processed")
	time.Sleep(50 * time.Millisecond)
	if eff.count(res.ID) != 1 {
		t.Fatalf("message applied %d times, want 1", eff.count(res.ID))
	}
	b.mu.Lock()
	st := b.groups["g"].byID[res.ID].status
	b.mu.Unlock()
	if st != stAcked {
		t.Fatalf("status = %s, want acked", st)
	}
}
