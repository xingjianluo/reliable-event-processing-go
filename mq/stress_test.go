package mq

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 高并发压力：大量路由键 + 动态扩缩容 + 混合失败重试，
// 最终每条消息恰好成功一次或进入死信，二者必居其一且不重复。
func TestStressScaleAndFailures(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.MaxAttempts = 3
	cfg.RetryDelay = 2 * time.Millisecond
	cfg.HandlerConcurrency = 16
	b, _ := OpenBroker(cfg)
	defer b.Close()

	eff := newEffects()
	var rejected atomic.Int64
	mkHandler := func(id int) Handler {
		_ = id
		return func(ctx context.Context, d *Delivery) Decision {
			// 不同消费者处理速度不一致（顶层 rand 并发安全）。
			time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
			// 约 10% 概率临时失败（可重试）。
			if rand.Intn(10) == 0 {
				rejected.Add(1)
				return RetryDecision(fmt.Errorf("transient-%d", d.Attempt()), time.Millisecond)
			}
			eff.mark(d.ID())
			return AckDecision()
		}
	}

	const nKeys = 24
	const perKey = 25
	const total = nKeys * perKey // 600

	var consumers []*Consumer
	addConsumer := func(id string) {
		c, err := b.Subscribe("grp", id, mkHandler(len(consumers)+1))
		if err != nil {
			t.Error(err)
			return
		}
		consumers = append(consumers, c)
	}
	addConsumer("c0")

	var publishWG sync.WaitGroup
	for k := 0; k < nKeys; k++ {
		publishWG.Add(1)
		go func(k int) {
			defer publishWG.Done()
			for i := 0; i < perKey; i++ {
				b.Publish(context.Background(),
					Message{Key: fmt.Sprintf("key-%02d", k)}, PublishOptions{})
			}
		}(k)
	}

	// 动态扩容、缩容。
	for n := 1; n <= 4; n++ {
		time.Sleep(30 * time.Millisecond)
		addConsumer(fmt.Sprintf("c%d", n))
	}
	time.Sleep(50 * time.Millisecond)
	consumers[3].Close()
	consumers[2].Close()
	time.Sleep(20 * time.Millisecond)
	addConsumer("c5")

	publishWG.Wait()

	// 等待结束：成功 + 死信 = 总数，且每条只出现一次。
	waitFor(t, func() bool {
		b.mu.Lock()
		done := eff.total() + len(b.groups["grp"].dead)
		b.mu.Unlock()
		return done == total
	}, 15*time.Second, "all messages terminal")

	b.mu.Lock()
	g := b.groups["grp"]
	success := eff.total()
	dead := len(g.dead)
	for id, n := range eff.success {
		if n != 1 {
			b.mu.Unlock()
			t.Fatalf("message %s applied %d times", id, n)
		}
	}
	deadIDs := map[string]bool{}
	for _, m := range g.dead {
		if deadIDs[m.id] {
			b.mu.Unlock()
			t.Fatalf("message %s dead-lettered more than once", m.id)
		}
		deadIDs[m.id] = true
		if eff.count(m.id) != 0 {
			b.mu.Unlock()
			t.Fatalf("message %s both succeeded and dead-lettered", m.id)
		}
	}
	b.mu.Unlock()

	if success+dead != total {
		t.Fatalf("success=%d dead=%d total=%d (must partition all messages)",
			success, dead, total)
	}
	t.Logf("STRESS success=%d dead=%d transientRejects=%d", success, dead, rejected.Load())
}
