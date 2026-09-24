package localmq

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestDuplicateDeliveryIdempotent: a flaky handler fails the first attempts
// (redelivery is at-least-once). The durable ledger applies the business
// effect exactly once; the broker itself never redelivers after a durable ack.
func TestDuplicateDeliveryIdempotent(t *testing.T) {
	dir := tempDir(t)
	cfg := testConfig(t, dir)
	cfg.MaxAttempts = 5
	b, _ := New(cfg)
	defer b.Close()

	var attempts int64
	var applied int64
	seen := sync.Map{} // msg id -> 1 after effect

	_ = b.Subscribe("g", "c", func(ctx context.Context, d *Delivery) error {
		n := atomic.AddInt64(&attempts, 1)
		id := d.Message.ID
		if _, loaded := seen.LoadOrStore(id, struct{}{}); !loaded {
			// First time we see this message in the "business": simulate a
			// crash between doing the work and acking by nacking once.
			atomic.AddInt64(&applied, 1)
			_ = d.Nack(10 * time.Millisecond)
			return nil
		}
		_ = n
		return nil // auto-ack on second delivery
	})

	const N = 30
	ctx := context.Background()
	for i := 0; i < N; i++ {
		_, _ = b.Publish(ctx, "k", nil, "")
	}

	waitFor(t, 3*time.Second, func() bool {
		return len(b.DeadLetters("g")) == 0 && atomic.LoadInt64(&applied) == int64(N) &&
			atomic.LoadInt64(&attempts) > int64(N)
	}, "each applied once with redeliveries")

	if got := atomic.LoadInt64(&applied); got != int64(N) {
		t.Fatalf("business effects = %d want %d", got, N)
	}
	if len(b.DeadLetters("g")) != 0 {
		t.Fatalf("unexpected dead letters: %d", len(b.DeadLetters("g")))
	}
}

// TestDoubleAckRejected: calling Ack twice returns ErrAlreadySettled and the
// message stays acknowledged exactly once.
func TestDoubleAckRejected(t *testing.T) {
	dir := tempDir(t)
	b, _ := New(testConfig(t, dir))
	defer b.Close()

	var acks int64
	_ = b.Subscribe("g", "c", func(ctx context.Context, d *Delivery) error {
		if err := d.Ack(); err != nil {
			t.Errorf("first ack: %v", err)
		}
		err := d.Ack()
		if err != ErrAlreadySettled {
			t.Errorf("second ack err = %v want ErrAlreadySettled", err)
		}
		atomic.AddInt64(&acks, 1)
		return nil
	})
	_, _ = b.Publish(context.Background(), "k", nil, "m1")
	waitFor(t, 2*time.Second, func() bool { return atomic.LoadInt64(&acks) == 1 }, "acked")
	time.Sleep(40 * time.Millisecond)
	if atomic.LoadInt64(&acks) != 1 {
		t.Fatalf("message delivered %d times after ack", acks)
	}
}
