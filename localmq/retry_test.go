package localmq

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestRetryExhaustion: a handler that always fails with a temporary error gets
// exactly MaxAttempts deliveries and then lands in the DLQ with
// ReasonMaxAttempts.
func TestRetryExhaustion(t *testing.T) {
	dir := tempDir(t)
	cfg := testConfig(t, dir)
	cfg.MaxAttempts = 3
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer b.Close()

	var mu sync.Mutex
	attempts := map[string]int{}
	err = b.Subscribe("g", "c", func(ctx context.Context, d *Delivery) error {
		mu.Lock()
		attempts[d.Message.ID]++
		mu.Unlock()
		// explicit temporary failure (zero -> use configured backoff)
		_ = d.Nack(0)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	id, err := b.Publish(context.Background(), "rk", []byte("x"), "")
	if err != nil {
		t.Fatal(err)
	}

	waitFor(t, 3*time.Second, func() bool {
		return len(b.DeadLetters("g")) == 1
	}, "one dead letter")

	dl := b.DeadLetters("g")[0]
	if dl.Message.ID != id {
		t.Fatalf("dl id = %s want %s", dl.Message.ID, id)
	}
	if dl.Reason != ReasonMaxAttempts {
		t.Fatalf("reason = %q want %q", dl.Reason, ReasonMaxAttempts)
	}
	if dl.Attempts != 3 {
		t.Fatalf("attempts = %d want 3", dl.Attempts)
	}
	mu.Lock()
	if attempts[id] != 3 {
		t.Fatalf("handler invocations = %d want 3", attempts[id])
	}
	mu.Unlock()

	// Settle after death must be rejected (no double effects).
	// Give scheduler time to ensure no more deliveries.
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	if attempts[id] != 3 {
		t.Fatalf("handler invoked after DLQ: %d", attempts[id])
	}
	mu.Unlock()
}

// TestRejectIsImmediateDead: Reject short-circuits retries with ReasonRejected.
func TestRejectIsImmediateDead(t *testing.T) {
	dir := tempDir(t)
	cfg := testConfig(t, dir)
	cfg.MaxAttempts = 5
	b, _ := New(cfg)
	defer b.Close()

	calls := 0
	var mu sync.Mutex
	_ = b.Subscribe("g", "c", func(ctx context.Context, d *Delivery) error {
		mu.Lock()
		calls++
		mu.Unlock()
		if err := d.Reject("bad-payload"); err != nil {
			t.Errorf("Reject: %v", err)
		}
		return nil
	})
	_, _ = b.Publish(context.Background(), "rk", []byte("y"), "")

	waitFor(t, 2*time.Second, func() bool {
		return len(b.DeadLetters("g")) == 1
	}, "dead letter")
	dl := b.DeadLetters("g")[0]
	if dl.Reason != ReasonRejected || dl.Detail != "bad-payload" || dl.Attempts != 1 {
		t.Fatalf("unexpected dl: %+v", dl)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("calls = %d want 1", calls)
	}
}
