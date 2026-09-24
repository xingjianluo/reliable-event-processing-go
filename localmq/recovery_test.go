package localmq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestRecoveryPublishedBeforeSubscribe: messages published with no consumers
// are durable; after reopen + subscribe they are all delivered.
func TestRecoveryPublishedBeforeSubscribe(t *testing.T) {
	dir := tempDir(t)
	cfg := testConfig(t, dir)
	b, _ := New(cfg)
	ctx := context.Background()
	for i := 0; i < 15; i++ {
		if _, err := b.Publish(ctx, "k", nil, "p-"+string(rune('a'+i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	b2, err := New(testConfig(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	var n int64
	_ = b2.Subscribe("g", "c", func(ctx context.Context, d *Delivery) error {
		atomic.AddInt64(&n, 1)
		return d.Ack()
	})
	waitFor(t, 2*time.Second, func() bool { return atomic.LoadInt64(&n) == 15 }, "15 after restart")
}

// TestRecoveryAckDurable: process some messages, ack them, close cleanly,
// reopen. Acknowledged messages must NOT be redelivered; only new ones are.
func TestRecoveryAckDurable(t *testing.T) {
	dir := tempDir(t)
	cfg := testConfig(t, dir)
	b, _ := New(cfg)
	var n int64
	_ = b.Subscribe("g", "c", func(ctx context.Context, d *Delivery) error {
		atomic.AddInt64(&n, 1)
		return d.Ack()
	})
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		_, _ = b.Publish(ctx, "k", nil, "a-"+itoa(i))
	}
	waitFor(t, 2*time.Second, func() bool { return atomic.LoadInt64(&n) == 10 }, "10 first run")
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	b2, _ := New(testConfig(t, dir))
	defer b2.Close()
	var n2 int64
	_ = b2.Subscribe("g", "c", func(ctx context.Context, d *Delivery) error {
		atomic.AddInt64(&n2, 1)
		return d.Ack()
	})
	// Publish one new message; only that one should be delivered.
	_, _ = b2.Publish(ctx, "k", nil, "new-1")
	waitFor(t, 2*time.Second, func() bool { return atomic.LoadInt64(&n2) == 1 }, "only new delivered")
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt64(&n2); got != 1 {
		t.Fatalf("redelivered %d already-acked messages", got-1)
	}
}

// TestRecoveryCrashInFlight simulates a process crash DURING processing: the
// handler blocks until killed, so no ack/nack is persisted. On reopen the
// message must be redelivered (at-least-once) and acked exactly once.
func TestRecoveryCrashInFlight(t *testing.T) {
	dir := tempDir(t)
	cfg := testConfig(t, dir)
	b, _ := New(cfg)

	started := make(chan string, 1)
	_ = b.Subscribe("g", "c", func(ctx context.Context, d *Delivery) error {
		started <- d.Message.ID
		<-ctx.Done() // simulate work in progress until the process dies
		return ctx.Err()
	})
	id, _ := b.Publish(context.Background(), "k", nil, "inflight-1")
	got := <-started
	if got != id {
		t.Fatalf("got %s want %s", got, id)
	}

	// Hard shutdown: do not wait for the handler; close the WAL from
	// underneath as if the process vanished. The ack never persisted.
	if err := b.store.wal.close(); err != nil {
		t.Fatal(err)
	}
	// Prevent Close from re-closing/fsync causing noise.
	b.mu.Lock()
	b.closed = true
	close(b.closeCh)
	b.mu.Unlock()

	// Verify the WAL has only the publish record (no ack) for this message.
	// Reopen and expect redelivery.
	b2, _ := New(testConfig(t, dir))
	defer b2.Close()
	var n int64
	_ = b2.Subscribe("g", "c", func(ctx context.Context, d *Delivery) error {
		atomic.AddInt64(&n, 1)
		return d.Ack()
	})
	waitFor(t, 2*time.Second, func() bool { return atomic.LoadInt64(&n) == 1 }, "redelivered after crash")
	if dls := b2.DeadLetters("g"); len(dls) != 0 {
		t.Fatalf("unexpected DLQ after in-flight crash: %d", len(dls))
	}
}

// TestRecoveryRetriesPreserved: a message in retry backoff at crash is
// redelivered after restart with its attempt count intact, and eventually
// succeeds (no infinite reset of the retry budget, no loss).
func TestRecoveryRetriesPreserved(t *testing.T) {
	dir := tempDir(t)
	cfg := testConfig(t, dir)
	cfg.MaxAttempts = 3
	b, _ := New(cfg)
	var n int64
	gate := make(chan struct{})
	_ = b.Subscribe("g", "c", func(ctx context.Context, d *Delivery) error {
		x := atomic.AddInt64(&n, 1)
		if x == 1 {
			_ = d.Nack(60 * time.Millisecond)
			// "crash" while waiting for retry: force-close storage.
			_ = b.store.wal.close()
			b.mu.Lock()
			if !b.closed {
				b.closed = true
				close(b.closeCh)
			}
			b.mu.Unlock()
			close(gate)
			return nil
		}
		return d.Ack()
	})
	_, _ = b.Publish(context.Background(), "k", nil, "retry-1")
	<-gate

	// Reopen: attempt budget must continue (this delivery is attempt 2, then
	// 3 would DLQ only if it failed; here it acks on the next attempt).
	b2, _ := New(testConfig(t, dir))
	defer b2.Close()
	var m int64
	_ = b2.Subscribe("g", "c", func(ctx context.Context, d *Delivery) error {
		atomic.AddInt64(&m, 1)
		if d.Attempt != 2 {
			t.Errorf("attempt after restart = %d want 2", d.Attempt)
		}
		return d.Ack()
	})
	waitFor(t, 2*time.Second, func() bool { return atomic.LoadInt64(&m) == 1 }, "ack after retry restart")
}

func itoa(i int) string {
	return string(rune('0' + i))
}
