package localmq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestGracefulCloseAcksInflight: a handler that is mid-flight when Close is
// called is allowed to finish; its ack persists, so a restart never
// redelivers it.
func TestGracefulCloseAcksInflight(t *testing.T) {
	dir := tempDir(t)
	b, _ := New(testConfig(t, dir))

	proceed := make(chan struct{})
	var processed int64
	_ = b.Subscribe("g", "c", func(ctx context.Context, d *Delivery) error {
		<-proceed
		atomic.AddInt64(&processed, 1)
		return d.Ack() // ack during graceful drain must succeed
	})
	_, _ = b.Publish(context.Background(), "k", nil, "g1")
	time.Sleep(30 * time.Millisecond) // delivered, now blocked

	closed := make(chan error, 1)
	go func() { closed <- b.Close() }()
	time.Sleep(30 * time.Millisecond) // Close is draining and waiting
	select {
	case <-closed:
		t.Fatal("Close returned while handler still blocked")
	default:
	}
	close(proceed) // let the in-flight handler ack

	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("graceful close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not finish after in-flight ack")
	}
	if atomic.LoadInt64(&processed) != 1 {
		t.Fatalf("processed = %d want 1", processed)
	}

	// Restart: no redelivery of the acked message.
	b2, _ := New(testConfig(t, dir))
	defer b2.Close()
	var n2 int64
	_ = b2.Subscribe("g", "c", func(ctx context.Context, d *Delivery) error {
		atomic.AddInt64(&n2, 1)
		return d.Ack()
	})
	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt64(&n2) != 0 {
		t.Fatalf("acked message redelivered %d times after graceful close", n2)
	}
}

// TestCloseTimeoutAborts: a stuck handler makes Close return an error after
// CloseTimeout and the unacked message is redelivered on restart.
func TestCloseTimeoutAborts(t *testing.T) {
	dir := tempDir(t)
	cfg := testConfig(t, dir)
	cfg.CloseTimeout = 100 * time.Millisecond
	b, _ := New(cfg)

	_ = b.Subscribe("g", "c", func(ctx context.Context, d *Delivery) error {
		<-ctx.Done() // never finishes on its own
		return ctx.Err()
	})
	_, _ = b.Publish(context.Background(), "k", nil, "stuck")
	time.Sleep(30 * time.Millisecond)

	err := b.Close()
	if err == nil {
		t.Fatal("expected close timeout error")
	}

	b2, _ := New(testConfig(t, dir))
	defer b2.Close()
	var n int64
	_ = b2.Subscribe("g", "c", func(ctx context.Context, d *Delivery) error {
		atomic.AddInt64(&n, 1)
		return d.Ack()
	})
	waitFor(t, 2*time.Second, func() bool { return atomic.LoadInt64(&n) == 1 }, "redelivered after aborted close")
}
