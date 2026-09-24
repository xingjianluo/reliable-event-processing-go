package localmq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestScaleUpDown: consumers join and leave while messages flow. Every
// message is applied exactly once; no message is lost across rebalances.
func TestScaleUpDown(t *testing.T) {
	dir := tempDir(t)
	cfg := testConfig(t, dir)
	b, _ := New(cfg)
	defer b.Close()

	var applied sync.Map // id -> consumer
	var dup int64
	handler := func(name string) HandlerFunc {
		return func(ctx context.Context, d *Delivery) error {
			time.Sleep(3 * time.Millisecond)
			if err := d.Ack(); err != nil {
				return err
			}
			if _, loaded := applied.LoadOrStore(d.Message.ID, name); loaded {
				atomic.AddInt64(&dup, 1)
			}
			return nil
		}
	}

	if err := b.Subscribe("g", "c1", handler("c1")); err != nil {
		t.Fatal(err)
	}

	const total = 120
	publishDone := make(chan struct{})
	go func() {
		defer close(publishDone)
		ctx := context.Background()
		for i := 0; i < total; i++ {
			_, _ = b.Publish(ctx, fmt.Sprintf("k%02d", i%6), nil, fmt.Sprintf("m%03d", i))
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// Add two consumers while traffic flows.
	time.Sleep(60 * time.Millisecond)
	if err := b.Subscribe("g", "c2", handler("c2")); err != nil {
		t.Fatal(err)
	}
	if err := b.Subscribe("g", "c3", handler("c3")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
	// Remove c1 (takes its in-flight work with it; handoff on unacked msgs).
	if err := b.Unsubscribe("g", "c1"); err != nil {
		t.Fatal(err)
	}
	<-publishDone

	waitFor(t, 5*time.Second, func() bool {
		n := 0
		applied.Range(func(_, _ any) bool { n++; return true })
		return n == total
	}, "all applied after rebalance")

	n := 0
	applied.Range(func(_, _ any) bool { n++; return true })
	if n != total {
		t.Fatalf("applied = %d want %d (lost=%d)", n, total, total-n)
	}
	if d := atomic.LoadInt64(&dup); d != 0 {
		t.Fatalf("duplicate effects: %d", d)
	}
	if dls := b.DeadLetters("g"); len(dls) != 0 {
		t.Fatalf("unexpected dead letters: %d", len(dls))
	}
}

// TestUnsubscribeWaitsInflight: Unsubscribe blocks until a handler that is
// mid-flight has finished and persisted its ack.
func TestUnsubscribeWaitsInflight(t *testing.T) {
	dir := tempDir(t)
	b, _ := New(testConfig(t, dir))
	defer b.Close()

	release := make(chan struct{})
	acked := make(chan struct{})
	_ = b.Subscribe("g", "leaver", func(ctx context.Context, d *Delivery) error {
		<-release
		if err := d.Ack(); err != nil {
			return err
		}
		close(acked)
		return nil
	})
	_, _ = b.Publish(context.Background(), "k", nil, "m")
	time.Sleep(30 * time.Millisecond) // ensure delivered & blocked

	unsubDone := make(chan error, 1)
	go func() { unsubDone <- b.Unsubscribe("g", "leaver") }()
	select {
	case <-unsubDone:
		t.Fatal("Unsubscribe returned before in-flight handler finished")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-unsubDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Unsubscribe did not return after handler finished")
	}
	select {
	case <-acked:
	default:
		t.Fatal("ack not persisted during unsubscribe")
	}
}
