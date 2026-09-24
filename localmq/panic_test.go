package localmq

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestPanicDeadLetters: a panicking handler is recovered; the message is
// dead-lettered immediately with ReasonPanic and processing continues.
func TestPanicDeadLetters(t *testing.T) {
	dir := tempDir(t)
	b, err := New(testConfig(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	var mu sync.Mutex
	ok := 0
	_ = b.Subscribe("g", "c", func(ctx context.Context, d *Delivery) error {
		if string(d.Message.Body) == "boom" {
			panic("kaboom")
		}
		mu.Lock()
		ok++
		mu.Unlock()
		return d.Ack()
	})

	_, _ = b.Publish(context.Background(), "k", []byte("boom"), "")
	_, _ = b.Publish(context.Background(), "k", []byte("fine1"), "")
	_, _ = b.Publish(context.Background(), "k", []byte("fine2"), "")

	waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return ok == 2
	}, "two fine messages processed")

	dls := b.DeadLetters("g")
	if len(dls) != 1 {
		t.Fatalf("dead letters = %d want 1", len(dls))
	}
	if dls[0].Reason != ReasonPanic {
		t.Fatalf("reason = %q want %q", dls[0].Reason, ReasonPanic)
	}
	if string(dls[0].Message.Body) != "boom" {
		t.Fatalf("dead body = %q", dls[0].Message.Body)
	}
}
