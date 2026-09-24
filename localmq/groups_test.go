package localmq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestConsumerGroupsIndependent: two groups independently consume the same
// publish stream; one group dead-letters a message while the other acks it,
// and the DLQs stay isolated.
func TestConsumerGroupsIndependent(t *testing.T) {
	dir := tempDir(t)
	cfg := testConfig(t, dir)
	cfg.MaxAttempts = 2
	b, _ := New(cfg)
	defer b.Close()

	var gAcks int64
	_ = b.Subscribe("groupA", "a1", func(ctx context.Context, d *Delivery) error {
		atomic.AddInt64(&gAcks, 1)
		return d.Ack()
	})
	_ = b.Subscribe("groupB", "b1", func(ctx context.Context, d *Delivery) error {
		return d.Reject("groupB never wants this")
	})

	_, _ = b.Publish(context.Background(), "k", nil, "shared")
	waitFor(t, 2*time.Second, func() bool {
		return atomic.LoadInt64(&gAcks) == 1 && len(b.DeadLetters("groupB")) == 1
	}, "A acked, B dead-lettered")

	if len(b.DeadLetters("groupA")) != 0 {
		t.Fatalf("groupA DLQ = %d want 0", len(b.DeadLetters("groupA")))
	}
	dl := b.DeadLetters("groupB")[0]
	if dl.Reason != ReasonRejected || dl.Message.ID != "shared" {
		t.Fatalf("groupB DLQ wrong: %+v", dl)
	}
}
