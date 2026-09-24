package localmq

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestOutOfOrderAck: independent route keys can complete out of order (the
// later-finishing message must not block other keys), while every key still
// processes its own messages in order exactly once.
func TestOutOfOrderAck(t *testing.T) {
	dir := tempDir(t)
	cfg := testConfig(t, dir)
	b, _ := New(cfg)
	defer b.Close()

	var mu sync.Mutex
	finished := map[string][]string{} // key -> message ids in completion order
	release := make(chan struct{})

	_ = b.Subscribe("g", "c", func(ctx context.Context, d *Delivery) error {
		// Make the message on "slow" key block until others are done.
		if d.Message.RouteKey == "slow" {
			<-release
		}
		if err := d.Ack(); err != nil {
			return err
		}
		mu.Lock()
		finished[d.Message.RouteKey] = append(finished[d.Message.RouteKey], d.Message.ID)
		mu.Unlock()
		return nil
	}, WithConcurrency(8))

	ctx := context.Background()
	// slow published FIRST but must not block the fast keys.
	_, _ = b.Publish(ctx, "slow", nil, "slow-1")
	for i := 1; i <= 4; i++ {
		_, _ = b.Publish(ctx, "fast", nil, idFor("fast", i))
	}

	// Fast messages drain while slow is held.
	waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(finished["fast"]) == 4
	}, "fast keys finish before slow released")

	mu.Lock()
	if len(finished["slow"]) != 0 {
		mu.Unlock()
		t.Fatal("slow key should still be blocked")
	}
	mu.Unlock()
	close(release)

	waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(finished["slow"]) == 1
	}, "slow finishes after release")

	mu.Lock()
	defer mu.Unlock()
	// Fast key preserved its own per-key order.
	want := []string{"fast-1", "fast-2", "fast-3", "fast-4"}
	for i, id := range finished["fast"] {
		if id != want[i] {
			t.Fatalf("fast order = %v", finished["fast"])
		}
	}
}

func idFor(key string, i int) string {
	return key + "-" + string(rune('0'+i))
}
