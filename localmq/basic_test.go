package localmq

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestBasicPublishConsume: messages on a route are processed in publish order
// by the single consumer of a group and all acknowledged.
func TestBasicPublishConsume(t *testing.T) {
	dir := tempDir(t)
	b, err := New(testConfig(t, dir))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer b.Close()

	var mu sync.Mutex
	var got []string
	err = b.Subscribe("g1", "c1", func(ctx context.Context, d *Delivery) error {
		if err := d.Ack(); err != nil {
			t.Errorf("Ack: %v", err)
		}
		mu.Lock()
		got = append(got, d.Message.RouteKey+":"+string(d.Message.Body))
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if _, err := b.Publish(ctx, "keyA", []byte{byte('0' + i)}, ""); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 10
	}, "10 consumed")

	mu.Lock()
	defer mu.Unlock()
	want := "keyA:0"
	for i := 0; i < 10; i++ {
		want = "keyA:" + string(rune('0'+i))
		if got[i] != want {
			t.Fatalf("order mismatch at %d: got %s want %s (all=%v)", i, got[i], want, got)
		}
	}
}
