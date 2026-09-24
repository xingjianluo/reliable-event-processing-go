package localmq

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// effectStore is the business-side idempotent ledger: an effect is applied
// exactly once per message ID, no matter how many attempts deliver it.
type effectStore struct {
	mu      sync.Mutex
	effects map[string]int
	order   map[string][]int // key -> applied sequence numbers
	owner   map[string]string
	dupAck  int
}

func newEffectStore() *effectStore {
	return &effectStore{
		effects: map[string]int{},
		order:   map[string][]int{},
		owner:   map[string]string{},
	}
}

// applyEffect is called after a durable Ack. It asserts the business state
// transition happens at most once.
func (e *effectStore) applyEffect(id, key, consumer string, seq int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.effects[id]; exists {
		e.dupAck++
		return
	}
	e.effects[id] = seq
	e.order[key] = append(e.order[key], seq)
	e.owner[id] = consumer
}

// TestConcurrentConsumers: 3 consumers, several route keys with many messages
// each; verifies (1) every message applies exactly once, (2) per-key business
// order equals publish order, (3) a key is handled by exactly one consumer.
func TestConcurrentConsumers(t *testing.T) {
	dir := tempDir(t)
	cfg := testConfig(t, dir)
	b, _ := New(cfg)
	defer b.Close()

	eff := newEffectStore()
	const numConsumers = 3
	for i := 0; i < numConsumers; i++ {
		name := fmt.Sprintf("c%d", i)
		err := b.Subscribe("grp", name, func(ctx context.Context, d *Delivery) error {
			// variable speed: simulate slow consumers / work
			time.Sleep(time.Duration(rand.Intn(2)) * time.Millisecond)
			// Body encodes "key-seq".
			key, seq := parseBody(string(d.Message.Body))
			eff.applyEffect(d.Message.ID, key, name, seq)
			return nil // auto-ack on successful return
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	const keys = 8
	const perKey = 25
	ctx := context.Background()
	total := 0
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("key%02d", k)
		for i := 1; i <= perKey; i++ {
			if _, err := b.Publish(ctx, key,
				[]byte(fmt.Sprintf("%s-%02d", key, i)),
				fmt.Sprintf("%s-%02d", key, i)); err != nil {
				t.Fatal(err)
			}
			total++
		}
	}

	waitFor(t, 5*time.Second, func() bool {
		eff.mu.Lock()
		defer eff.mu.Unlock()
		return len(eff.effects) == total
	}, fmt.Sprintf("%d effects", total))

	eff.mu.Lock()
	defer eff.mu.Unlock()
	if eff.dupAck != 0 {
		t.Fatalf("duplicate business effects: %d", eff.dupAck)
	}
	if len(eff.effects) != total {
		t.Fatalf("effects = %d want %d (lost messages)", len(eff.effects), total)
	}
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("key%02d", k)
		got := eff.order[key]
		if len(got) != perKey {
			t.Fatalf("key %s applied %d want %d", key, len(got), perKey)
		}
		for i, v := range got {
			if v != i+1 {
				t.Fatalf("key %s order = %v, want ascending 1..%d", key, got, perKey)
			}
		}
	}

	// Every key maps to exactly one consumer (ownership stability).
	keyOwner := map[string]string{}
	for id, c := range eff.owner {
		key, _ := parseBody(id)
		if prev, ok := keyOwner[key]; ok && prev != c {
			t.Fatalf("key %s owned by both %s and %s", key, prev, c)
		}
		keyOwner[key] = c
	}
}

func parseBody(body string) (string, int) {
	i := strings.LastIndex(body, "-")
	n, _ := strconv.Atoi(body[i+1:])
	return body[:i], n
}
