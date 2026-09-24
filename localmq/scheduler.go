package localmq

import (
	"context"
	"hash/fnv"
	"time"
)

// scheduler owns dispatch for one consumer group.
//
// Ordering model: per route key, messages become eligible strictly in publish
// sequence (store.pending is sorted key->seq), and a key is marked busy for
// the whole interval between dispatch and the first durable settlement. The
// scheduler never dispatches a busy key, so at most one delivery per key is
// outstanding at any moment: the message behind a slow message for the same
// key cannot overtake it. Retries stay on the same ordering track (the key
// stays busy during the retry backoff).
//
// All busyKeys membership is mutated by this single scheduler goroutine;
// handler goroutines only send completion events on finishCh, so the map is
// never accessed concurrently.
type scheduler struct {
	b     *Broker
	group string
	gr    *groupRuntime

	stopCh   chan struct{}
	doneCh   chan struct{}
	finishCh chan string // completed route keys, returned by handlers
}

func newScheduler(b *Broker, group string, gr *groupRuntime) *scheduler {
	return &scheduler{
		b: b, group: group, gr: gr,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
		finishCh: make(chan string, 256),
	}
}

func (s *scheduler) start() { go s.run() }

func (s *scheduler) stop() {
	select {
	case <-s.stopCh:
	default:
		close(s.stopCh)
	}
	<-s.doneCh
}

func (s *scheduler) run() {
	defer close(s.doneCh)
	ticker := time.NewTicker(s.b.cfg.ScheduleTick)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case key := <-s.finishCh:
			s.releaseKey(key)
			s.tick()
		case <-s.gr.notifyCh:
			s.tick()
		case <-ticker.C:
			s.tick()
		}
	}
}

func (s *scheduler) releaseKey(key string) {
	s.gr.mu.Lock()
	delete(s.gr.busyKeys, key)
	s.gr.mu.Unlock()
}

// tick performs one scan-and-dispatch pass. It runs only on the scheduler
// goroutine. Members and busy keys are read under gr.mu; every busy-key
// transition happens here. The pass is a single forward scan so a consumer
// with a full pipeline cannot starve later route keys.
func (s *scheduler) tick() {
	type job struct {
		owner *Consumer
		d     *Delivery
		key   string
	}
	var jobs []job

	s.gr.mu.Lock()
	if len(s.gr.consumers) != 0 {
		members := make([]*Consumer, len(s.gr.ordered))
		copy(members, s.gr.ordered)
		now := time.Now().UnixNano()
		items := s.b.store.pending(s.group)
		skippedKeys := map[string]bool{}
		for _, it := range items {
			key := it.Msg.RouteKey
			if s.gr.busyKeys[key] || skippedKeys[key] {
				continue
			}
			if it.NotBefore > now {
				// Earliest pending message of the key waits for backoff; the
				// rest of the same key waits behind it.
				skippedKeys[key] = true
				continue
			}
			owner := assignConsumer(members, key)
			if owner == nil || !owner.tryAcquire() {
				// No owner or pipeline full: skip the rest of this key this
				// pass but keep scanning other keys.
				skippedKeys[key] = true
				continue
			}
			s.gr.busyKeys[key] = true
			jobs = append(jobs, job{owner: owner, d: &Delivery{
				Message:  it.Msg,
				Attempt:  it.Attempts + 1,
				Group:    s.group,
				Consumer: owner.name,
			}, key: key})
		}
	}
	s.gr.mu.Unlock()

	for _, jb := range jobs {
		jb.owner.inFlight.add(1)
		go s.handle(jb.owner, jb.d, jb.key)
	}
}

func (s *scheduler) handle(owner *Consumer, d *Delivery, key string) {
	defer owner.inFlight.done()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-s.b.abortCh:
			// Only a forced shutdown cancels cooperative handlers. During a
			// normal graceful drain they are left free to finish and ack.
			cancel()
		case <-ctx.Done():
		}
	}()
	settle := func(group, id string, a settleAction) error {
		return s.b.settlePersist(group, id, a)
	}
	owner.process(ctx, d, settle, func() {
		// Release the key back on the scheduler goroutine only.
		select {
		case s.finishCh <- key:
		case <-s.stopCh:
		}
	})
}

// assignConsumer maps a route key to exactly one live consumer using a stable
// hash over the (sorted) members. Membership changes only remap keys at hash
// boundaries; there is never more than one owner for a key at an instant, and
// in-flight keys are blocked by busyKeys during rebalance.
func assignConsumer(members []*Consumer, key string) *Consumer {
	if len(members) == 0 {
		return nil
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return members[int(h.Sum32()%uint32(len(members)))]
}
