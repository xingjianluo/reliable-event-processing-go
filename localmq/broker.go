package localmq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Broker is an in-process, locally persisted reliable message broker.
type Broker struct {
	cfg   Config
	store *store

	mu        sync.Mutex
	closed    bool
	closeCh   chan struct{} // closed once graceful shutdown begins (no new dispatch)
	abortCh   chan struct{} // closed only if in-flight handlers overrun CloseTimeout
	groups    map[string]*groupRuntime
	closeOnce sync.Once
}

// closedAborted reports whether shutdown forcefully aborted in-flight work.
func (b *Broker) closedAborted() bool {
	select {
	case <-b.abortCh:
		return true
	default:
		return false
	}
}

type groupRuntime struct {
	mu        sync.Mutex
	scheduler *scheduler
	consumers map[string]*Consumer
	ordered   []*Consumer // consumers sorted by name (stable hash assignment)

	// busyKeys holds keys with an unacknowledged delivery. Its presence is the
	// single ordering gate: the scheduler never sends a key's next message
	// while the key stays here.
	busyKeys map[string]bool

	notifyCh chan struct{} // scheduler wake-up (capacity 1)
}

// New creates (or reopens) a broker rooted in cfg.Dir and replays history.
func New(cfg Config) (*Broker, error) {
	cfg.normalize()
	st, err := openStore(cfg.Dir, cfg.Fsync, cfg.RetryBaseDelay, cfg.RetryMaxDelay)
	if err != nil {
		return nil, err
	}
	b := &Broker{
		cfg:     cfg,
		store:   st,
		closeCh: make(chan struct{}),
		abortCh: make(chan struct{}),
		groups:  make(map[string]*groupRuntime),
	}
	// Restart groups that have history so their dead letters/pending state are
	// observable and they are ready for subscribers.
	for _, g := range st.groupNames() {
		b.ensureGroup(g)
	}
	return b, nil
}

func (b *Broker) ensureGroup(name string) *groupRuntime {
	gr, ok := b.groups[name]
	if !ok {
		gr = &groupRuntime{
			consumers: make(map[string]*Consumer),
			busyKeys:  make(map[string]bool),
			notifyCh:  make(chan struct{}, 1),
		}
		gr.scheduler = newScheduler(b, name, gr)
		b.groups[name] = gr
		gr.scheduler.start()
	}
	return gr
}

// Publish stores a message durably and wakes schedulers.
func (b *Broker) Publish(ctx context.Context, routeKey string, body []byte, id string) (string, error) {
	if routeKey == "" {
		return "", errors.New("localmq: empty route key")
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return "", ErrBrokerClosed
	}
	b.mu.Unlock()

	if id == "" {
		id = newID()
	}
	msg := &Message{ID: id, RouteKey: routeKey, Body: append([]byte(nil), body...),
		CreatedAt: time.Now().UnixNano()}
	if err := b.store.publish(msg); err != nil {
		return "", err
	}
	b.cfg.Logger.Info("published", "msg_id", id, "route", routeKey, "state", "published")
	b.notifyAll()
	return id, nil
}

// Subscribe joins a consumer to a named group with a message handler.
func (b *Broker) Subscribe(group, consumerName string, h HandlerFunc, opts ...ConsumerOption) error {
	if group == "" || consumerName == "" {
		return errors.New("localmq: group and consumer names must be non-empty")
	}
	if h == nil {
		return errors.New("localmq: nil handler")
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrBrokerClosed
	}
	gr := b.ensureGroup(group)
	b.mu.Unlock()

	gr.mu.Lock()
	defer gr.mu.Unlock()
	if _, dup := gr.consumers[consumerName]; dup {
		return fmt.Errorf("localmq: consumer %q already in group %q", consumerName, group)
	}
	c := newConsumer(group, consumerName, h, b.cfg.Logger, opts...)
	gr.consumers[consumerName] = c
	gr.ordered = append(gr.ordered, c)
	sort.Slice(gr.ordered, func(i, j int) bool { return gr.ordered[i].name < gr.ordered[j].name })
	b.cfg.Logger.Info("subscribed", "group", group, "consumer", consumerName, "state", "joined")
	gr.signal()
	return nil
}

// Unsubscribe removes a consumer, blocking until its in-flight handlers have
// finished and their outcomes persisted. Their keys then become eligible again
// and the remaining consumers pick them up.
func (b *Broker) Unsubscribe(group, consumerName string) error {
	b.mu.Lock()
	gr := b.groups[group]
	b.mu.Unlock()
	if gr == nil {
		return fmt.Errorf("localmq: unknown group %q", group)
	}
	return gr.removeConsumer(b, consumerName)
}

func (gr *groupRuntime) removeConsumer(b *Broker, name string) error {
	gr.mu.Lock()
	c := gr.consumers[name]
	if c == nil {
		gr.mu.Unlock()
		return fmt.Errorf("localmq: unknown consumer %q", name)
	}
	delete(gr.consumers, name)
	gr.ordered = gr.ordered[:0]
	for _, k := range sortedConsumers(gr.consumers) {
		gr.ordered = append(gr.ordered, gr.consumers[k])
	}
	gr.mu.Unlock()

	// Wait for handlers currently owned by this consumer. Their settlements
	// are persisted before process() returns; afterwards the keys they held
	// become eligible again and are reassigned to remaining consumers.
	ctx, cancel := context.WithTimeout(context.Background(), b.cfg.CloseTimeout)
	defer cancel()
	if err := c.inFlight.wait(ctx); err != nil {
		return fmt.Errorf("localmq: unsubscribe %q timed out: %w", name, err)
	}
	b.cfg.Logger.Info("unsubscribed", "group", gr.scheduler.group, "consumer", name, "state", "left")
	gr.signal()
	return nil
}

// DeadLetters returns all dead letters recorded for a group.
func (b *Broker) DeadLetters(group string) []*DeadLetter {
	return b.store.deadLetters(group)
}

// settlePersist is the single path used by handlers to persist outcomes. It is
// safe for concurrent use; the first terminal transition for a message wins
// and later ones receive ErrAlreadySettled semantics via ErrTerminal.
func (b *Broker) settlePersist(group, id string, a settleAction) error {
	// In-flight handlers are allowed to persist their outcome during the
	// graceful drain window. Only an aborted shutdown (timeout / hard kill)
	// rejects settlement; the message is then redelivered on restart.
	if b.closedAborted() {
		return ErrBrokerClosed
	}
	_, err := b.store.settle(group, id, a, time.Now().UnixNano(), b.cfg.MaxAttempts)
	if err != nil {
		if errors.Is(err, ErrTerminal) {
			return ErrAlreadySettled
		}
		return err
	}
	b.mu.Lock()
	gr := b.groups[group]
	b.mu.Unlock()
	if gr != nil {
		gr.signal()
	}
	return nil
}

func (b *Broker) notifyAll() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, gr := range b.groups {
		gr.signal()
	}
}

func (gr *groupRuntime) signal() {
	select {
	case gr.notifyCh <- struct{}{}:
	default:
	}
}

// Close performs an explicit, explainable shutdown:
//  1. closeCh is closed: schedulers stop, so no new deliveries are started.
//  2. In-flight handlers keep running and CAN still Ack/Nack (their records
//     are fsynced); Publish/Subscribe now return ErrBrokerClosed.
//  3. It waits up to CloseTimeout for every in-flight handler to finish.
//  4. On timeout abortCh is closed: handler contexts are canceled, their
//     late settlements are rejected, and Close returns an error. Those
//     messages have no terminal record and are redelivered on next Open
//     (never silently lost, never falsely marked succeeded).
func (b *Broker) Close() error {
	var firstErr error
	b.closeOnce.Do(func() {
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return
		}
		b.closed = true
		groups := make([]*groupRuntime, 0, len(b.groups))
		for _, gr := range b.groups {
			groups = append(groups, gr)
		}
		close(b.closeCh) // stop new dispatch; in-flight work is NOT canceled yet
		b.mu.Unlock()

		for _, gr := range groups {
			gr.scheduler.stop()
		}

		ctx, cancel := context.WithTimeout(context.Background(), b.cfg.CloseTimeout)
		defer cancel()
		timedOut := false
		for _, gr0 := range groups {
			gr0.mu.Lock()
			cs := make([]*Consumer, 0, len(gr0.consumers))
			for _, c0 := range gr0.consumers {
				cs = append(cs, c0)
			}
			gr0.mu.Unlock()
			for _, c0 := range cs {
				if err := c0.inFlight.wait(ctx); err != nil {
					timedOut = true
				}
			}
		}
		if timedOut {
			close(b.abortCh) // force-cancel stuck handlers; reject late settles
			b.cfg.Logger.Error("close timed out waiting for in-flight handlers",
				"state", "close_timeout",
				"consequence", "unacked messages will be redelivered after restart")
			firstErr = errors.New("localmq: close timeout, in-flight handlers aborted and will be redelivered after restart")
		}
		if err := b.store.wal.close(); err != nil && firstErr == nil {
			firstErr = err
		}
		b.cfg.Logger.Info("closed", "state", "closed", "graceful", !timedOut)
	})
	return firstErr
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func sortedConsumers(m map[string]*Consumer) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
