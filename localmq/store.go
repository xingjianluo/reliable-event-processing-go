package localmq

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// PendingItem is a message that is not yet terminal in a group, presented in
// scheduling order: per route key sequence, ready-time filtered by the caller.
type PendingItem struct {
	Msg       *Message
	Seq       int64
	Attempts  int   // attempts already finished
	NotBefore int64 // Unix nano; 0 means ready now
}

// ErrTerminal is returned when Ack/Nack targets a message that is already
// acknowledged or dead-lettered in the target group.
var ErrTerminal = errors.New("localmq: message already terminal")

// store keeps messages once (global) and per-group progress in memory, rebuilt
// by replaying the WAL. Every mutating call appends(+fsyncs) before mutating
// memory, so in-memory state never runs ahead of durable history.
type store struct {
	mu       sync.Mutex
	wal      *wal
	messages map[string]*Message
	order    []string // message IDs in publish order
	keySeq   map[string]int64
	seqByID  map[string]int64
	groups   map[string]*groupState

	retryBase time.Duration
	retryMax  time.Duration
}

type msgState int

const (
	statePending msgState = iota
	stateAcked
	stateDead
)

type groupMsg struct {
	state     msgState
	seq       int64 // 1-based per-key publish position
	attempts  int   // number of finished (nacked) attempts
	notBefore int64 // earliest next-attempt time (retry backoff)
	key       string
	dead      *DeadLetter
}

type groupState struct {
	name string
	msgs map[string]*groupMsg
	// deadOrder preserves dead-letter insertion order.
	deadOrder []*DeadLetter
}

func openStore(dir string, fsyncEnabled bool, retryBase, retryMax time.Duration) (*store, error) {
	w, err := openWAL(dir, fsyncEnabled)
	if err != nil {
		return nil, err
	}
	s := &store{
		wal:       w,
		messages:  make(map[string]*Message),
		keySeq:    make(map[string]int64),
		seqByID:   make(map[string]int64),
		groups:    make(map[string]*groupState),
		retryBase: retryBase,
		retryMax:  retryMax,
	}
	if err := s.replay(); err != nil {
		_ = w.close()
		return nil, err
	}
	return s, nil
}

func (s *store) replay() error {
	return s.wal.replay(func(_ int64, payload []byte) error {
		e, err := unmarshalEvent(payload)
		if err != nil {
			return err
		}
		s.apply(e, true)
		return nil
	})
}

// apply mutates memory to reflect one event. On the replay path unknown
// referenced messages never occur (publish precedes group events in the log).
func (s *store) apply(e *event, replay bool) {
	switch e.Type {
	case evPublished:
		r := e.Pub
		if _, ok := s.messages[r.ID]; ok {
			return // idempotent under replay
		}
		s.keySeq[r.RouteKey]++
		seq := s.keySeq[r.RouteKey]
		s.seqByID[r.ID] = seq
		msg := &Message{ID: r.ID, RouteKey: r.RouteKey, Body: r.Body, CreatedAt: r.CreatedAt}
		s.messages[r.ID] = msg
		s.order = append(s.order, r.ID)
	case evAcked:
		gm := s.groupMsg(e.Ack.Group, e.Ack.ID)
		gm.state = stateAcked
	case evNacked:
		gm := s.groupMsg(e.Nack.Group, e.Nack.ID)
		gm.state = statePending
		gm.attempts++
		gm.notBefore = e.Nack.DelayUntil
	case evDead:
		gm := s.groupMsg(e.Dead.Group, e.Dead.ID)
		gm.state = stateDead
		gm.attempts = e.Dead.Attempts
		dl := &DeadLetter{
			Message:  s.messages[e.Dead.ID],
			Group:    e.Dead.Group,
			Reason:   e.Dead.Reason,
			Detail:   e.Dead.Detail,
			Attempts: e.Dead.Attempts,
			DeadAt:   e.Dead.At,
		}
		gm.dead = dl
		s.groups[e.Dead.Group].deadOrder = append(s.groups[e.Dead.Group].deadOrder, dl)
	}
}

// groupMsg returns (creating if needed) the per-group record for a message.
func (s *store) groupMsg(group, id string) *groupMsg {
	g, ok := s.groups[group]
	if !ok {
		g = &groupState{name: group, msgs: make(map[string]*groupMsg)}
		s.groups[group] = g
	}
	gm, ok := g.msgs[id]
	if !ok {
		msg := s.messages[id]
		gm = &groupMsg{key: msg.RouteKey, seq: s.seqByID[id]}
		g.msgs[id] = gm
	}
	return gm
}

func (s *store) publish(msg *Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.messages[msg.ID]; exists {
		return fmt.Errorf("localmq: duplicate message id %q", msg.ID)
	}
	e := &event{Type: evPublished, Pub: &pubRec{
		ID: msg.ID, RouteKey: msg.RouteKey, Body: msg.Body, CreatedAt: msg.CreatedAt,
	}}
	payload, err := marshalEvent(e)
	if err != nil {
		return err
	}
	if _, err := s.wal.append(payload); err != nil {
		return err
	}
	s.apply(e, false)
	return nil
}

// settle persists and applies one delivery outcome for group/id. The broker is
// responsible for deciding attempt limits, backoff and dead-letter reasons;
// store performs the transition atomically vs. other settles and rejects
// double settlement.
type settleResult struct {
	gm        *groupMsg
	wasFirst  bool
	backoffTo int64
}

func (s *store) settle(group, id string, a settleAction, now int64, maxAttempts int) (*groupMsg, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.groups[group]
	if !ok {
		g = &groupState{name: group, msgs: make(map[string]*groupMsg)}
		s.groups[group] = g
	}
	gm, ok := g.msgs[id]
	if !ok {
		msg := s.messages[id]
		if msg == nil {
			return nil, fmt.Errorf("localmq: settle unknown message %q", id)
		}
		gm = &groupMsg{key: msg.RouteKey, seq: s.keySeq[msg.RouteKey]}
		g.msgs[id] = gm
	}
	if gm.state != statePending {
		return nil, ErrTerminal
	}

	switch a.kind {
	case settleAck:
		e := &event{Type: evAcked, Ack: &ackRec{Group: group, ID: id, At: now}}
		if err := s.appendApply(e); err != nil {
			return nil, err
		}
	case settleReject:
		gm.attempts++
		e := s.deadEvent(group, id, ReasonRejected, a.reason, gm.attempts, now)
		if err := s.appendApply(e); err != nil {
			return nil, err
		}
	case settlePanic:
		gm.attempts++
		e := s.deadEvent(group, id, ReasonPanic, a.reason, gm.attempts, now)
		if err := s.appendApply(e); err != nil {
			return nil, err
		}
	case settleNack:
		// attempts is the count of already-finished attempts; this failing
		// attempt number is attempts+1.
		thisAttempt := gm.attempts + 1
		if thisAttempt >= maxAttempts {
			detail := a.reason
			e := s.deadEvent(group, id, ReasonMaxAttempts, detail, thisAttempt, now)
			if err := s.appendApply(e); err != nil {
				return nil, err
			}
			break
		}
		// gm.attempts is incremented when apply records the nack event.
		delay := a.retryAfter
		if delay <= 0 {
			delay = s.backoff(thisAttempt)
		}
		until := now + int64(delay)
		e := &event{Type: evNacked, Nack: &nackRec{
			Group: group, ID: id, At: now, DelayUntil: until,
		}}
		if err := s.appendApply(e); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("localmq: unknown settle kind %d", a.kind)
	}
	return gm, nil
}

func (s *store) deadEvent(group, id string, reason ReasonCode, detail string, attempts int, now int64) *event {
	return &event{Type: evDead, Dead: &deadRec{
		Group: group, ID: id, Reason: reason, Detail: detail, Attempts: attempts, At: now,
	}}
}

// backoff returns the delay before the attempt following thisAttempt.
// Deterministic exponential: base * 2^(thisAttempt-1), capped at retryMax.
func (s *store) backoff(thisAttempt int) time.Duration {
	d := s.retryBase
	for i := 1; i < thisAttempt; i++ {
		d *= 2
		if d >= s.retryMax {
			return s.retryMax
		}
	}
	if d > s.retryMax {
		d = s.retryMax
	}
	return d
}

func (s *store) appendApply(e *event) error {
	payload, err := marshalEvent(e)
	if err != nil {
		return err
	}
	if _, err := s.wal.append(payload); err != nil {
		return err
	}
	s.apply(e, false)
	return nil
}

// pending returns non-terminal messages of a group, sorted by route key then
// per-key sequence, so the scheduler walks each key's messages in order.
func (s *store) pending(group string) []PendingItem {
	s.mu.Lock()
	defer s.mu.Unlock()
	// A group with no state at all (brand new subscription) sees every
	// published message as pending. A message with no per-group record is
	// either never delivered or was in-flight when a process died (no
	// nack/ack record): it must be redelivered as well.
	g, hasGroup := s.groups[group]
	out := make([]PendingItem, 0)
	for _, id := range s.order {
		var gm *groupMsg
		seen := false
		if hasGroup {
			gm, seen = g.msgs[id]
		}
		if seen && gm.state != statePending {
			continue
		}
		item := PendingItem{Msg: s.messages[id], Seq: s.seqByID[id]}
		if seen {
			item.Attempts = gm.attempts
			item.NotBefore = gm.notBefore
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Msg.RouteKey != out[j].Msg.RouteKey {
			return out[i].Msg.RouteKey < out[j].Msg.RouteKey
		}
		return out[i].Seq < out[j].Seq
	})
	return out
}

func (s *store) message(id string) *Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.messages[id]
}

func (s *store) deadLetters(group string) []*DeadLetter {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.groups[group]
	if !ok {
		return nil
	}
	out := make([]*DeadLetter, len(g.deadOrder))
	copy(out, g.deadOrder)
	return out
}

func (s *store) groupNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.groups))
	for n := range s.groups {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
