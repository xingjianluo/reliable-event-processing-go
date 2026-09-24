package localmq

import (
	"encoding/json"
	"fmt"
)

// Event type tags used inside the write-ahead log.
const (
	evPublished = 1
	evAcked     = 2
	evNacked    = 3
	evDead      = 4
)

// event is the persisted record. Exactly one log file holds all groups;
// group identity is carried on per-group events.
type event struct {
	Type   int      `json:"type"`
	Offset int64    `json:"-"` // assigned when read back
	Pub    *pubRec  `json:"pub,omitempty"`
	Ack    *ackRec  `json:"ack,omitempty"`
	Nack   *nackRec `json:"nack,omitempty"`
	Dead   *deadRec `json:"dead,omitempty"`
}

type pubRec struct {
	ID        string `json:"id"`
	RouteKey  string `json:"route"`
	Body      []byte `json:"body"`
	CreatedAt int64  `json:"at"`
}

type ackRec struct {
	Group string `json:"g"`
	ID    string `json:"id"`
	At    int64  `json:"at"`
}

type nackRec struct {
	Group string `json:"g"`
	ID    string `json:"id"`
	At    int64  `json:"at"`
	// DelayUntil is the earliest Unix-nano time the next attempt may run.
	DelayUntil int64 `json:"until"`
}

type deadRec struct {
	Group    string     `json:"g"`
	ID       string     `json:"id"`
	Reason   ReasonCode `json:"r"`
	Detail   string     `json:"d"`
	Attempts int        `json:"a"`
	At       int64      `json:"at"`
}

func marshalEvent(e *event) ([]byte, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("localmq: marshal event: %w", err)
	}
	return b, nil
}

func unmarshalEvent(b []byte) (*event, error) {
	var e event
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, fmt.Errorf("localmq: unmarshal event: %w", err)
	}
	return &e, nil
}
