package localmq

import (
	"context"
	"errors"
	"time"
)

// ReasonCode identifies why a message ended up in the dead-letter queue.
type ReasonCode string

const (
	// ReasonMaxAttempts: all attempts failed with temporary errors.
	ReasonMaxAttempts ReasonCode = "max_attempts"
	// ReasonRejected: the handler explicitly rejected the message as
	// non-recoverable.
	ReasonRejected ReasonCode = "rejected"
	// ReasonPanic: the handler panicked and the message could not be
	// acknowledged; such messages are dead-lettered immediately.
	ReasonPanic ReasonCode = "panic"
)

// Message is a routed, persistently stored unit of delivery.
type Message struct {
	// ID is globally unique and stable across all delivery attempts.
	ID string
	// RouteKey determines ordering and parallelism boundaries.
	RouteKey string
	// Body is the opaque application payload.
	Body []byte
	// CreatedAt is a Unix-nano timestamp assigned by the broker.
	CreatedAt int64
}

// Delivery is a single attempt to process a message. Ack/Nack/Reject are
// idempotent: only the first terminal call takes effect, later calls return
// ErrAlreadySettled.
type Delivery struct {
	Message *Message
	// Attempt is 1-based.
	Attempt  int
	Group    string
	Consumer string

	settle func(settleAction) error
}

// ErrAlreadySettled is returned when a delivery is acknowledged, rejected or
// negatively acknowledged more than once.
var ErrAlreadySettled = errors.New("localmq: delivery already settled")

// ErrBrokerClosed is returned when an operation is attempted after Close.
var ErrBrokerClosed = errors.New("localmq: broker closed")

type settleActionKind int

const (
	settleAck settleActionKind = iota
	settleNack
	settleReject
	settlePanic
)

type settleAction struct {
	kind       settleActionKind
	retryAfter time.Duration
	reason     string
}

// Ack marks the message successfully processed. The success is durable before
// the call returns; an acknowledged message is never delivered again.
func (d *Delivery) Ack() error {
	if d.settle == nil {
		return ErrBrokerClosed
	}
	return d.settle(settleAction{kind: settleAck})
}

// Nack signals a temporary failure. retryAfter, when > 0, overrides the
// configured exponential backoff for this attempt.
func (d *Delivery) Nack(retryAfter time.Duration) error {
	if d.settle == nil {
		return ErrBrokerClosed
	}
	return d.settle(settleAction{kind: settleNack, retryAfter: retryAfter})
}

// Reject signals a permanent, non-recoverable failure. The message is moved to
// the dead-letter queue immediately with the given reason.
func (d *Delivery) Reject(reason string) error {
	if d.settle == nil {
		return ErrBrokerClosed
	}
	return d.settle(settleAction{kind: settleReject, reason: reason})
}

// HandlerFunc processes one delivery. Returning nil is equivalent to Ack;
// returning a non-nil error is equivalent to Nack with backoff. Explicit
// Ack/Nack/Reject on the delivery always wins if it happens first.
type HandlerFunc func(ctx context.Context, d *Delivery) error

// DeadLetter is a message that will not be retried anymore within a group.
type DeadLetter struct {
	Message  *Message
	Group    string
	Reason   ReasonCode
	Detail   string
	Attempts int
	DeadAt   int64
}
