package mq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 临时失败 -> 重试 -> 最终成功：业务只生效一次，尝试次数递增。
func TestRetryThenSuccess(t *testing.T) {
	dir := t.TempDir()
	b, _ := OpenBroker(testConfig(t, dir))
	defer b.Close()

	eff := newEffects()
	var (
		attMu        sync.Mutex
		attemptsSeen []int
	)
	c, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		if d.Attempt() < 3 {
			return RetryDecision(errors.New("transient"), 0)
		}
		eff.mark(d.ID())
		attMu.Lock()
		attemptsSeen = append(attemptsSeen, d.Attempt())
		attMu.Unlock()
		return AckDecision()
	})
	defer c.Close()

	res, _ := b.Publish(context.Background(), Message{Key: "k", Payload: []byte("x")}, PublishOptions{})
	waitFor(t, func() bool { return eff.count(res.ID) == 1 }, 3*time.Second, "retry then success")
	attMu.Lock()
	if len(attemptsSeen) != 1 || attemptsSeen[0] != 3 {
		attMu.Unlock()
		t.Fatalf("attempts at success = %v, want [3]", attemptsSeen)
	}
	attMu.Unlock()
	b.mu.Lock()
	st := b.groups["g"].byID[res.ID].status
	at := b.groups["g"].byID[res.ID].attempts
	b.mu.Unlock()
	if st != stAcked {
		t.Fatalf("status=%s want acked", st)
	}
	if at != 3 {
		t.Fatalf("attempts=%d want 3", at)
	}
}

// 重试耗尽 -> 死信，原因可区分，且不再投递。
func TestRetryExhaustedDeadLetter(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.MaxAttempts = 3
	b, _ := OpenBroker(cfg)
	defer b.Close()

	var deliveries atomic.Int32
	c, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		deliveries.Add(1)
		return RetryDecision(errors.New("boom"), time.Millisecond)
	})
	defer c.Close()

	res, _ := b.Publish(context.Background(), Message{Key: "k"}, PublishOptions{})
	waitFor(t, func() bool {
		dl := b.DeadLetters("g")
		return len(dl) == 1 && dl[0].ID == res.ID
	}, 3*time.Second, "dead letter present")

	dl := b.DeadLetters("g")[0]
	if dl.Reason != ReasonRetryExhausted {
		t.Fatalf("reason=%s want %s", dl.Reason, ReasonRetryExhausted)
	}
	if dl.Attempts != 3 {
		t.Fatalf("dead attempts=%d want 3", dl.Attempts)
	}
	if dl.LastError != "boom" {
		t.Fatalf("err=%q want boom", dl.LastError)
	}
	time.Sleep(50 * time.Millisecond)
	if n := deliveries.Load(); n != 3 {
		t.Fatalf("deliveries after dead=%d want exactly 3", n)
	}
	b.mu.Lock()
	st := b.groups["g"].byID[res.ID].status
	b.mu.Unlock()
	if st != stDead {
		t.Fatalf("status=%s want dead", st)
	}
}

// 不可恢复失败 -> 立即死信，原因区分，且不占用重试次数。
func TestUnrecoverableDeadLetter(t *testing.T) {
	dir := t.TempDir()
	b, _ := OpenBroker(testConfig(t, dir))
	defer b.Close()

	var deliveries atomic.Int32
	c, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		deliveries.Add(1)
		return DropDecision(errors.New("bad payload"))
	})
	defer c.Close()

	res, _ := b.Publish(context.Background(), Message{Key: "k"}, PublishOptions{})
	waitFor(t, func() bool { return len(b.DeadLetters("g")) == 1 }, time.Second, "dead")
	dl := b.DeadLetters("g")[0]
	if dl.ID != res.ID || dl.Reason != ReasonUnrecoverable {
		t.Fatalf("unexpected dead letter %+v", dl)
	}
	time.Sleep(30 * time.Millisecond)
	if deliveries.Load() != 1 {
		t.Fatalf("deliveries=%d want 1 (immediate dead)", deliveries.Load())
	}
}
