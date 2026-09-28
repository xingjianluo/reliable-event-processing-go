package mq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// assertSnapshotConsistentLocked 在同一临界区内同时计算快照并直接扫描原始
// 队列做交叉校验。这样“快照结论”与“真实已提交状态”的比对是原子的，
// 不依赖任何 goroutine 时序，在 -race 与高负载下也不会读到撕裂结论。
// 可从任意 goroutine 调用（只用 t.Errorf）。调用方必须持有 b.mu。
// now 由调用方在同一临界区内给定，保证“到期判定、快照、原始状态”三者
// 基于完全相同的时刻（time.Now 两次调用可能相差数百纳秒而跨过到期点）。
func assertSnapshotConsistentLockedAt(t *testing.T, b *Broker, group, key string, now time.Time) KeySnapshot {
	t.Helper()
	snap := b.keyStatusLocked(group, key, now)
	return checkSnapshotLocked(t, b, group, key, now, snap)
}

// assertSnapshotConsistentLocked 在持锁瞬间取时并校验，供不关心具体时刻的
// 调用方使用。
func assertSnapshotConsistentLocked(t *testing.T, b *Broker, group, key string) KeySnapshot {
	return assertSnapshotConsistentLockedAt(t, b, group, key, time.Now())
}

func checkSnapshotLocked(t *testing.T, b *Broker, group, key string, now time.Time, snap KeySnapshot) KeySnapshot {
	t.Helper()

	g := b.groups[group]
	if g == nil {
		if snap.Exists || snap.State != "" || snap.Paused {
			t.Errorf("nil group snapshot inconsistent: %+v", snap)
		}
		return snap
	}

	// 直接扫描原始队列，独立计算一份“真相”。
	var rawHead *msgState
	rawBacklog := 0
	rawExisting := false
	if msgs, ok := g.keyMsgs[key]; ok {
		rawExisting = true
		for _, ms := range msgs {
			if ms.status == stAcked || ms.status == stDead {
				continue
			}
			if rawHead == nil {
				rawHead = ms
			} else {
				rawBacklog++
			}
		}
	}
	paused := g.paused[key]

	if snap.Paused != paused {
		t.Errorf("Paused=%v want %v: %+v", snap.Paused, paused, snap)
	}
	if rawExisting && !snap.Exists {
		t.Errorf("raw has records but snap.Exists=false: %+v", snap)
	}

	if rawHead == nil {
		// 关键不变量：没有未完成消息时才允许 drained / paused(无队头)。
		// 这保证“读取绝不会把还没成功的消息当成已完成”。
		if snap.HeadSeq != 0 || snap.HeadID != "" || snap.Backlog != 0 {
			t.Errorf("no unfinished msgs but snap reports head: %+v", snap)
			return snap
		}
		switch snap.State {
		case StateDrained:
			if paused || snap.WaitReason != ReasonNone {
				t.Errorf("drained inconsistent: %+v", snap)
			}
		case StatePaused:
			if !paused || snap.WaitReason != ReasonPaused {
				t.Errorf("paused(no head) inconsistent: %+v", snap)
			}
		default:
			// 未知键且未暂停时 State 允许为空；其它情况非法。
			if rawExisting || paused {
				t.Errorf("no head but state=%s: %+v", snap.State, snap)
			}
		}
		return snap
	}

	// 有未完成队头：状态绝不允许是 drained。
	if snap.State == StateDrained {
		t.Errorf("raw head seq=%d status=%s but snap drained: %+v",
			rawHead.seq, rawHead.status, snap)
	}
	if snap.HeadSeq != rawHead.seq || snap.HeadID != rawHead.id {
		t.Errorf("head mismatch: snap=(%s,%d) raw=(%s,%d)",
			snap.HeadID, snap.HeadSeq, rawHead.id, rawHead.seq)
	}
	if snap.HeadAttempts != rawHead.attempts || snap.Backlog != rawBacklog {
		t.Errorf("attempts/backlog mismatch: snap=%+v raw attempts=%d backlog=%d",
			snap, rawHead.attempts, rawBacklog)
	}

	switch {
	case paused:
		if snap.State != StatePaused || snap.WaitReason != ReasonPaused {
			t.Errorf("paused head snap=%+v", snap)
		}
	case rawHead.status == stInflight:
		if snap.State != StateProcessing || snap.WaitReason != ReasonInFlight {
			t.Errorf("inflight head snap=%+v raw status=%s", snap, rawHead.status)
		}
	case rawHead.status == stPending && rawHead.notBefore.After(now):
		if snap.State != StateWaitingRetry || snap.WaitReason != ReasonRetryDelay {
			t.Errorf("waiting_retry head snap=%+v", snap)
		}
		if !snap.HeadNotBefore.Equal(rawHead.notBefore) {
			t.Errorf("HeadNotBefore=%v want %v", snap.HeadNotBefore, rawHead.notBefore)
		}
	case g.ownerOf(key) == nil:
		if snap.State != StateNoConsumer || snap.WaitReason != ReasonNoConsumer {
			t.Errorf("no_consumer head snap=%+v", snap)
		}
	default:
		if snap.State != StateReady || snap.WaitReason != ReasonNone {
			t.Errorf("ready head snap=%+v raw status=%s", snap, rawHead.status)
		}
	}
	return snap
}

// 在途消息期间，多个 goroutine 对同一键连续读取运行态：
// 每次快照都与原始已提交状态一致；读取绝不打断在途消费。
func TestKeyStatusConcurrentReadsWhileInflight(t *testing.T) {
	dir := t.TempDir()
	b, _ := OpenBroker(testConfig(t, dir))
	defer b.Close()

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var reads atomic.Int64
	eff := newEffects()
	c, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		if d.Seq() == 1 {
			entered <- struct{}{}
			<-release
		}
		eff.mark(d.ID())
		return AckDecision()
	})
	defer c.Close()

	for i := 0; i < 4; i++ {
		b.Publish(context.Background(), Message{Key: "k"}, PublishOptions{})
	}
	<-entered

	stop := make(chan struct{})     // 放行后停止公开 API 读线程的计数
	stopDeep := make(chan struct{}) // 全部 drained 后停止深度一致性校验
	var wg sync.WaitGroup
	// 两路读：一路只调公开 API（验证读取不阻塞/打断消费），
	// 一路在同一锁内与原始状态交叉校验（验证不会误报 drained）。
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(deep bool) {
			defer wg.Done()
			for {
				halt := stop
				if deep {
					halt = stopDeep
				}
				select {
				case <-halt:
					return
				default:
				}
				if deep {
					b.mu.Lock()
					assertSnapshotConsistentLocked(t, b, "g", "k")
					b.mu.Unlock()
				} else {
					// 放行（drained）后可能恰好在 stop 检查与本次读取之间发生，
					// 这是合法的；“持在途期间不得 drained”由深校验在同一锁内保证。
					_ = b.KeyStatus("g", "k")
				}
				reads.Add(1)
			}
		}(r%2 == 0)
	}

	// 在途期间持续读取一段时间，随后用同锁断言：此刻 seq1 仍在途且为 processing。
	time.Sleep(60 * time.Millisecond)
	b.mu.Lock()
	snap := assertSnapshotConsistentLocked(t, b, "g", "k")
	rawHead := b.groups["g"].keyMsgs["k"][0]
	held := rawHead.status == stInflight
	b.mu.Unlock()
	if !held {
		t.Fatalf("seq1 no longer inflight during hold window")
	}
	if snap.State != StateProcessing || snap.HeadSeq != 1 || snap.Backlog != 3 {
		t.Fatalf("during inflight snap=%+v", snap)
	}
	close(release) // 放行：读取没有打断消费，放行后应完整补齐。
	close(stop)

	waitFor(t, func() bool { return eff.total() == 4 }, 2*time.Second, "drained after release")
	drained := waitKeyState(t, b, "g", "k", StateDrained)
	close(stopDeep)
	wg.Wait()
	if reads.Load() == 0 {
		t.Fatal("no concurrent reads happened")
	}
	if drained.State != StateDrained {
		t.Fatalf("final=%+v", drained)
	}
}

// 端到端排空期间持续读取：业务全部生效后，在 ACK 落盘窗口内快照可以是
// processing/ready，但绝不能出现“原始状态还有未完成消息、快照却 drained”。
// 全程用同锁交叉校验，并要求最终 drained 且每条恰好生效一次、键内有序。
func TestKeyStatusConcurrentReadsThroughoutDrain(t *testing.T) {
	dir := t.TempDir()
	b, _ := OpenBroker(testConfig(t, dir))
	defer b.Close()

	eff := newEffects()
	var mu sync.Mutex
	order := map[string][]int64{}
	c, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		eff.mark(d.ID())
		mu.Lock()
		order[d.RoutingKey()] = append(order[d.RoutingKey()], d.Seq())
		mu.Unlock()
		return AckDecision()
	})
	defer c.Close()

	const nKeys = 8
	const perKey = 15
	keys := make([]string, nKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%02d", i)
		for j := 0; j < perKey; j++ {
			b.Publish(context.Background(), Message{Key: keys[i]}, PublishOptions{})
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var bad atomic.Int64
	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func(deep bool) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, k := range keys {
					if deep {
						b.mu.Lock()
						assertSnapshotConsistentLocked(t, b, "g", k)
						b.mu.Unlock()
					} else {
						s := b.KeyStatus("g", k)
						if s.State != StateDrained {
							continue
						}
						b.mu.Lock()
						for _, ms := range b.groups["g"].keyMsgs[k] {
							if ms.status != stAcked && ms.status != stDead {
								b.mu.Unlock()
								bad.Add(1)
								t.Errorf("false drained: key=%s raw head seq=%d status=%s snap=%+v",
									k, ms.seq, ms.status, s)
								break
							}
						}
						b.mu.Unlock()
					}
				}
			}
		}(r%2 == 0)
	}

	waitFor(t, func() bool { return eff.total() == nKeys*perKey }, 5*time.Second, "all effects")
	for _, k := range keys {
		waitKeyState(t, b, "g", k, StateDrained)
	}
	close(stop)
	wg.Wait()
	if bad.Load() != 0 {
		t.Fatalf("observed %d false-drained snapshots", bad.Load())
	}

	mu.Lock()
	defer mu.Unlock()
	for _, k := range keys {
		got := order[k]
		if len(got) != perKey {
			t.Fatalf("%s processed %d want %d", k, len(got), perKey)
		}
		for i, s := range got {
			if s != int64(i+1) {
				t.Fatalf("%s out of order at %d: %d", k, i, s)
			}
		}
	}
}

// 暂停/恢复与消费组扩缩容叠加，同时持续读取：任意时刻快照都必须自洽，
// 暂停期间不投递，恢复后按序补齐，最终 drained、恰好一次。
// 每轮都在暂停状态下发布该轮批次，保证“暂停期间零投递”的断言确定性。
func TestKeyStatusReadsDuringPauseAndScale(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.ShutdownTimeout = 300 * time.Millisecond
	b, _ := OpenBroker(cfg)
	defer b.Close()

	eff := newEffects()
	var mu sync.Mutex
	order := map[string][]int64{}
	mk := func() Handler {
		return func(ctx context.Context, d *Delivery) Decision {
			eff.mark(d.ID())
			mu.Lock()
			order[d.RoutingKey()] = append(order[d.RoutingKey()], d.Seq())
			mu.Unlock()
			return AckDecision()
		}
	}
	c1, _ := b.Subscribe("g", "c1", mk())
	defer c1.Close()

	const rounds = 4
	const batch = 6
	keys := []string{"ka", "kb", "kc", "kd"}

	// 先暂停，再发布：初始批次一条都不会投递。
	for _, k := range keys {
		if err := b.Pause("g", k); err != nil {
			t.Fatal(err)
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, k := range keys {
					b.mu.Lock()
					assertSnapshotConsistentLocked(t, b, "g", k)
					b.mu.Unlock()
				}
			}
		}()
	}

	var extra []*Consumer
	wantTotal := 0
	for round := 0; round < rounds; round++ {
		// 该轮批次在暂停状态下发布。
		for _, k := range keys {
			for j := 0; j < batch; j++ {
				if _, err := b.Publish(context.Background(), Message{Key: k}, PublishOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			if s := b.KeyStatus("g", k); !s.Paused || s.State != StatePaused {
				t.Fatalf("round %d key=%s not paused: %+v", round, k, s)
			}
		}

		// 奇数轮扩容、偶数轮缩容（c1 始终保留，故总有归属）。
		if round%2 == 1 {
			c, _ := b.Subscribe("g", fmt.Sprintf("c%d", round+2), mk())
			extra = append(extra, c)
		} else if len(extra) > 0 {
			c := extra[len(extra)-1]
			extra = extra[:len(extra)-1]
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
		}

		// 暂停期间：该轮新批次零投递（上一轮已 drained，无残留途消息）。
		before := eff.total()
		assertNotDeliveredFor(t, eff.total, before, 50*time.Millisecond,
			fmt.Sprintf("round %d delivery while paused", round))

		for _, k := range keys {
			if err := b.Resume("g", k); err != nil {
				t.Fatal(err)
			}
		}
		wantTotal += len(keys) * batch
		waitFor(t, func() bool { return eff.total() == wantTotal },
			5*time.Second, fmt.Sprintf("round %d caught up", round))
		for _, k := range keys {
			waitKeyState(t, b, "g", k, StateDrained)
		}

		// 为下一轮重新暂停（drained 后暂停，下一批发布时仍处于暂停）。
		if round < rounds-1 {
			for _, k := range keys {
				if err := b.Pause("g", k); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	for _, c := range extra {
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()

	if eff.total() != wantTotal {
		t.Fatalf("effects=%d want %d", eff.total(), wantTotal)
	}
	mu.Lock()
	defer mu.Unlock()
	for id, n := range eff.success {
		if n != 1 {
			t.Fatalf("message %s applied %d times", id, n)
		}
	}
	wantPerKey := rounds * batch
	for _, k := range keys {
		got := order[k]
		if len(got) != wantPerKey {
			t.Fatalf("%s processed %d want %d", k, len(got), wantPerKey)
		}
		for i, s := range got {
			if s != int64(i+1) {
				t.Fatalf("%s out of order at %d: %d", k, i, s)
			}
		}
	}
}

// 队头处于延期重试时持续读取：等待重试期间快照恒不为 ready，
// 重试到期后转为 processing/drained，全程自洽，且读取不影响重试调度。
func TestKeyStatusConcurrentReadsDuringDelayedHead(t *testing.T) {
	dir := t.TempDir()
	b, _ := OpenBroker(testConfig(t, dir))
	defer b.Close()

	var mu sync.Mutex
	var n1 int
	eff := newEffects()
	c, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		if d.Seq() == 1 {
			mu.Lock()
			n1++
			first := n1 == 1
			mu.Unlock()
			if first {
				return RetryDecision(errors.New("transient"), 60*time.Millisecond)
			}
		}
		eff.mark(d.ID())
		return AckDecision()
	})
	defer c.Close()

	for i := 0; i < 3; i++ {
		b.Publish(context.Background(), Message{Key: "k"}, PublishOptions{})
	}
	waitKeyState(t, b, "g", "k", StateWaitingRetry)
	// 记录到期时刻：到期前快照必须恒为 waiting_retry，绝不允许 ready。
	due := b.KeyStatus("g", "k").HeadNotBefore
	if !due.After(time.Now()) {
		t.Fatalf("retry notBefore=%s already due at test start", due)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// now 与快照必须是同一时刻：在同一临界区内取值并与原始
				// 状态交叉校验，避免持锁等待跨过到期点造成假失败。
				b.mu.Lock()
				now := time.Now()
				s := assertSnapshotConsistentLockedAt(t, b, "g", "k", now)
				early := now.Before(due)
				b.mu.Unlock()
				if early && s.State != StateWaitingRetry {
					t.Errorf("before due=%s state=%s snap=%+v", due, s.State, s)
					return
				}
			}
		}()
	}

	// 到期前再确认一次 waiting_retry（留出调度余量，不依赖固定长 sleep）。
	waitFor(t, func() bool {
		s := b.KeyStatus("g", "k")
		return s.State == StateWaitingRetry && time.Now().Add(20*time.Millisecond).Before(due)
	}, time.Second, "still well before retry due")
	if s := b.KeyStatus("g", "k"); s.State != StateWaitingRetry {
		t.Fatalf("before due snap=%+v want waiting_retry", s)
	}

	// 等待重试到期并成功补齐：读取不应拖慢或取消重试计时器。
	waitFor(t, func() bool { return eff.total() == 3 }, 3*time.Second, "drained after retry")
	waitKeyState(t, b, "g", "k", StateDrained)
	close(stop)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if n1 != 2 {
		t.Fatalf("seq1 attempts=%d want 2", n1)
	}
}
