package mq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// snapshotReader 在停止前持续读取 (group,key) 运行态，对每一帧做严格不变量
// 校验；同时统计出现过的状态，供测试事后断言“处理中/清空”等状态确实被观察到。
// 读取只走公开 API KeyStatus，绝不调用任何会改变投递行为的方法。
type snapshotReader struct {
	b     *Broker
	group string
	key   string

	stopCh chan struct{}
	wg     sync.WaitGroup

	// terminal 是读取者自行结束的收尾状态集合：队列已无未完成消息
	//（drained），或无未完成消息但暂停位仍在（paused 且无头）。
	terminal map[KeyState]bool

	mu     sync.Mutex
	states map[KeyState]int
	errs   []string
}

func newSnapshotReader(b *Broker, group, key string) *snapshotReader {
	return &snapshotReader{
		b:        b,
		group:    group,
		key:      key,
		stopCh:   make(chan struct{}),
		terminal: map[KeyState]bool{StateDrained: true},
		states:   map[KeyState]int{},
	}
}

// withPauseTerminal 额外把“暂停且无未完成队头”视为合法收尾帧
// （暂停/恢复抖动结束时键可能停在暂停态但消息已全部终态）。
func (r *snapshotReader) withPauseTerminal() *snapshotReader {
	r.terminal[StatePaused] = true
	return r
}

// start 启动 n 个并发读取 goroutine。每个 goroutine 对读到的每一帧都做
// 快照不变量校验；读到收尾终态帧（drained，或配置了 paused 时的
// paused-but-empty）后自动停止（终态不回退）。
func (r *snapshotReader) start(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			for {
				select {
				case <-r.stopCh:
					return
				default:
				}
				s := r.b.KeyStatus(r.group, r.key)
				if s.Exists {
					if err := invariantError(s); err != nil {
						r.mu.Lock()
						r.errs = append(r.errs, err.Error())
						r.mu.Unlock()
					}
					r.mu.Lock()
					r.states[s.State]++
					isTerminal := r.terminal[s.State] &&
						!(s.State == StatePaused && s.HeadSeq != 0) // paused 且仍有队头不算收尾
					r.mu.Unlock()
					if isTerminal {
						return
					}
				}
			}
		}()
	}
}

// stop 等待所有读取 goroutine 自然结束（它们在观察到收尾状态帧后
// 自行退出：drained，或 paused-but-empty）；wait 为有界等待，
// 超时才发停止信号并判失败
// （例如消费被读取打断、ACK 一直不落盘、状态到不了终态）。
func (r *snapshotReader) stop(t *testing.T, wait time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() { r.wg.Wait(); close(done) }()
	select {
	case <-done:
		// 正常：每个读取者都已亲眼读到收尾终态帧。
	case <-time.After(wait):
		close(r.stopCh)
		r.wg.Wait()
		t.Fatalf("snapshot readers did not reach terminal state within %s (consumption stuck?)", wait)
	}
	close(r.stopCh) // 正常路径下收尾（超时路径已 t.Fatalf，不会执行到这里）
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.errs {
		t.Errorf("snapshot invariant violated: %s", e)
	}
}

func (r *snapshotReader) count(state KeyState) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.states[state]
}

// waitObserved 有界等待读取者至少采到一帧指定状态（用于在改变状态前
// 确认前一状态已被并发读取真实观察到，避免依赖 goroutine 调度时序）。
func (r *snapshotReader) waitObserved(t *testing.T, state KeyState, wait time.Duration) {
	t.Helper()
	waitFor(t, func() bool { return r.count(state) > 0 }, wait, "observe state="+string(state))
}

func (r *snapshotReader) snapshotStates() map[KeyState]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make(map[KeyState]int, len(r.states))
	for k, v := range r.states {
		cp[k] = v
	}
	return cp
}

// TestKeyStatusConcurrentReadsInFlight 对同一个键连续、并发读取运行态：
// 读取期间正好有消息在途，快照必须帧帧自洽、绝不能把在途消息误报成已清空，
// 且读取本身不能打断消费（最终必须全部处理完、状态进入 drained）。
func TestKeyStatusConcurrentReadsInFlight(t *testing.T) {
	dir := t.TempDir()
	b, _ := OpenBroker(testConfig(t, dir))
	defer b.Close()

	const n = 6
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	eff := newEffects()
	c, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		if d.Seq() == 1 {
			entered <- struct{}{}
			<-release // 人为拉长在途窗口，让并发读取稳定观察到 processing
		}
		eff.mark(d.ID())
		return AckDecision()
	})
	defer c.Close()

	for i := 0; i < n; i++ {
		b.Publish(context.Background(), Message{Key: "k"}, PublishOptions{})
	}
	<-entered

	// 第一条明确在途时启动多个读取者，持续读取直到观察到 drained 终态帧。
	// 这覆盖了整个“在途 -> 业务生效 -> ACK 落盘 -> drained”过程。
	reader := newSnapshotReader(b, "g", "k")
	reader.start(t, 3)
	reader.waitObserved(t, StateProcessing, 2*time.Second) // 确定性确认在途帧已被读到
	close(release)

	// 读取者自行等到 drained；消费被打断或 ACK 不落盘都会在这里超时失败。
	reader.stop(t, 3*time.Second)

	if got := reader.count(StateProcessing); got == 0 {
		t.Fatalf("expected to observe processing during reads, states=%v", reader.snapshotStates())
	}
	if got := reader.count(StateDrained); got == 0 {
		t.Fatalf("expected to observe drained after completion, states=%v", reader.snapshotStates())
	}
	if eff.total() != n {
		t.Fatalf("reads disrupted consumption: effects=%d want %d", eff.total(), n)
	}
}

// TestKeyStatusReadsAcrossPauseAndRetry 队头处于延期重试、随后暂停、再恢复
// 的全过程中并发读取运行态：每一帧都必须自洽；必须依次观察到
// waiting_retry（到期重试被暂停压住）-> paused -> processing -> drained；
// 暂停期间该键一次都不得投递（业务计数保持 0）。
func TestKeyStatusReadsAcrossPauseAndRetry(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.RetryDelay = 500 * time.Millisecond
	cfg.MaxRetryDelay = 2 * time.Second
	b, _ := OpenBroker(cfg)
	defer b.Close()

	const n = 4
	const retry = 400 * time.Millisecond
	var mu sync.Mutex
	var seq1Deliveries int
	var ackedSeqs []int64
	eff := newEffects()
	attempt2Entered := make(chan struct{}, 1)
	releaseAttempt2 := make(chan struct{})
	c, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		if d.Seq() == 1 {
			mu.Lock()
			seq1Deliveries++
			k := seq1Deliveries
			mu.Unlock()
			if k == 1 {
				return RetryDecision(errors.New("transient"), retry)
			}
			// 恢复后的队头重投：人为保持在 processing，供并发读取稳定观察。
			attempt2Entered <- struct{}{}
			<-releaseAttempt2
		}
		eff.mark(d.ID())
		mu.Lock()
		ackedSeqs = append(ackedSeqs, d.Seq())
		mu.Unlock()
		return AckDecision()
	})
	defer c.Close()

	for i := 0; i < n; i++ {
		b.Publish(context.Background(), Message{Key: "k"}, PublishOptions{})
	}
	waitKeyState(t, b, "g", "k", StateWaitingRetry, 2*time.Second)

	reader := newSnapshotReader(b, "g", "k")
	reader.start(t, 3)
	reader.waitObserved(t, StateWaitingRetry, time.Second)

	if err := b.Pause("g", "k"); err != nil {
		t.Fatal(err)
	}
	// 暂停持续时间已超过 400ms 重试到期点：到期重试仍不得放行。
	time.Sleep(retry + 100*time.Millisecond)
	mu.Lock()
	deliv := seq1Deliveries
	mu.Unlock()
	if deliv != 1 {
		t.Fatalf("paused key redelivered after retry due: deliveries=%d", deliv)
	}
	if eff.total() != 0 {
		t.Fatalf("paused key made progress: effects=%d", eff.total())
	}
	if got := reader.count(StatePaused); got == 0 {
		t.Fatalf("expected to observe paused state, states=%v", reader.snapshotStates())
	}
	if got := reader.count(StateWaitingRetry); got == 0 {
		t.Fatalf("expected to observe waiting_retry before pause, states=%v", reader.snapshotStates())
	}

	if err := b.Resume("g", "k"); err != nil {
		t.Fatal(err)
	}
	// 恢复后队头重投必然先进入 processing（在途），门控保证该窗口
	// 不被调度饿死，可被并发读取稳定观察；之后放行让它及后续消息完成。
	<-attempt2Entered
	reader.waitObserved(t, StateProcessing, 2*time.Second)
	close(releaseAttempt2)
	reader.stop(t, 3*time.Second) // 等所有读取者亲眼读到 drained 终态帧

	if got := reader.count(StateProcessing); got == 0 {
		t.Fatalf("expected to observe processing after resume, states=%v", reader.snapshotStates())
	}
	if eff.total() != n {
		t.Fatalf("effects=%d want %d", eff.total(), n)
	}
	mu.Lock()
	defer mu.Unlock()
	for i, s := range ackedSeqs {
		if s != int64(i+1) {
			t.Fatalf("out of order at %d: seq=%d", i, s)
		}
	}
}

// TestKeyStatusReadsAcrossPauseAndScale 暂停与消费组扩缩容叠加、且缩容时
// 在途消息被收回的全过程中并发读取运行态：任何一帧都必须自洽；
// 收回后键处于 paused + 有队头（不得误报 drained）；扩容新消费者也不得
// 在暂停期间投递；恢复后从最早未完成消息按序补齐。
func TestKeyStatusReadsAcrossPauseAndScale(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.ShutdownTimeout = 120 * time.Millisecond
	b, _ := OpenBroker(cfg)
	defer b.Close()

	const n = 3
	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	eff1 := newEffects()
	c1, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		if d.Seq() == 1 {
			entered <- struct{}{}
			select {
			case <-block:
			case <-ctx.Done(): // 缩容超时收回时取消
				return RetryDecision(errors.New("revoked"), 0)
			}
		}
		eff1.mark(d.ID())
		return AckDecision()
	})
	defer c1.Close()

	for i := 0; i < n; i++ {
		b.Publish(context.Background(), Message{Key: "k"}, PublishOptions{})
	}
	<-entered

	if err := b.Pause("g", "k"); err != nil {
		t.Fatal(err)
	}
	reader := newSnapshotReader(b, "g", "k")
	reader.start(t, 3)
	reader.waitObserved(t, StatePaused, time.Second)

	// 缩容：在途消息超时收回为 pending。
	if err := c1.Close(); err != nil {
		t.Fatal(err)
	}
	close(block)

	// 扩容新消费者：键仍暂停，暂停期间不得投递。
	eff2 := newEffects()
	c2, _ := b.Subscribe("g", "c2", func(ctx context.Context, d *Delivery) Decision {
		eff2.mark(d.ID())
		return AckDecision()
	})
	defer c2.Close()
	time.Sleep(120 * time.Millisecond)
	if got := eff2.total(); got != 0 {
		t.Fatalf("delivered while paused after rebalance: %d", got)
	}
	// 收回后快照必须仍报告 paused 且队头未完成（绝不能是 drained/processing）。
	snap := b.KeyStatus("g", "k")
	if snap.State != StatePaused || snap.WaitReason != ReasonPaused || snap.HeadSeq != 1 {
		t.Fatalf("after revoke while paused snapshot = %+v, want paused head=seq1", snap)
	}
	if got := reader.count(StateDrained); got != 0 {
		t.Fatalf("must never report drained while paused with backlog, states=%v", reader.snapshotStates())
	}

	if err := b.Resume("g", "k"); err != nil {
		t.Fatal(err)
	}
	reader.stop(t, 3*time.Second)
	if got := eff2.total(); got != n {
		t.Fatalf("after resume effects=%d want %d", got, n)
	}
}

// TestKeyStatusReadsDuringPauseFlapping 暂停/恢复高速抖动期间，多个 goroutine
// 对多个键连续读取运行态：任何一帧都必须自洽；抖动结束并恢复后，所有消息
// 按键内顺序、恰好一次处理完，最终状态 drained。
func TestKeyStatusReadsDuringPauseFlapping(t *testing.T) {
	dir := t.TempDir()
	b, _ := OpenBroker(testConfig(t, dir))
	defer b.Close()

	keys := []string{"ka", "kb", "kc"}
	const perKey = 8
	var mu sync.Mutex
	order := map[string][]int64{}
	eff := newEffects()
	c, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		eff.mark(d.ID())
		mu.Lock()
		order[d.RoutingKey()] = append(order[d.RoutingKey()], d.Seq())
		mu.Unlock()
		return AckDecision()
	})
	defer c.Close()

	var readers []*snapshotReader
	for _, k := range keys {
		for i := 0; i < perKey; i++ {
			b.Publish(context.Background(), Message{Key: k}, PublishOptions{})
		}
		rd := newSnapshotReader(b, "g", k).withPauseTerminal()
		rd.start(t, 2)
		readers = append(readers, rd)
	}

	// 高速暂停/恢复抖动。
	stopFlap := make(chan struct{})
	var flapWG sync.WaitGroup
	for _, k := range keys {
		flapWG.Add(1)
		k := k
		go func() {
			defer flapWG.Done()
			paused := false
			for {
				select {
				case <-stopFlap:
					if paused {
						b.Resume("g", k)
					}
					return
				default:
				}
				if paused {
					b.Resume("g", k)
					paused = false
				} else {
					b.Pause("g", k)
					paused = true
				}
				time.Sleep(time.Duration(1+(int(time.Now().UnixNano())%3)) * time.Millisecond)
			}
		}()
	}

	total := len(keys) * perKey
	waitFor(t, func() bool { return eff.total() == total }, 5*time.Second, "all processed despite flapping")
	close(stopFlap)
	flapWG.Wait()
	for _, k := range keys {
		if err := b.Resume("g", k); err != nil { // 幂等兜底
			t.Fatal(err)
		}
	}
	// 业务计数到齐后仍可能处在“生效 -> ACK 落盘”窗口：
	// 让每个键的读取者亲眼读到收尾终态帧再结束。
	for _, rd := range readers {
		rd.stop(t, 3*time.Second)
	}
	// 抖动 goroutine 退出时已保证最终处于恢复态；确认最终快照为 drained。
	for _, k := range keys {
		waitKeyState(t, b, "g", k, StateDrained, 2*time.Second)
	}

	if eff.total() != total {
		t.Fatalf("effects=%d want %d", eff.total(), total)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, k := range keys {
		got := order[k]
		if len(got) != perKey {
			t.Fatalf("key %s processed %d want %d", k, len(got), perKey)
		}
		for i, s := range got {
			if s != int64(i+1) {
				t.Fatalf("key %s out of order at %d: seq=%d", k, i, s)
			}
		}
	}
}

// TestKeyStatusReadsDuringRetryWindow 队头处于较长延期重试等待期间连续读取：
// 每一帧的 waiting_retry 都必须给出未来的 HeadNotBefore，且
// HeadSeq=1/HeadAttempts=1/Backlog 恒定，后续消息不得越过队头；
// 随后暂停并越过到期时刻，快照稳定为 paused 且队头仍未完成、不投递；
// 恢复后从队头按序补齐。
func TestKeyStatusReadsDuringRetryWindow(t *testing.T) {
	dir := t.TempDir()
	// 自定义较宽的重试上限，避免被默认 MaxRetryDelay=50ms 截断，
	// 并给调度抖动留出充足余量。
	cfg := testConfig(t, dir)
	cfg.RetryDelay = 500 * time.Millisecond
	cfg.MaxRetryDelay = 2 * time.Second
	b, _ := OpenBroker(cfg)
	defer b.Close()

	const n = 3
	const retry = 300 * time.Millisecond
	var mu sync.Mutex
	var seq1Deliveries int
	var acked []int64
	eff := newEffects()
	c, _ := b.Subscribe("g", "c1", func(ctx context.Context, d *Delivery) Decision {
		if d.Seq() == 1 {
			mu.Lock()
			seq1Deliveries++
			k := seq1Deliveries
			mu.Unlock()
			if k == 1 {
				return RetryDecision(errors.New("boom"), retry)
			}
		}
		eff.mark(d.ID())
		mu.Lock()
		acked = append(acked, d.Seq())
		mu.Unlock()
		return AckDecision()
	})
	defer c.Close()

	for i := 0; i < n; i++ {
		b.Publish(context.Background(), Message{Key: "k"}, PublishOptions{})
	}
	waitKeyState(t, b, "g", "k", StateWaitingRetry, 2*time.Second)

	// 重试窗口内连续读取：字段必须稳定且自洽（在到期前结束观察）。
	// 只在窗口前半段采样，即便机器有负载也不会越过到期边界。
	deadline := time.Now().Add(retry / 3)
	for time.Now().Before(deadline) {
		s := b.KeyStatus("g", "k")
		if s.State != StateWaitingRetry || s.WaitReason != ReasonRetryDelay {
			t.Fatalf("retry-window snapshot = %+v, want waiting_retry", s)
		}
		if s.HeadSeq != 1 || s.HeadAttempts != 1 || s.Backlog != n-1 {
			t.Fatalf("retry-window head/backlog drifted: %+v", s)
		}
		if !s.HeadNotBefore.After(time.Now()) {
			t.Fatalf("HeadNotBefore not in future during retry window: %+v", s)
		}
		if eff.total() != 0 {
			t.Fatalf("messages crossed delayed head: effects=%d", eff.total())
		}
	}

	if err := b.Pause("g", "k"); err != nil {
		t.Fatal(err)
	}
	// 越过重试到期时刻后仍暂停：不得放行。
	time.Sleep(retry + 100*time.Millisecond)
	mu.Lock()
	deliv := seq1Deliveries
	mu.Unlock()
	if deliv != 1 || eff.total() != 0 {
		t.Fatalf("paused key progressed past retry expiry: deliveries=%d effects=%d", deliv, eff.total())
	}
	s := b.KeyStatus("g", "k")
	if s.State != StatePaused || s.WaitReason != ReasonPaused || s.HeadSeq != 1 || s.Backlog != n-1 {
		t.Fatalf("paused snapshot after retry expiry = %+v", s)
	}

	if err := b.Resume("g", "k"); err != nil {
		t.Fatal(err)
	}
	waitKeyState(t, b, "g", "k", StateDrained, 2*time.Second)
	if eff.total() != n {
		t.Fatalf("effects=%d want %d", eff.total(), n)
	}
	mu.Lock()
	defer mu.Unlock()
	for i, seq := range acked {
		if seq != int64(i+1) {
			t.Fatalf("out of order at %d: seq=%d", i, seq)
		}
	}
}
