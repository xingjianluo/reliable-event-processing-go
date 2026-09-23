package mq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/xingjianluo/reliable-event-processing-go/mq/wal"
)

// 状态常量。
const (
	stPending  = "pending"  // 待投递（或等待重试计时）
	stInflight = "inflight" // 已投递给处理器、等待裁决
	stAcked    = "acked"    // 已确认（终态，不再投递）
	stDead     = "dead"     // 死信（终态，隔离）
)

// Broker 是本地可靠消息代理：负责持久化、按路由键排序、
// 消费组分发、重试调度与死信管理。
type Broker struct {
	cfg    Config
	w      *wal.WAL
	log    Logger
	baseCtx context.Context
	cancel context.CancelFunc

	mu sync.Mutex
	// 消息主体（负载只存一份）。
	messages map[string]*storedMessage
	keySeq   map[string]int64
	// 发布幂等：dedupKey -> messageID。
	dedup map[string]string
	// 消费组。
	groups map[string]*groupState

	closed        bool
	closeDeadline time.Time
	wg          sync.WaitGroup // 所有 runner goroutine
}

type storedMessage struct {
	id      string
	key     string
	seq     int64
	payload []byte
}

// msgState 是某消息在某消费组内的投递状态。
type msgState struct {
	id        string
	key       string
	seq       int64
	status    string
	attempts  int
	notBefore time.Time // 重试到期时间
	deadAt    time.Time // 进入死信的时间
	reason    FailureReason
	errMsg    string
}

// gk 标识一个（消费组, 路由键）。
type gk struct{ group, key string }

type consumerHandle struct {
	id      string
	handler Handler
	sem     chan struct{} // 限制该消费者并发处理的不同路由键数
	closing bool
}

type groupState struct {
	name       string
	registered bool
	consumers  map[string]*consumerHandle
	// 该组内每个路由键的消息（严格按 seq 升序，含终态）。
	keyMsgs map[string][]*msgState
	byID    map[string]*msgState
	runners map[string]*runner // key -> runner
	// 死信（保持追加顺序）。
	dead []*msgState
}

// OpenBroker 打开（不存在则初始化）本地消息代理并重放持久化日志。
func OpenBroker(cfg Config) (*Broker, error) {
	cfg = withDefaults(cfg)
	w, err := wal.Open(filepath.Join(cfg.Dir, "events.wal"))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{
		cfg:      cfg,
		w:        w,
		log:      cfg.Logger,
		baseCtx:  ctx,
		cancel:   cancel,
		messages: map[string]*storedMessage{},
		keySeq:   map[string]int64{},
		dedup:    map[string]string{},
		groups:   map[string]*groupState{},
	}
	if err := b.replay(); err != nil {
		w.Close()
		cancel()
		return nil, err
	}
	b.log.Printf("broker opened: dir=%s maxAttempts=%d retryDelay=%s groups=%d",
		cfg.Dir, cfg.MaxAttempts, cfg.RetryDelay, len(b.groups))
	return b, nil
}

func withDefaults(cfg Config) Config {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = 100 * time.Millisecond
	}
	if cfg.MaxRetryDelay <= 0 {
		cfg.MaxRetryDelay = 30 * time.Second
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = 2
	}
	if cfg.HandlerConcurrency <= 0 {
		cfg.HandlerConcurrency = 8
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 10 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = newDefaultLogger()
	}
	if cfg.Dir == "" {
		cfg.Dir = "./mq-data"
	}
	return cfg
}

// replay 重放 WAL 重建内存状态。WAL 中事件先于状态读取的顺序天然保证：
// 消费组注册 -> 消息发布(fanout) -> 确认/拒绝/死信。
func (b *Broker) replay() error {
	return b.w.Replay(func(rec wal.Record) error {
		switch rec.Kind {
		case kindGroup:
			var e groupRegEvent
			if err := json.Unmarshal(rec.Data, &e); err != nil {
				return err
			}
			b.ensureGroup(e.Group)
		case kindPublished:
			var e publishedEvent
			if err := json.Unmarshal(rec.Data, &e); err != nil {
				return err
			}
			b.applyPublished(&e)
		case kindAcked, kindNacked, kindDead:
			var e groupEvent
			if err := json.Unmarshal(rec.Data, &e); err != nil {
				return err
			}
			b.applyGroupEvent(rec.Kind, &e)
		default:
			// 未知记录类型跳过，保持前向兼容。
		}
		return nil
	})
}

func (b *Broker) ensureGroup(name string) *groupState {
	g := b.groups[name]
	if g == nil {
		g = &groupState{
			name:      name,
			consumers: map[string]*consumerHandle{},
			keyMsgs:   map[string][]*msgState{},
			byID:      map[string]*msgState{},
			runners:   map[string]*runner{},
		}
		b.groups[name] = g
	}
	return g
}

func (b *Broker) applyPublished(e *publishedEvent) {
	if _, ok := b.messages[e.ID]; !ok {
		b.messages[e.ID] = &storedMessage{id: e.ID, key: e.Key, seq: e.Seq, payload: e.Payload}
	}
	if cur := b.keySeq[e.Key]; e.Seq > cur {
		b.keySeq[e.Key] = e.Seq
	}
	if e.DedupKey != "" {
		if _, ok := b.dedup[e.DedupKey]; !ok {
			b.dedup[e.DedupKey] = e.ID
		}
	}
	// 对发布时点已注册的全部消费组 fanout 一份独立状态。
	for _, g := range b.groups {
		if _, ok := g.byID[e.ID]; ok {
			continue
		}
		ms := &msgState{id: e.ID, key: e.Key, seq: e.Seq, status: stPending}
		g.byID[e.ID] = ms
		g.keyMsgs[e.Key] = append(g.keyMsgs[e.Key], ms)
	}
}

func (b *Broker) applyGroupEvent(kind string, e *groupEvent) {
	g := b.ensureGroup(e.Group)
	ms := g.byID[e.ID]
	if ms == nil {
		// 组在消息发布之后才注册：发布事件 fanout 时没有它，
		// 这条历史状态与它无关（消息流从订阅时刻开始）。
		return
	}
	switch kind {
	case kindAcked:
		ms.status = stAcked
	case kindNacked:
		ms.status = stPending
		ms.attempts = e.Attempt
		if e.NotBefore > 0 {
			ms.notBefore = time.Unix(0, e.NotBefore)
		}
		ms.errMsg = e.ErrMsg
	case kindDead:
		ms.status = stDead
		ms.attempts = e.Attempt
		ms.reason = FailureReason(e.Reason)
		ms.errMsg = e.ErrMsg
		dead := *ms
		g.dead = append(g.dead, &dead)
	}
}

// Publish 发布一条消息，返回发布结果（含消息 ID 与序号）。
// 相同 DedupKey 的重复发布只生效一次（幂等发布）。
func (b *Broker) Publish(ctx context.Context, msg Message, opts PublishOptions) (PublishResult, error) {
	if msg.Key == "" {
		return PublishResult{}, errors.New("mq: routing key is required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return PublishResult{}, errors.New("mq: broker closed")
	}
	if opts.DedupKey != "" {
		if existing, ok := b.dedup[opts.DedupKey]; ok {
			b.log.Printf("PUBLISH dedup-hit key=%s dedupKey=%s id=%s -> ignored",
				msg.Key, opts.DedupKey, existing)
			return PublishResult{ID: existing, Ignored: true}, nil
		}
	}

	seq := b.keySeq[msg.Key] + 1
	id := newMessageID(msg.Key)
	ev := publishedEvent{ID: id, Key: msg.Key, Seq: seq, DedupKey: opts.DedupKey, Payload: msg.Payload}
	if _, err := b.w.Append(kindPublished, mustJSON(&ev)); err != nil {
		return PublishResult{}, err
	}
	if opts.DedupKey != "" {
		b.dedup[opts.DedupKey] = id
	}
	b.applyPublished(&ev)

	res := PublishResult{ID: id, Seq: seq}
	b.log.Printf("PUBLISH id=%s key=%s seq=%d groups=%d", id, msg.Key, seq, len(b.groups))
	// 唤醒相关消费循环。
	for _, g := range b.groups {
		b.wakeLocked(g, msg.Key)
	}
	return res, nil
}

// Subscribe 创建/加入一个消费组，返回消费者句柄。
// 同一消费组可多次 Subscribe 以扩容；不同消费者按路由键哈希分担。
func (b *Broker) Subscribe(group GroupName, consumer ConsumerID, handler Handler) (*Consumer, error) {
	if group == "" || consumer == "" {
		return nil, errors.New("mq: group and consumer are required")
	}
	if handler == nil {
		return nil, errors.New("mq: handler is required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, errors.New("mq: broker closed")
	}
	g := b.ensureGroup(group)
	if _, exists := g.consumers[consumer]; exists {
		return nil, fmt.Errorf("mq: consumer %q already exists in group %q", consumer, group)
	}
	// 首次见到该组：持久化注册，使重启后组关系可恢复。
	if len(g.consumers) == 0 && !g.registered {
		if _, err := b.w.Append(kindGroup, mustJSON(&groupRegEvent{Group: group})); err != nil {
			return nil, err
		}
		g.registered = true
	}
	h := &consumerHandle{
		id:      consumer,
		handler: handler,
		sem:     make(chan struct{}, b.cfg.HandlerConcurrency),
	}
	g.consumers[consumer] = h
	c := &Consumer{broker: b, group: group, id: consumer}
	b.log.Printf("SUBSCRIBE group=%s consumer=%s members=%d", group, consumer, len(g.consumers))

	// 让该组所有已有 runner 重新评估归属。
	for key := range g.runners {
		b.wakeLocked(g, key)
	}
	// 为积压但尚无 runner 的键启动循环。
	for key := range g.keyMsgs {
		b.ensureRunnerLocked(g, key)
	}
	return c, nil
}

// leaveConsumer 将消费者移出消费组并触发再均衡（由 Consumer.Close 调用）。
func (b *Broker) leaveConsumer(group, consumer string) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	g := b.groups[group]
	if g == nil {
		b.mu.Unlock()
		return nil
	}
	h := g.consumers[consumer]
	if h != nil {
		h.closing = true
	}
	b.log.Printf("UNSUBSCRIBE group=%s consumer=%s", group, consumer)
	for key := range g.runners {
		b.wakeLocked(g, key)
	}
	b.mu.Unlock()
	// 等待以该消费者为归属的在途消息完成或被收回（有界等待）。
	deadline := time.Now().Add(b.cfg.ShutdownTimeout)
	for {
		b.mu.Lock()
		busy := false
		for _, r := range g.runners {
			if r.handlerConsumer == consumer && r.inflight != nil {
				busy = true
			}
		}
		b.mu.Unlock()
		if !busy || time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	b.mu.Lock()
	timedOut := false
	// 超时仍在途：取消其处理器 context 并收回消息为 pending，避免状态悬挂。
	for _, r := range g.runners {
		if r.handlerConsumer == consumer && r.inflight != nil {
			timedOut = true
			r.revokeInflightLocked("consumer-removal-timeout")
		}
	}
	delete(g.consumers, consumer)
	b.log.Printf("UNSUBSCRIBE-DONE group=%s consumer=%s remaining=%d timedOut=%v",
		group, consumer, len(g.consumers), timedOut)
	// 归属映射已改变，唤醒所有 runner 重新评估。
	for _, r := range g.runners {
		r.cond.Signal()
	}
	b.mu.Unlock()
	return nil
}

// DeadLetters 返回指定消费组当前的死信快照。
func (b *Broker) DeadLetters(group GroupName) []DeadLetterEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	g := b.groups[group]
	if g == nil {
		return nil
	}
	out := make([]DeadLetterEntry, 0, len(g.dead))
	for _, ms := range g.dead {
		out = append(out, DeadLetterEntry{
			ID:        ms.id,
			Key:       ms.key,
			Group:     group,
			Attempts:  ms.attempts,
			Reason:    ms.reason,
			LastError: ms.errMsg,
			Payload:   b.messages[ms.id].payload,
			DeadAt:    ms.deadAt,
		})
	}
	return out
}

// Close 优雅关闭：停止新投递，有界等待在途消息完成裁决并落盘；
// 超时仍未完成的在途消息不做任何确认落盘——重启后按未确认重投（at-least-once）。
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.closeDeadline = time.Now().Add(b.cfg.ShutdownTimeout)
	// 唤醒所有在 cond 上等待的空闲 runner，使其观察到 closed 并退出。
	for _, g := range b.groups {
		for _, r := range g.runners {
			r.cond.Broadcast()
		}
	}
	b.log.Printf("CLOSE begin: shutdownTimeout=%s, waiting in-flight", b.cfg.ShutdownTimeout)
	b.mu.Unlock()

	done := make(chan struct{})
	go func() { b.wg.Wait(); close(done) }()
	select {
	case <-done:
		b.log.Printf("CLOSE all in-flight settled gracefully")
	case <-time.After(b.cfg.ShutdownTimeout + 250*time.Millisecond):
		b.cancel() // 强制取消仍挂起的处理器
		b.log.Printf("CLOSE shutdown timeout; forcing cancel; unacked will redeliver after restart")
		<-done
	}
	b.mu.Lock()
	b.cancel()
	b.mu.Unlock()
	return b.w.Close()
}

// crashForTest 模拟进程被硬杀死：不等待在途、不优雅落盘业务确认，
// 仅停止消费循环、取消处理器并关闭 WAL。已 fsync 的事件保留，
// 未确认的在途消息重启后按 pending 重投。仅供测试使用。
func (b *Broker) crashForTest() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	for _, g := range b.groups {
		for _, r := range g.runners {
			r.cond.Broadcast()
		}
	}
	b.cancel()
	b.mu.Unlock()
	b.wg.Wait()
	return b.w.Close()
}

// ownerOf 返回 gk 当前负责的消费者（按存活消费者 ID 排序后哈希）。
func (g *groupState) ownerOf(key string) *consumerHandle {
	if len(g.consumers) == 0 {
		return nil
	}
	ids := make([]string, 0, len(g.consumers))
	for id := range g.consumers {
		// 正在缩容的消费者不再接收“下一条”消息，但允许其在途消息跑完。
		if g.consumers[id].closing {
			continue
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil
	}
	sort.Strings(ids)
	h := fnv.New32a()
	h.Write([]byte(key))
	idx := int(h.Sum32()) % len(ids)
	return g.consumers[ids[idx]]
}

func newMessageID(key string) string {
	var rb [4]byte
	rand.Read(rb[:])
	return fmt.Sprintf("m_%s_%d_%s", key, time.Now().UnixNano(), hex.EncodeToString(rb[:]))
}
