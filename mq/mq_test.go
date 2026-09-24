package mq

import (
	"sync"
	"testing"
	"time"
)

// testLogger 把系统日志转发到 testing 日志，便于在 -race 与 -v 下观察
// 消息标识、路由键、尝试次数和状态迁移。
type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) {
	l.t.Helper()
	l.t.Logf(format, args...)
}

// effects 是业务侧幂等账本：记录每个消息“成功生效”次数。
// 正确语义下每个被确认的消息恰好生效一次。
type effects struct {
	mu      sync.Mutex
	success map[string]int
	order   []string
}

func newEffects() *effects {
	return &effects{success: map[string]int{}}
}

func (e *effects) mark(id string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.success[id]++
	e.order = append(e.order, id)
}

func (e *effects) count(id string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.success[id]
}

func (e *effects) total() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.order)
}

func testConfig(t *testing.T, dir string) Config {
	return Config{
		Dir:                dir,
		MaxAttempts:        4,
		RetryDelay:         5 * time.Millisecond,
		MaxRetryDelay:      50 * time.Millisecond,
		RetryBackoff:       1,
		HandlerConcurrency: 4,
		ShutdownTimeout:    3 * time.Second,
		Logger:             testLogger{t},
	}
}

// waitStatus 等待某消息在指定消费组到达目标状态（用于等待框架确认/死信落盘，
// 避免在“业务已生效但 ACK 尚未落盘”的窗口里读到 inflight）。
func waitStatus(t *testing.T, b *Broker, group, id, want string, timeout time.Duration) {
	t.Helper()
	waitFor(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		ms := b.groups[group].byID[id]
		return ms != nil && ms.status == want
	}, timeout, "status="+want+" for "+id)
}

// waitGroupDrained 等待某消费组所有路由键的消息都进入终态（队列被压缩为空）。
func waitGroupDrained(t *testing.T, b *Broker, group string, timeout time.Duration) {
	t.Helper()
	waitFor(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		for _, msgs := range b.groups[group].keyMsgs {
			if len(msgs) != 0 {
				return false
			}
		}
		return true
	}, timeout, "group "+group+" drained")
}
