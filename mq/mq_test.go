package mq

import (
	"fmt"
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

// waitKeyState 轮询公开 API KeyStatus，直到该键运行态到达目标态。
// 用于等待“业务生效 -> 处理器返回 -> ACK fsync 落盘 -> 内存态转终态”
// 整个窗口闭合：不依赖固定 sleep，也不放宽断言语义（超时即失败）。
// drained/dead 等终态一旦到达不会再回退，因此轮询到目标态时断言必然成立。
func waitKeyState(t *testing.T, b *Broker, group, key string, want KeyState, timeout time.Duration) {
	t.Helper()
	waitFor(t, func() bool { return b.KeyStatus(group, key).State == want },
		timeout, "KeyStatus state="+string(want)+" for "+group+"/"+key)
}

// invariantError 严格校验一次 KeyStatus 快照的内部一致性，返回首个不自洽点。
// 它不放宽任何语义：每个字段都必须与 State/WaitReason/Paused 自洽；
// 只要快照声称有未完成队头，就不得表现为 drained；HeadSeq/Backlog
// 必须自洽（Backlog 条未完成消息的序号必然都晚于队头）。
// 用于并发“连续读取运行态”场景，对读到的每一帧都校验，
// 确保任何一瞬间的中间状态都不会产生自相矛盾的快照。
func invariantError(s KeySnapshot) error {
	if s.State == "" {
		// 未知 (group,key)：键不存在，不得报告 Exists/队头；
		// 唯一合法例外是“仅暂停、从无消息记录”——那种情况 State 应为 paused，
		// 因此走到这里时 Paused 也必须为 false，WaitReason 必须为空。
		if s.Exists || s.Paused || s.WaitReason != "" || s.HeadSeq != 0 || s.HeadID != "" || s.Backlog != 0 {
			return fmt.Errorf("inconsistent unknown snapshot: %+v", s)
		}
		return nil
	}
	switch s.State {
	case StateDrained:
		if s.WaitReason != ReasonNone || s.Paused || s.HeadSeq != 0 || s.HeadID != "" || s.Backlog != 0 {
			return fmt.Errorf("drained snapshot must carry no unfinished head: %+v", s)
		}
	case StatePaused:
		if !s.Paused || s.WaitReason != ReasonPaused {
			return fmt.Errorf("paused snapshot inconsistent: %+v", s)
		}
		// 两种合法形态：暂停且有未完成队头；或键已清空（消息全部终态、
		// 队列已裁剪）但暂停位仍在——暂停优先报告，此时不得携带队头字段。
		if s.HeadSeq == 0 {
			if s.HeadID != "" || s.Backlog != 0 {
				return fmt.Errorf("paused-but-empty snapshot must carry no head: %+v", s)
			}
		} else if s.HeadID == "" {
			return fmt.Errorf("paused snapshot with head seq lacks head id: %+v", s)
		}
	case StateProcessing:
		if s.Paused || s.WaitReason != ReasonInFlight || s.HeadSeq <= 0 || s.HeadID == "" || s.HeadAttempts < 1 {
			return fmt.Errorf("processing snapshot inconsistent: %+v", s)
		}
	case StateWaitingRetry:
		// 到期边界帧是合法的：计时器尚未唤醒的那一瞬间，状态仍是
		// waiting_retry 而 HeadNotBefore 可能恰好不晚于 now。
		// 因此只要求记录了非零的到期时间，不强制它一定在未来。
		if s.Paused || s.WaitReason != ReasonRetryDelay || s.HeadSeq <= 0 || s.HeadAttempts < 1 || s.HeadNotBefore.IsZero() {
			return fmt.Errorf("waiting_retry snapshot inconsistent: %+v", s)
		}
	case StateReady:
		if s.Paused || s.WaitReason != ReasonNone || s.HeadSeq <= 0 || s.HeadID == "" {
			return fmt.Errorf("ready snapshot inconsistent: %+v", s)
		}
	case StateNoConsumer:
		if s.Paused || s.WaitReason != ReasonNoConsumer || s.HeadSeq <= 0 || s.HeadID == "" {
			return fmt.Errorf("no_consumer snapshot inconsistent: %+v", s)
		}
	default:
		return fmt.Errorf("unknown snapshot state: %+v", s)
	}
	if s.Backlog < 0 {
		return fmt.Errorf("negative backlog: %+v", s)
	}
	return nil
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
