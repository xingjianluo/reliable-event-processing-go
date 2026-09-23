package mq

import (
	"context"
	"time"
)

// RoutingKey 是消息流标识。同一个路由键内的消息严格按发布顺序串行处理。
type RoutingKey = string

// MessageID 是消息全局唯一标识。
type MessageID = string

// GroupName 是消费组名；不同消费组各自维护独立的消费进度。
type GroupName = string

// ConsumerID 是消费组内某个消费者实例的标识。
type ConsumerID = string

// FailureReason 描述消息进入死信的可区分原因。
type FailureReason string

const (
	// ReasonRetryExhausted 表示达到最大重试次数仍失败。
	ReasonRetryExhausted FailureReason = "retry_exhausted"
	// ReasonUnrecoverable 表示处理器声明的不可恢复失败。
	ReasonUnrecoverable FailureReason = "unrecoverable"
)

// Message 是发布到某个路由键的消息。
type Message struct {
	Key     RoutingKey
	Payload []byte
}

// PublishOptions 控制发布行为。
type PublishOptions struct {
	// DedupKey 用于发布端去重：相同 DedupKey 只生效一次。空串表示不去重。
	DedupKey string
}

// PublishResult 是发布结果。
type PublishResult struct {
	ID      MessageID
	Seq     int64 // 该消息在路由键内的序号（从 1 开始）
	Ignored bool  // 因 DedupKey 命中而被忽略（幂等发布）
}

// Handler 处理单条消息。返回 Decision 决定确认或拒绝。
// 同一 gk 的消息不会被并发调用；不同 gk 可以并发。
type Handler func(ctx context.Context, d *Delivery) Decision

// Decision 是处理器对一次投递的裁决。
type Decision struct {
	// Ack 为 true 表示确认成功；false 表示拒绝。
	Ack bool
	// RequeueDelay 拒绝时的重试延迟；为 0 使用配置默认值。
	RequeueDelay time.Duration
	// Unrecoverable 拒绝时若为 true，直接进入死信，不再重试。
	Unrecoverable bool
	// Err 附带的失败原因描述。
	Err error
}

// AckDecision 构造一个确认裁决。
func AckDecision() Decision { return Decision{Ack: true} }

// RetryDecision 构造一个可重试的拒绝裁决。
func RetryDecision(err error, delay time.Duration) Decision {
	return Decision{Ack: false, RequeueDelay: delay, Err: err}
}

// DropDecision 构造一个不可恢复、直接死信的拒绝裁决。
func DropDecision(err error) Decision {
	return Decision{Ack: false, Unrecoverable: true, Err: err}
}

// Config 是 Broker 配置。
type Config struct {
	// Dir 持久化目录。
	Dir string
	// MaxAttempts 最大投递尝试次数（首次为 1）；超过则死信。
	MaxAttempts int
	// RetryDelay 默认重试延迟。
	RetryDelay time.Duration
	// MaxRetryDelay 重试延迟上限。
	MaxRetryDelay time.Duration
	// RetryBackoff 重试退避乘数；<=1 表示固定延迟。
	RetryBackoff float64
	// HandlerConcurrency 每个消费者可并发处理的不同路由键数量。
	HandlerConcurrency int
	// ShutdownTimeout Close 时等待在途消息的最长时间。
	ShutdownTimeout time.Duration
	// Logger 日志输出；nil 使用默认 logger。
	Logger Logger
}

// Logger 是系统内部日志接口。
type Logger interface {
	Printf(format string, args ...any)
}

// DeadLetterEntry 是一条进入死信的消息记录。
type DeadLetterEntry struct {
	ID        MessageID
	Key       RoutingKey
	Group     GroupName
	Attempts  int
	Reason    FailureReason
	LastError string
	Payload   []byte
	DeadAt    time.Time
}
