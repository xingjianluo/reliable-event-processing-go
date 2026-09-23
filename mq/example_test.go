package mq_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	mq "github.com/xingjianluo/reliable-event-processing-go/mq"
)

// discardLogger 丢弃系统日志，使示例输出只包含业务打印。
type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

// Example 展示完整用法：打开代理、订阅消费组、发布、显式 Ack/Nack、
// 死信查询与优雅关闭。
func Example() {
	dir, _ := os.MkdirTemp("", "mq-example-")
	defer os.RemoveAll(dir)

	broker, err := mq.OpenBroker(mq.Config{
		Dir:                filepath.Join(dir, "data"),
		MaxAttempts:        3,
		RetryDelay:         10 * time.Millisecond,
		MaxRetryDelay:      time.Second,
		RetryBackoff:       2,
		HandlerConcurrency: 8,
		ShutdownTimeout:    time.Second,
		Logger:             discardLogger{},
	})
	if err != nil {
		panic(err)
	}
	defer broker.Close()

	consumer, err := broker.Subscribe("orders", "worker-1",
		func(ctx context.Context, d *mq.Delivery) mq.Decision {
			// 业务处理应自身幂等：可靠投递在崩溃恢复下可能重投。
			if len(d.Payload()) == 0 {
				// 不可恢复失败：立即进入死信。
				return mq.DropDecision(errors.New("empty payload"))
			}
			return mq.AckDecision()
		})
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	res, err := broker.Publish(context.Background(),
		mq.Message{Key: "orders", Payload: []byte("hello")},
		mq.PublishOptions{DedupKey: ""})
	if err != nil {
		panic(err)
	}
	fmt.Printf("published id=%s seq=%d\n", res.ID[:1]+"...", res.Seq)

	// 幂等发布：相同 DedupKey 只生效一次。
	dup, _ := broker.Publish(context.Background(),
		mq.Message{Key: "orders", Payload: []byte("hello")},
		mq.PublishOptions{DedupKey: "order-42"})
	again, _ := broker.Publish(context.Background(),
		mq.Message{Key: "orders", Payload: []byte("hello")},
		mq.PublishOptions{DedupKey: "order-42"})
	fmt.Printf("dedup ignored=%v sameID=%v\n", again.Ignored, again.ID == dup.ID)

	time.Sleep(50 * time.Millisecond)
	fmt.Println("done processing")
	// Unordered output:
	// published id=m... seq=1
	// dedup ignored=true sameID=true
	// done processing
}