package mq

// Consumer 表示消费组内的一个消费者实例，负责运行被分配路由键的消费循环。
type Consumer struct {
	broker *Broker
	group  string
	id     string
	closed bool
}

// Group 返回消费组名。
func (c *Consumer) Group() string { return c.group }

// ID 返回消费者 ID。
func (c *Consumer) ID() string { return c.id }

// Close 将该消费者移出消费组并触发再均衡（缩容）。
// 在途消息会被等待完成或收回重投，绝不静默丢失。
func (c *Consumer) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	return c.broker.leaveConsumer(c.group, c.id)
}
