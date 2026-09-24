package mq

import "encoding/json"

// WAL 记录类型。
const (
	kindGroup     = "group"     // 消费组首次注册
	kindPublished = "published" // 消息发布（对所有已存在的消费组 fanout 一份状态）
	kindAcked     = "acked"     // 某消费组确认某消息
	kindNacked    = "nacked"    // 某消费组拒绝某消息（安排重试）
	kindDead      = "dead"      // 某消费组将某消息打入死信
)

// groupRegEvent: 消费组首次注册。
type groupRegEvent struct {
	Group string `json:"g"`
}

// publishedEvent: 一条消息发布。payload 存一次，按 (key,seq) 引用。
type publishedEvent struct {
	ID       string `json:"id"`
	Key      string `json:"key"`
	Seq      int64  `json:"seq"`
	DedupKey string `json:"dedup,omitempty"`
	Payload  []byte `json:"p"`
}

// groupEvent 是针对某消费组的状态变更（确认/拒绝/死信）。
type groupEvent struct {
	Group     string `json:"g"`
	ID        string `json:"id"`
	Key       string `json:"k"`
	Seq       int64  `json:"s"`
	Attempt   int    `json:"a,omitempty"`  // 拒绝/死信时已发生的尝试次数
	Reason    string `json:"r,omitempty"`  // 死信原因
	ErrMsg    string `json:"e,omitempty"`  // 失败描述
	NotBefore int64  `json:"nb,omitempty"` // 重试到期时间（UnixNano）
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		// 事件结构均为可 JSON 编码的基础类型，不应失败。
		panic(err)
	}
	return b
}
