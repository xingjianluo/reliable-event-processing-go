# reliable-event-processing-go

一个**纯本地、零外部依赖**的可靠消息投递与消费库（Go）。消息持久化在本地
追加日志（WAL），消费在进程内并发完成。目标是在**并发、重启、异常退出**
场景下行为可解释、可复现。

包路径：`github.com/xingjianluo/reliable-event-processing-go/localmq`

---

## 1. 快速开始

```go
cfg := localmq.DefaultConfig("./data") // 所有超时/重试均可在此覆盖
broker, err := localmq.New(cfg)
if err != nil { log.Fatal(err) }
defer broker.Close()

err = broker.Subscribe("orders", "worker-1", func(ctx context.Context, d *localmq.Delivery) error {
    // 处理 d.Message.Body（路由键为 d.Message.RouteKey）
    return d.Ack() // 也可以直接 return nil（等价于 Ack）
})
if err != nil { log.Fatal(err) }

id, err := broker.Publish(ctx, "order.created", payload, "") // id 为空则自动生成
```

- `Publish(routeKey, body, id)`：落盘成功后返回；`id` 由调用方给定则可用于
  业务幂等键。
- 处理器三种结局：
  - `d.Ack()` 或返回 `nil`：成功，位点持久化，永不重投；
  - `d.Nack(retryAfter)` 或返回非 nil 错误：临时失败，进入退避重试；
  - `d.Reject(reason)`：不可恢复失败，**立即**进入死信。
  - 处理器 `panic`：被捕获并恢复，消息以 `ReasonPanic` 立即死信，不拖垮进程。

---

## 2. 投递语义（Delivery Semantics）

### 至少一次（At-least-once）+ 业务幂等

- 成功的**确认（Ack）先落盘 fsync 后返回**。已确认的消息在任何重启后都
  **不会**再次投递。
- 但“业务执行成功”和“Ack 落盘”是两个动作，中间崩溃无法做到恰好一次，因此
  系统提供的是**至少一次**：若消息在处理器执行期间、Ack 落盘前崩溃，重启后
  会被重新投递。
- **业务侧必须幂等**。推荐做法：以 `Message.ID` 作为幂等键，先做
  “存在性检查/去重落库”，执行业务，再 Ack；即使消息被重复投递，业务状态也只
  生效一次（见 `dedup_test.go` 的 `effectStore` 范式）。

### 同一消息不会被两个消费者同时成功提交

- 每条消息的终态（Ack/死信）是存储层的一次原子状态迁移；第一个落盘的结算
  生效，之后的重复 Ack/Nack 返回 `localmq.ErrAlreadySettled`。
- 同一时刻，同一路由键最多只有**一个**在途投递（见顺序模型），因此从机制上
  排除了两个消费者同时持有同一消息。

### 结算优先级

处理器内部若显式调用了 `Ack/Nack/Reject`，则以**先发生的显式调用为准**，
处理器随后的返回值被忽略；未显式结算时，`nil`→Ack、非 nil→Nack。每个
`Delivery` 的结算只生效一次。

---

## 3. 顺序与并发边界（Ordering & Concurrency）

- **同一路由键内严格有序**：调度器对每个路由键维护“忙”标记，键在“投递 → 首个
  终态结算（含重试退避期）”整个区间内处于忙状态，期间绝不会投递该键的下一条
  消息。因此即便某条消息处理很慢，其后续消息也不会越过它；重试也在同一顺序
  轨道上。
- **不同路由键可并行**：消息按路由键哈希（FNV-1a）分配给消费组内存活的消费者，
  不同键会落到不同消费者并并行处理。
- **消费者并发度**：默认每个消费者并发度为 1（处理器调用也严格串行）。可用
  `localmq.WithConcurrency(n)` 提高；并发度提高只影响同一消费者名下**不同路由
  键**的并行度，broker 仍然保证“同一键同时只有一个在途投递”，所以不破坏按键顺序。
- **扩容 / 缩容**：
  - 成员变更后按排序后的消费者名单重新做稳定哈希映射，任一时刻一个键只有一个
    属主消费者。
  - `Unsubscribe` 会**阻塞等待该消费者所有在途处理器执行完并把结果落盘**，然后
    其占用的键才重新可被其余消费者领取，缩容不丢消息、不重复成功。
  - 重新分配是尽力而为：只影响尚未投递/未确认的消息，已在途的消息会跑到终态，
    不做抢占式迁移（避免同消息双跑）。

> 注意：如果单一路由键上的消息持续处理缓慢，由于严格按键串行，该键吞吐会受限——
> 这是“严格顺序”与“单键高吞吐”之间有意的取舍。需要并行时应使用更细粒度的路由键。

---

## 4. 重试与死信规则（Retry & DLQ）

- `Config.MaxAttempts`（默认 5）为最大投递次数（含首次）。第 N 次投递仍临时
  失败时：
  - `N < MaxAttempts`：记录一次 Nack，`attempts++`，按退避延迟后重投；
  - `N == MaxAttempts`：进入死信，原因为 `max_attempts`。
- 退避（确定性指数，无随机抖动，便于复现）：

  ```
  delay(第 n 次失败后) = min(RetryBaseDelay * 2^(n-1), RetryMaxDelay)
  ```

  处理器也可在 `Nack(retryAfter)` 中显式指定本次延迟覆盖配置值。
- 重试尝试次数是**持久化状态**：崩溃发生在退避等待期间，重启后尝试次数不会清零，
  不会因反复重启而无限重试。
- 三类可区分的死信原因（`DeadLetter.Reason`）：
  | Reason            | 触发条件                         |
  |-------------------|----------------------------------|
  | `max_attempts`    | 临时失败达到 `MaxAttempts` 上限  |
  | `rejected`        | 处理器显式 `Reject(reason)`      |
  | `panic`           | 处理器 panic（捕获恢复后立即死信）|
- 死信与正常流隔离：进入 DLQ 的消息不再参与调度，但可通过
  `broker.DeadLetters(group)` 查询（含原因、详情、最终尝试次数、死亡时间）。
- 各消费组的进度、重试与死信**互相独立**：同一条发布在不同组独立消费、独立死信。

---

## 5. 持久化与恢复（Persistence & Recovery）

### 存储结构

- 单一 WAL 文件 `<Dir>/wal.log`，所有组的事件按全局追加顺序写入，帧格式：

  ```
  [4B 大端 payload 长度][4B CRC32(payload)][payload(JSON 事件)]
  ```

- 每条事件类型：发布(Published)、确认(Acked)、否定(Nacked，含下次可投递时间)、
  死信(Dead)。每次写事件后按 `Fsync` 配置执行 `fsync`（默认开启）。
- 启动时**顺序重放**整条日志重建内存态：消息只存一份（全局），每个消费组维护
  各自的逐消息状态与按路由键的序号。

### 异常退出的三种情形（均可复现）

| 崩溃时点 | 日志中有什么 | 重启后行为 |
|---|---|---|
| 已发布、未投递 | Published | 正常投递 |
| 处理器执行中（Ack 前） | 仅 Published（该组无终态记录） | **重新投递**（至少一次），需业务幂等 |
| Ack/Nack/死信落盘后 | 含对应终态事件 | Ack/死信的消息不再投递；Nack 的按已持久化的退避与尝试次数继续 |

- **已确认不丢、不重提**：Ack 是先落盘后返回的，恢复后已确认位点之前的消息不会
  再被当作成功提交。
- **崩溃写坏半条帧**：掉电/`SIGKILL` 只可能损坏最后一条半写帧。打开时按长度+CRC
  校验扫描，遇到不完整或 CRC 不符的尾部即**截断到最后一条完整帧**，已 fsync 的
  历史完整无损（见 `wal_test.go` 的撕裂尾帧用例）。

### 关闭（优雅/超时）

`Close()` 的行为是显式的，不静默丢消息、不留状态悬空：

1. 停止调度，**不再产生新投递**（`Publish/Subscribe` 返回 `ErrBrokerClosed`）；
2. 在途处理器继续运行，且在宽限期内**仍可 Ack/Nack**，结果照常 fsync；
3. 等待至多 `Config.CloseTimeout`，让所有在途处理器跑完；
4. 若超时：取消处理器 context、拒绝迟到的结算，`Close` 返回明确错误。这些未确认
   消息在下次打开时被重新投递——既不丢，也不会被错误标记为成功。

---

## 6. 配置项（Config）

| 字段 | 默认值 | 说明 |
|---|---|---|
| `Dir` | 必填 | WAL 所在目录，不存在会创建 |
| `MaxAttempts` | `5` | 最大投递次数（含首次），`<1` 回落到默认 |
| `RetryBaseDelay` | `10ms` | 指数退避基数 |
| `RetryMaxDelay` | `1s` | 指数退避上限 |
| `ScheduleTick` | `5ms` | 调度器扫描就绪消息/重试到期的周期 |
| `Fsync` | `true` | 每条记录是否 fsync；关闭可提速，但 OS/主机崩溃可能丢最近记录（进程崩溃仍安全）|
| `CloseTimeout` | `10s` | 优雅关闭等待在途处理器的最长时间 |
| `Logger` | `slog.Default()` | 结构化日志（记录 msg_id / route / group / consumer / attempt / state 等迁移）|

---

## 7. 本地验证

要求 Go 1.25+。

```bash
# 全部单测（普通）
go test ./...

# race detector，多轮，确保并发稳定（CI 建议）
go test -race -count=10 ./...

go vet ./...
```

测试覆盖矩阵（`localmq/*_test.go`）：

| 场景 | 测试 |
|---|---|
| 基础发布/消费、同键顺序 | `TestBasicPublishConsume` |
| 多消费者并行、同键顺序、只成功一次、键单一属主 | `TestConcurrentConsumers` |
| 重复投递下业务幂等、重复 Ack 被拒 | `TestDuplicateDeliveryIdempotent`、`TestDoubleAckRejected` |
| 不同路由键乱序确认互不阻塞、各键内部有序 | `TestOutOfOrderAck` |
| 重试耗尽进死信（次数/原因） | `TestRetryExhaustion` |
| Reject 立即死信 | `TestRejectIsImmediateDead` |
| panic 恢复+立即死信+与正常消息隔离 | `TestPanicDeadLetters` |
| 消费组扩缩容交接、不丢不重 | `TestScaleUpDown`、`TestUnsubscribeWaitsInflight` |
| 多消费组独立消费/独立死信 | `TestConsumerGroupsIndependent` |
| 发布后/在途崩溃/Ack 后重启恢复 | `TestRecovery*` |
| 优雅关闭在途 Ack 持久化、超时中止后重投 | `TestGracefulCloseAcksInflight`、`TestCloseTimeoutAborts` |
| WAL 撕裂尾帧截断、顺序回放 | `TestWALTruncatesTornTail`、`TestWALOrderAndPayload` |

单测日志默认输出到 stdout（slog TextHandler），每条投递与结算都会打印
**消息标识(msg_id)、路由键(route)、尝试次数(attempt)、状态迁移(state)**，例如：

```
msg=deliver msg_id=... route=order.created group=orders consumer=worker-1 attempt=1 state=delivered
msg=settle  msg_id=... route=order.created group=orders consumer=worker-1 attempt=1 state=acked
```

---

## 8. 设计边界与取舍

- 单进程、单 WAL、无网络；适合作为本地可靠出站箱（outbox）/单机任务队列，
  非跨进程的分布式 MQ。
- 当前实现不做日志压缩（compaction）：长时间运行 WAL 会增长；所有状态都可由
  日志重放重建。后续如需，可在全部组某位点之前均为终态时安全截断。
- 严格按路由键顺序优先于单键吞吐；需要并行请细化路由键。
- 至少一次而非恰好一次：重复投递的去重由业务幂等承担，broker 保证“确认后绝不
  重投、终态只生效一次”。
