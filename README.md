# 本地可靠消息投递与消费（Go）

一个**仅依赖本地文件与进程内并发**的可靠消息系统：不依赖任何外部中间件
（无 Kafka / RabbitMQ / Redis / 数据库服务）。消息按路由键（routing key）
发布与消费，支持至少一次投递、确认 / 拒绝、延期重试、死信隔离，以及
进程重启与异常退出后的状态恢复。

- 包路径：`github.com/xingjianluo/reliable-event-processing-go/mq`
- 持久化：`mq/wal`（单文件追加日志 + `fsync` + CRC 校验）
- 并发：每个 `(消费组, 路由键)` 一个 goroutine，`sync.Cond` 协调唤醒

---

## 1. 快速开始

```go
broker, _ := mq.OpenBroker(mq.Config{Dir: "./data"})
defer broker.Close()

consumer, _ := broker.Subscribe("orders", "worker-1",
    func(ctx context.Context, d *mq.Delivery) mq.Decision {
        // 业务处理必须幂等（见“投递语义”）。
        if err := handle(d.Payload()); err != nil {
            // 临时失败：按配置延期重试。
            return mq.RetryDecision(err, 0)
        }
        return mq.AckDecision() // 成功确认
    })
defer consumer.Close()

res, _ := broker.Publish(ctx,
    mq.Message{Key: "orders", Payload: body},
    mq.PublishOptions{DedupKey: "biz-id-123"}) // 可选：发布端幂等
```

处理器既可以**返回 `Decision`**，也可以在处理过程中显式调用
`d.Ack()` / `d.Nack(...)`；二者等价，且**只有第一个裁决生效**，
重复或过期的确认 / 拒绝会被安全忽略。

完整可运行示例见 [`mq/example_test.go`](mq/example_test.go)。

---

## 2. 投递语义（Delivery Semantics）

系统提供 **至少一次（at-least-once）投递**：

- 消息在**确认（ACK）落盘之前**不视为成功。ACK 先写 WAL 并 `fsync`，
  然后才更新内存状态。因此“业务已成功但确认丢失”的情况，会在恢复后
  **重新投递**该消息。
- 因此，**业务处理器自身必须幂等**。框架在每次投递中提供稳定的
  `Message ID`、`Routing Key`、`Seq` 与 `Attempt`，业务应据此去重
  （例如以 Message ID / 业务单号作为唯一键）。
- 在“确认之后崩溃”的情况下，确认已持久化，消息**绝不会**被再次投递，
  也不会把已确认位点之前的消息重新提交为成功。

> 一句话：**可能重复投递，但绝不丢失、绝不把已确认消息再提交成功。**
> “恰好一次生效”由业务幂等 + 框架去重能力共同实现。

框架提供两层去重帮助：

1. **发布幂等**：`PublishOptions.DedupKey` 相同的多次发布只生效一次
   （返回相同 Message ID，后一次 `Ignored=true`）。
2. **裁决幂等**：同一条投递的重复 / 乱序 `Ack/Nack` 只接受第一次；
   针对已终态消息的迟到裁决被忽略并记录 `DECISION-IGNORE` 日志。

---

## 3. 顺序与并发边界

### 顺序

- **同一路由键严格有序**：每个 `(消费组, 路由键)` 只有一个消费循环，
  同一时刻该键最多一条消息处于在途（in-flight）。前一条未裁决
  （ACK / NACK / 死信）之前，后一条不会被投递（队头阻塞）。
- 消息按该键内单调递增的 `Seq`（从 1 开始）处理；处理器可通过
  `d.Seq()` 获取。
- 前一条临时失败进入延期重试时，后一条消息**不会越过它**先行处理，
  从而保证严格顺序（代价是慢 / 失败消息会阻塞同键后续消息）。

### 并发

- **不同路由键并行**：不同键拥有独立 goroutine，可在多个 CPU 上并发。
- **多个消费组并行且独立**：每个消费组维护各自独立的消费进度。
  同一条消息会被每个消费组各消费一次（fan-out）。
- **同一消费组可扩容多个消费者**：`Subscribe(group, consumerID, h)`
  多次调用即扩容。路由键按 `FNV-1a(key) mod 消费者数`（消费者 ID
  排序后）分配给固定消费者，因此**同键始终落在同一消费者**，
  不同键分散到不同消费者。
- 每个消费者的跨键并发度由 `HandlerConcurrency` 限制（信号量），
  防止无限 goroutine。
- 消费者处理速度不一致时，只会影响分配给它的那些键，不影响其他键。

### 扩缩容

- 扩容 / 缩容（`Consumer.Close()`）会触发**再均衡**：归属映射改变后，
  各键循环重新评估负责消费者。
- **已经在途的消息允许原处理器跑完并落盘裁决**；只有“下一条”消息
  按新归属投递。缩容时会有界等待在途消息完成，超时则收回。
- 无论如何扩缩容，一条消息在同一消费组内**同一时刻只被一个处理器
  成功提交**，最终 **恰好成功一次或进入死信**（见压力测试
  `TestStressScaleAndFailures`）。

---

## 4. 确认、拒绝、重试与死信

处理器对每次投递返回（或显式调用）以下裁决之一：

| 裁决 | 行为 |
| --- | --- |
| `AckDecision()` / `d.Ack()` | 确认成功：写 `acked` 事件并 `fsync`，消息进入终态，不再投递 |
| `RetryDecision(err, delay)` / `d.Nack(...)` | 临时失败：进入延期重试 |
| `DropDecision(err)` / `Nack{Unrecoverable:true}` | 不可恢复失败：**立即**进入死信，不再重试 |

### 重试规则

- `Attempt` 从 1 开始，每次（重新）投递递增；NACK 时把当前尝试次数
  与下次到期时间持久化。
- 延迟取 `RetryDecision` 显式指定的 `delay`；为 0 时使用配置：
  `RetryDelay × RetryBackoff^(attempt-1)`，并以 `MaxRetryDelay` 封顶。
- 重试到期时间随 NACK 事件落盘，**重启后继续沿用**，尝试次数跨重启累计。
- 当 `Attempt >= MaxAttempts` 仍失败，进入死信，原因记为
  **`retry_exhausted`（重试耗尽）**。

### 死信（Dead Letter）

两种可区分的进入原因（`DeadLetterEntry.Reason`）：

- `mq.ReasonRetryExhausted = "retry_exhausted"`：达到最大尝试次数仍失败；
- `mq.ReasonUnrecoverable = "unrecoverable"`：处理器声明的不可恢复失败
  （首次投递即隔离）。

死信记录包含：消息 ID、路由键、消费组、累计尝试次数、原因、最后一次
错误描述、原始负载与进入时间。通过 `broker.DeadLetters(group)` 获取快照。

死信与正常队列**隔离**：死信消息不再被投递、不阻塞同键后续消息、
也不会与成功结果重叠（一条消息不可能既成功又死信）。

---

## 5. 按路由键的投递控制（暂停 / 恢复 / 运行态快照）

线上排障时可以按 `(消费组, 路由键)` 临时停下一条业务线，确认积压与
等待原因后再放行，且不破坏同键顺序。

### 暂停与恢复

```go
_ = broker.Pause("orders", "worker-1")  // 暂停 (orders 组, worker-1 键)
snap := broker.KeyStatus("orders", "worker-1") // 查看运行态快照
_ = broker.Resume("orders", "worker-1") // 显式恢复
```

- **暂停（`Pause`）**：该键的新消息与**已经到期的重试**都继续排队，
  不交给处理器；已在途的消息允许跑完并落盘裁决（不收回、不取消）。
  其他路由键、其他消费组完全不受影响，不会互相拖慢。
- **恢复（`Resume`）**：从该键**最早未完成**的消息继续投递，后续消息
  严格排在其后，不会插到前面。
- 暂停状态先写 WAL（`pause` 记录）再生效，**跨重启保留**：重启后仍
  保持暂停，直到显式 `Resume`。`Pause` / `Resume` 均幂等，重复调用
  不产生额外日志记录。
- 暂停只作用于指定 `(消费组, 路由键)`；同一键在别的消费组、同组内
  其他键都正常消费。

### 运行态快照（`KeyStatus`）

`KeyStatus(group, key)` 返回只读快照 `KeySnapshot`，用于直接判断
“为什么没往下走”：

| 字段 | 含义 |
| --- | --- |
| `Exists` | 该 `(group, key)` 是否有消息记录 |
| `Paused` | 是否处于暂停状态 |
| `State` | `ready` / `processing` / `waiting_retry` / `paused` / `no_consumer` / `drained` |
| `WaitReason` | 未推进原因：`none` / `paused` / `retry_delay` / `in_flight` / `no_consumer` |
| `HeadID` / `HeadSeq` / `HeadAttempts` | 最早未完成消息的 ID、序号与已尝试次数 |
| `HeadNotBefore` | 队头预计下次可投递时间（零值 = 立即可投递） |
| `Backlog` | 队头之后尚未完成的消息条数（不含队头） |

语义保证：

- **未完成 = 未确认且未死信**。快照绝不会把尚未成功的消息显示成
  已处理；全部终态后报告 `drained`。
- 读取只在持锁瞬间拷贝状态，**不会打断**正在进行的消费，也不改变
  任何投递行为；常见积压规模下读取为一次线性扫描，无额外持久化开销。
- 暂停与重试叠加时，`State` 优先报告 `paused`（暂停是更直接的可干预
  原因），`HeadNotBefore` 仍给出重试到期时间。
- 若键上消息已全部进入终态（队列被裁剪）但暂停位仍在，快照报告
  `paused` 且不携带队头字段（paused-but-empty）——“暂停”是更
  actionable 的状态；显式 `Resume` 后即为 `drained`。

#### 运行态读取与 ACK 落盘的语义边界（重要）

处理器内“业务生效”与框架“ACK 已落盘”是两个先后分离的时刻：

```
处理器执行业务副作用(eff.mark)  ->  处理器返回 Decision
   ->  WAL 追加 acked 记录并 fsync  ->  内存态翻转为 acked / 队列裁剪
   ->  KeyStatus 才可能报告 drained
```

因此：

1. **业务侧观测到生效后，立刻读 `KeyStatus` 仍可能得到 `processing`
   （或短暂的 `ready`/`waiting_retry`）**。这不是误报，恰恰是
   “不把还没成功（ACK 未落盘）的消息当成已完成”的正确语义：
   此刻若进程崩溃，该消息按 at-least-once 必须重投。
2. **`drained` 是单调终态**：一旦读到 `drained`，对应 ACK 必然已经
   fsync 落盘，之后不会再回退到 `processing`/`ready`（前提是没有向
   该键发布新消息）。所以需要“处理完了”的确定信号时，应当**轮询
   `KeyStatus` 直到 `State == drained`**，而不是在业务计数刚到齐时
   做一次性读取断言；本仓库测试统一使用 `waitKeyState(...)` 等待终态，
   不依赖固定 sleep，超时即判失败。
3. 在途窗口内读到 `processing`、重试到期边界读到 `waiting_retry`
   （`HeadNotBefore` 恰好不晚于当前时刻、但计时 goroutine 尚未唤醒）
   都是合法的瞬时帧，调用方不得把这类中间帧当作失败。

### 与既有行为的兼容边界

- 暂停 / 恢复与延期重试、扩缩容、进程重启可任意叠加：同键严格按序、
  至少一次、已确认消息不重投、不重复成功提交的语义全部保持不变。
- 缩容收回的在途消息回到 pending 后，若键已暂停则继续排队，恢复后
  仍从队头按序重投。
- 旧版本数据目录**完全兼容**：`pause` 是新记录类型，旧日志中没有它，
  重放时等价于“从未暂停”；未知记录类型重放时跳过，前向兼容。

---

## 6. 持久化与重启恢复

### WAL

所有状态变迁先追加到 `events.wal` 并 `fsync`，然后才更新内存：

- 帧：`magic | seq | kindLen | dataLen | kind | data | crc32`（大端），
  CRC32(IEEE) 覆盖序号与负载。
- 记录类型：`group`（消费组注册）、`published`（消息发布）、
  `acked` / `nacked` / `dead`（消费组级状态变迁）、
  `pause`（按路由键暂停 / 恢复投递）。
- 每条 `published` 消息负载只存一份；各消费组通过 `(key, seq)` 引用，
  并各自维护投递状态。
- 打开时重放全部记录重建内存。遇到**异常退出残留的撕裂尾帧**
  （短读 / CRC 不符）会在重放后截断到最后一条完整记录边界。
- 打开时对 WAL 文件加 `flock` 排他锁，禁止同一数据目录被两个进程
  同时打开。

### 三种崩溃位置的恢复行为

| 崩溃位置 | 恢复行为 |
| --- | --- |
| **确认前**（尚未处理 / 待投递） | 消息状态为 pending，恢复后重新投递（至少一次） |
| **处理中**（已投递、未确认） | 在途状态未持久化，恢复时仍为 pending，重新投递；业务幂等保证不重复生效 |
| **确认后**（ACK 已落盘） | 状态为 acked，恢复后**绝不**重投，位点之前的消息不会被再次提交成功 |

NACK（等待重试中）崩溃时，尝试次数与到期时间已持久化，恢复后继续计时
并从累计尝试次数重试，见 `TestRestartRetryStatePreserved`。

### 优雅关闭（`Broker.Close` / `Consumer.Close`）

1. 设置关闭标志，**不再投递新消息**，并唤醒所有消费循环；
2. 有界等待所有在途消息完成裁决并落盘（`ShutdownTimeout`）；
3. 在途消息在超时内完成的，其 ACK/NACK 正常持久化（不丢失）；
4. 超时仍未完成（处理器卡死）则取消其 context 并**收回为 pending，
   不写入任何确认**——重启后作为未确认重新投递（at-least-once）；
5. 最后 `fsync` 并关闭 WAL。

因此关闭时**不存在静默丢失，也不存在状态悬空**：每条在途消息要么
确认 / 拒绝已落盘，要么明确地留待重启重投，并有 `REVOKE` 日志说明。

---

## 7. 配置项（`mq.Config`）

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `Dir` | `./mq-data` | 持久化目录，WAL 文件为 `Dir/events.wal` |
| `MaxAttempts` | `5` | 最大投递尝试次数（首次为 1）；达到后死信 |
| `RetryDelay` | `100ms` | 默认重试延迟（`RetryDecision` 的 delay 为 0 时使用） |
| `MaxRetryDelay` | `30s` | 重试延迟上限（退避封顶） |
| `RetryBackoff` | `2` | 退避乘数；`<=1` 表示固定 `RetryDelay` |
| `HandlerConcurrency` | `8` | 单个消费者并发处理的不同路由键数量上限 |
| `ShutdownTimeout` | `10s` | 关闭时等待在途消息的最长时间 |
| `Logger` | 输出到 stderr 的默认 logger | 实现 `Printf(format, args...)` 即可自定义 |

---

## 8. 可观测日志

默认日志逐条打印**消息标识、路由键、尝试次数与状态迁移**，例如：

```
PUBLISH id=m_orders_... key=orders seq=3 groups=1
DELIVER id=m_orders_... seq=3 attempt=1 status: pending->inflight consumer=worker-1
NACK    id=m_orders_... seq=3 attempt=1 retryIn=5ms err="transient" status: inflight->pending
ACK     id=m_orders_... seq=3 attempt=2 status: inflight->acked
DEAD    id=... seq=1 attempt=3 reason=retry_exhausted err="boom" status: inflight->dead
REVOKE  id=... seq=1 cause=shutdown-timeout -> pending (at-least-once redelivery)
DECISION-IGNORE id=... status=acked (stale)
```

测试通过自定义 `Logger` 将这些日志转发到 `testing.T.Log`，`go test -v`
即可观察完整迁移轨迹。

---

## 9. 本地验证方法

要求 Go 1.25+（开发环境为 Go 1.26）。

```bash
# 构建与静态检查
go build ./...
go vet ./...

# 全部单元测试（普通模式）
go test ./... -count=1 -v

# race detector（要求必须稳定通过）
go test ./... -race -count=1

# 压力 / 稳定性：重复多轮竞态检测（每一轮都必须通过）
go test ./mq/ -race -count=50

# 只盯暂停 / 运行态 / 扩缩容这批并发用例，更高重复次数
go test ./mq/ -race -count=200 -run 'TestPause|TestKeyStatus|TestConsumerRemoval|TestConsumerScale'
```

### 高负载下的稳定性验证（复现随机失败用）

这批并发用例历史上的随机失败只在“竞态检测 + 高重复次数 + 机器同时有
别的负载”时才偶发（业务已生效但 ACK 尚在落盘窗口，一次性快照读到
`processing`）。可用下面的方式制造 CPU 争抢并限制调度并行度后反复验证：

```bash
# 后台制造 3 个满载 CPU 负载（yes），再以 2 个 P 跑竞态检测，重复 30 轮
(yes >/dev/null & yes >/dev/null & yes >/dev/null &)
GOMAXPROCS=2 go test -race -count=30 ./mq/
pkill -x yes   # 结束后清理后台负载
```

**通过判据**：

- 命令退出码为 0，输出 `ok ...`；任意一轮出现 `FAIL`、`DATA RACE`、
  panic、超时都视为不通过；
- 断言严格性不降低：暂停期间投递计数必须保持 0、到期重试不得放行、
  恢复后按序补齐（逐序号校验）、运行态不得把未 ACK 的消息报成
  `drained`；等待一律轮询终态，**不允许靠加大固定 sleep 或删掉断言
  来“压”住失败**。

### 测试覆盖一览

| 测试 | 覆盖场景 |
| --- | --- |
| `TestBasicDeliveryAndOrdering` | 基本投递、同键严格有序、跨键并行、每消息恰好生效一次 |
| `TestParallelAcrossKeysSerialWithin` | 不同路由键并行、同键串行 |
| `TestDedupPublish` | 相同 DedupKey 的重复发布只生效一次 |
| `TestDuplicateAckOnlyOnce` | 重复 / 乱序 Ack、Nack 只确认一次 |
| `TestRetryThenSuccess` | 临时失败延期重试、尝试次数递增、最终成功只生效一次 |
| `TestRetryExhaustedDeadLetter` | 重试耗尽 → 死信（`retry_exhausted`），不再投递 |
| `TestUnrecoverableDeadLetter` | 不可恢复失败首次即死信（`unrecoverable`） |
| `TestRestartAckedNotRedelivered` | 重启后已确认消息不重投，恢复后仍可正常收发 |
| `TestRestartInflightRedelivered` | 处理中硬崩溃（绕过优雅关闭）后未确认消息重投且不重复 |
| `TestRestartRetryStatePreserved` | 重试状态 / 尝试次数跨重启保留 |
| `TestConsumerScaleOutAndIn` | 消费组扩容、缩容，无双重成功、无丢失 |
| `TestConsumerRemovalStuckHandler` | 缩容时处理器卡死：超时收回在途消息并立即重投，不悬挂、不重复 |
| `TestIndependentGroups` | 多消费组各自独立消费一次 |
| `TestGracefulShutdownInFlight` | 优雅关闭处理好在途消息，ACK 已落盘 |
| `TestShutdownTimeoutForceCancel` | 处理器卡死时关闭不被阻塞，未确认消息重启重投 |
| `TestHeadOfLineOrdering` | 队头阻塞验证严格顺序 |
| `TestPauseBlocksDeliveryOtherKeysUnaffected` | 暂停后该键不投递、他键不受影响；恢复快照与按序补齐 |
| `TestPausedKeyWithDelayedRetryHead` | 队头延期重试 + 暂停叠加：到期不投递、后续不越过；恢复后按序 |
| `TestPausePersistsAcrossRestart` | 暂停状态跨重启保留，显式恢复后按序处理 |
| `TestKeyStatusStates` | 快照各状态（处理中 / 无消费者 / 已清空 / 不存在）与字段正确性 |
| `TestPauseWithConsumerRemoval` | 暂停 + 缩容收回在途叠加：不重投、恢复后按序完成 |
| `TestKeyStatusConcurrentReadsInFlight` | 消息在途时多 goroutine 连续读取：快照帧帧自洽、不误报 drained、读取不打断消费 |
| `TestKeyStatusReadsAcrossPauseAndRetry` | 延期重试→暂停→恢复全程并发读取：状态序列正确、到期重试被暂停压住、恢复按序 |
| `TestKeyStatusReadsAcrossPauseAndScale` | 暂停 + 扩缩容 + 在途收回全程并发读取：收回后 paused 有队头、不投递、恢复补齐 |
| `TestKeyStatusReadsDuringPauseFlapping` | 暂停/恢复高速抖动 + 多键连续读取：帧帧自洽、最终按序恰好一次、drained |
| `TestKeyStatusReadsDuringRetryWindow` | 延期重试窗口内连续读取：HeadNotBefore/HeadSeq/Attempts/Backlog 字段稳定自洽 |
| `TestStressScaleAndFailures` | 600 消息 / 24 键 / 动态扩缩容 / 混合失败：成功 + 死信恰好覆盖全部且无重复 |
| `mq/wal` 包测试 | 追加 / 重放、序号连续、撕裂尾帧截断恢复 |

所有测试在 `-race` 下稳定通过：已在正常负载下重复 50 轮、并在
`GOMAXPROCS=2` 加 3 路满载 CPU 争抢的极端条件下重复 60 轮验证
（暂停 / 运行态批次单独重复 200 轮）。

---

## 10. 设计边界与权衡（明确不做的事）

- 单进程、单数据目录（`flock` 保证）；不是网络多副本队列。
- 同键严格有序意味着**队头阻塞**：一条消息持续重试会延后同键后续消息，
  这是“顺序”语义的有意取舍。
- WAL 为单文件追加，运行期只追加不压缩；内存会裁剪已终态消息，
  但日志文件本身不做周期性 compaction（适合本地验证 / 单机场景）。
- 至少一次投递要求业务幂等；框架不替业务回滚副作用。
