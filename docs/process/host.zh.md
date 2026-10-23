# 进程宿主

[English](host.md) | 中文

`ora-process-host`（`apps/ora-process-host`）拥有 Node 受管程序持久的 Scope 和 Run 意图，并协调每个 Scope 的
[guardian](guardian.zh.md)。它通过本地 IPC 接受 Run、Stop 和 Close 意图；每个 guardian 都独立于宿主和发起请求的 Node 继续运行。
不需要 root、helper、token 或密钥：这是同一用户下的协作式部署。总览见[进程运行时](process-runtime.zh.md)。相关决策是
[rootless guardian 启动引导](https://github.com/ora-space/specs/blob/main/decisions/node/process/recovery/20260917-rootless-guardian-bootstrap-and-reconnect.md)
和[独立日志](https://github.com/ora-space/specs/blob/main/decisions/node/process/persistence/0-independent-journals-and-responsibility-retirement.md)
两份 ADR。

## 运行

```text
ora-process-host create  /absolute/private/process-state /absolute/private/bin/ora-process-guardian
ora-process-host recover /absolute/private/process-state /absolute/private/bin/ora-process-guardian
```

`create` 要求状态目录不存在，`recover` 要求原来完整的日志和锁；失败从不退回到创建。宿主在前台运行。guardian 可执行文件必须位于
受信的绝对路径（release 构建拒绝组或其他用户可写的祖先目录；两种构建都检查 owner、符号链接和 inode 类型）。`SIGTERM`/`SIGINT`
停止的是宿主的协调，而不是工作负载：先关闭 Scope 并观察到关闭，再终止工作。容器或服务管理器按进程组终止，与只停止宿主不同，见
[容器镜像](../operations/container-images.zh.md)。

`ora_process_client::ProcessHost::new(state_dir, expected_uid)` 连接到该目录。`execute(HostOperation)` 支持 `Inspect`、
`CreateScope`、`Start`、`QueryRun`、`Stop`、`Close`、`QueryScope` 和有界的 `Output`。每次交换使用一个新 socket，调用方在重连时
保持原来的 id。Start 被接受不代表执行成功，Stop 被接受不代表清理完成，丢弃客户端或等待也从不取消已接受的操作。

## 状态目录

`HostState` 接受一个绝对路径的专用 `state_dir`，从不是 `HOME` 或业务的 cwd，与 Node 数据分开注入。

- `HostState::create` 要求目录不存在（已存在的空目录也会被拒绝），只创建 `host.lock`、`host.sqlite` 及其 SQLite 附属文件和
  `scopes/`。
- `HostState::recover` 要求已有的锁、数据库和 `scopes/`。缺失的文件、未知的根目录条目、格式错误的身份和不兼容的日志都会失败，
  不重置任何东西。
- 目录只有 owner 可访问，文件是只有一个硬链接的私有普通 inode；符号链接和可写的祖先目录通过 `ora-utils::path` 拒绝。从不修改
  已有的权限。
- 只接受 ext 系列、XFS 和 Btrfs；网络、内存、overlay 和未知的文件系统都会被拒绝。
- 规范的 Scope id 加上 `control.sock` 必须在 Linux 的 socket 路径长度限制之内。过长的路径会被拒绝而不是截短，所以组可写的
  checkout 或 `/tmp` 都不是受支持的父目录。

创建以关闭方式失败：中断的初始化会留下恢复时会被拒绝的半成品目录。永远不要删除锁文件、在被占用时替换目录，或删除记录来绕过
失败。

## 日志

日志（`application_id` `0x4f524148`，`user_version = 6`）检查精确的 schema、完整性和持久化的身份，并启用 WAL 和
`synchronous=FULL`。在返回新事实之前，事务已提交，所在目录的条目也已同步；因此提交之后的文件系统错误可能让调用报告失败、记录
却已被接受，所以调用方应查询原来的身份，而不是推断没有被接受。

- 恢复不等待地获取原来的 `host.lock`，先运行只读的兼容性检查，然后才提交新的宿主 incarnation（一个正的 epoch 加一个实例 id）。
  有竞争是错误，而不是替换任何东西的许可。
- `record_scope_intent` 提交 Scope 原始的意图（或原样返回它）。它不创建目录、guardian 或进程；没有意图却已存在的 Scope 路径会
  阻止登记。
- `start_guardian` 提交一条 `launch_unknown` 记录，创建私有的 Scope 目录，获取它的锁并 exec guardian。之后的任何错误或取消都
  会消耗这次尝试：即使证明了 exec 失败，也从不授权为该 Scope 再启动一次。`guardian_access` 在恢复后还原发现信息，不启动任何
  东西。
- `record_run_intent` 在发送 Start 之前保存不可变的 ScopeId、RunId、精确的 `RunSpec` 和宿主断开策略；整个请求必须放得进一个
  线上帧。环境变量值原样保存（原生的非 UTF-8 值也会保留）。相同的 RunId 和意图返回原来的记录；参数改变则冲突。`run_intents`
  按 RunId 顺序列出所有记录。
- `request_run_stop` 记录强制停止的责任，`request_scope_close` 永久封住准入；两者都在重启后保留，Close 会停止该 Scope 的所有
  Run，但不改写它们的 spec。

已确认启动是持久的正面证据：后续未知观测仍保留 `LaunchFact::Started`。之后的启动事实或直接进程状态若报告 `NotStarted`，
宿主会拒绝矛盾应答并保留原观测，宿主恢复后也一样。这防止对 guardian 缺失 Run 的取消抹去已知启动事实，进而制造停止确认。

旧的精确日志布局（版本 1 到 5）在事务中迁移。已有 Scope 目录的版本 2 日志会在迁移前被拒绝，因为那些 guardian 仍在使用旧的
token 协议：保留一个兼容的旧宿主，或使用新的状态目录，永远不要为了强制升级而删除 Scope。不保证已删除的 token 值会从 SQLite
空闲页或备份中被安全擦除。

## 协调

`HostCoordinator` 把日志和 guardian 客户端组合起来；它的 owner 驱动 `tick()`，已接受的 Start、Stop 和 Close 随后在不依赖发起
连接的情况下推进。每个 Scope 最多有一个进行中的交换，最多 32 个 Scope 同时进行，轮流调度。失败的交换从几百毫秒退避到五秒，再
加上按 Scope 区分的抖动。等待对端时，socket I/O 和启动引导的投递都不持有日志。

新接受的 Close 会唤醒该 Scope，不再等待旧观测的退避。已经进行的交换继续持有责任，完成后再派发关闭；重复 Close 不重置失败退避。
唤醒协调不是关闭证据：调用方仍须等待原 guardian 的 Closed 观测。

本次宿主已对账并持久化 Scope 的 Closed、且每个已接受 Run 都有直接终态与完整清理证据后，该 Scope 不再占用周期交换槽位。
未知或未清理的 Run 继续按原身份对账。这个终态缓存只存在于本次宿主内存中：宿主恢复后先重新绑定原 guardian，
再根据原事实停止重复交换。Scope、Run、关闭意图和输出均保留，关闭后的查询与输出读取仍可使用。

改变的 Run 和 Scope 观测在查询暴露它们之前就已持久化。查询返回持久的 `last_observed` 事实和单独的 `coordination` 状态（恢复后
为 `Pending`，丢失 guardian 时为 `Unavailable`）；历史上的 `Running` 从不证明进程还活着，存储的 PID 或快照也从不成为启动或发
信号的依据。在任何 guardian 尝试之前的关闭在本地完成；否则必须由原来的 guardian 应答。丢弃协调器只停止它的传输任务，不停止
guardian。

## 线上协议

宿主线上协议版本 2 使用有界的 MessagePack 编解码（16 KiB 帧，深度 16）。控制和输出使用分开的 `host.sock` 和 `host-io.sock`
端点，各有 16 个连接槽位和五秒的交换截止时间；输出块最多 4096 字节，所以慢的输出客户端不会占用控制槽位。双方都检查内核 UID，
宿主绑定来自宿主自己的日志，从不来自 Node 提供的 epoch。恢复只替换能识别的、拒绝连接的私有单链接 socket；仍在监听的端点、普通
文件、符号链接和不兼容的日志都会保留。关闭时不删除端点。
