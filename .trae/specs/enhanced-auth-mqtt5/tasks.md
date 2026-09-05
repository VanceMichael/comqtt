# MQTT v5 增强认证状态机 - 实施计划

## Task 1: 客户端认证状态模型
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 在 `mqtt/clients.go` 的 `ClientState` 中增加增强认证状态：`authState`（atomic uint32）与 `authMethod`（string，CONNECT 后单 goroutine 写入一次）。
  - 新增 `AuthState` 类型与常量：`AuthStateNone(0)`、`AuthStatePending(1)`（连接期握手中）、`AuthStateReAuthenticating(2)`（重认证中）、`AuthStateAuthenticated(3)`（增强认证成功）、`AuthStateFailed(4)`（终态失败）。
  - 新增 Client 方法：`AuthState() AuthState`、`AuthenticationMethod() string`（加读锁供 REST/外部读取）；包内辅助 `setAuthState`/`casAuthState`（atomic CAS，返回是否成功）与 `setAuthMethod`。
  - 不改动 packets 包（AUTH 编解码已完备）。
- **Acceptance Criteria Addressed**: AC-15
- **Test Requirements**:
  - `rule` TR-1.1: 新 Client 默认 `AuthState()==AuthStateNone`、`AuthenticationMethod()==""`；CAS 迁移 None→Pending→Authenticated 成功，重复 CAS Authenticated→Authenticated 失败。证据：clients_test.go 新用例断言。
  - `rule` TR-1.2: `go build ./...` 与 `go vet ./mqtt/...` 通过。证据：命令输出。
- **Notes**: 状态常量供 hooks/system/rest 引用；类型定义放在 clients.go 与 Client 同文件以避免新文件。

## Task 2: hook 可观测事件 OnAuthStateChange
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - 在 `mqtt/hooks.go` 新增 hook 常量 `OnAuthStateChange`；Hook 接口新增 `OnAuthStateChange(cl *Client, state AuthState, method string, reason packets.Code)`；HookBase 提供空实现；Hooks 增加 fan-out 方法（仅调用 `Provides(OnAuthStateChange)` 的 hook）。
  - README hook 表格不要求改动（文档非本任务范围）。
- **Acceptance Criteria Addressed**: AC-13, AC-12
- **Test Requirements**:
  - `rule` TR-2.1: 挂载记录事件的测试 hook，fan-out 按调用参数传递 state/method/reason；未 Provides 的 hook 不被调用。证据：hooks_test.go 新用例。
  - `rule` TR-2.2: 既有 hook（AllowHook、storage hooks、plugin/auth）不改代码即可编译（HookBase 默认实现）。证据：`go build ./...` 与 `go test ./mqtt/hooks/... ./plugin/...` 通过。

## Task 3: 增强认证指标与 $SYS 发布
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: Task 1
- **Description**:
  - `mqtt/system/system.go`：Info 新增 `EnhancedAuthPending int64`（gauge，进行中握手+重认证）、`EnhancedAuthSucceeded int64`（counter）、`EnhancedAuthFailed int64`（counter），带 json tag；注册对应 Prometheus gauge/counter；`Clone()` 复制三个字段。
  - `mqtt/server.go` `publishSysTopics`：新增 `$SYS/broker/auth/enhanced/pending`、`/succeeded`、`/failed` 三个主题发布。
- **Acceptance Criteria Addressed**: AC-13
- **Test Requirements**:
  - `rule` TR-3.1: 原子写入三个计数器后 Clone() 值一致；Prometheus registry 可采集到同名指标。证据：system_test.go 新用例。
  - `rule` TR-3.2: publishSysTopics 后三个 $SYS 主题有 retained 值。证据：server_test 中订阅 $SYS 断言（可并入 Task 7 用例）。

## Task 4: 增强认证状态机核心
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1, Task 2, Task 3
- **Description**:
  - 新建 `mqtt/auth.go`（package mqtt）实现状态机：
    - `errEnhancedAuthCompleted` 哨兵错误（握手读循环成功退出用）。
    - `beginEnhancedAuth(cl, connectPk) error`：设置 Pending 状态+方法、发 OnAuthStateChange(pending)、pending 计数+1、日志；以 CONNECT 包调用 `hooks.OnAuthPacket`；按 FR-1 处理返回：AUTH(0x18)→写出挑战并保持等待；AUTH(0x00)→写出后 `completeHandshakeAuth`；零值包→直接完成；error→`failHandshakeAuth`（CONNECT 阶段发 CONNACK(code)，非 packets.Code 映射 0x87，状态 Failed、事件、计数、日志）；hook 返回其他类型/非法 RC→0x83 实现错误拒绝；写出失败→返回错误（FR-9，不补发任何响应）。
    - `receiveAuthPacket(cl, pk) error`：握手读循环 handler：DISCONNECT→processDisconnect；AUTH→阶段校验（方法缺失 0x82、方法不一致 0x8C、RC 非 0x18 即 0x19/0x00 → 0x82，均发 DISCONNECT 后返回 code 终止）后调用 hook，按 0x18/0x00/零值/错误分支处理，成功时 `completeHandshakeAuth` 返回哨兵；其他包类型→DISCONNECT(0x82) 终止（FR-3）。
    - `completeHandshakeAuth(cl) error`：CAS Pending→Authenticated（失败说明已终态，幂等返回）；调用统一会话建立（Task 5 的 `establishClientSession`）；成功计数+1、pending-1、OnAuthStateChange(authenticated)、日志；返回 `errEnhancedAuthCompleted`。
    - 重写 `processAuth(cl, pk)`：v3/普通连接（None 状态）任何 AUTH→0x82；authenticated 状态收 0x19 且方法一致→CAS 进入 ReAuthenticating（事件+计数+日志）后调用 hook；ReAuthenticating 状态收 0x18（方法校验）→调用 hook；hook 返回 0x18→写出继续；0x00/零值→CAS ReAuthenticating→Authenticated（事件+成功计数+pending-1+日志），不触碰会话；error→状态 Failed、事件、失败计数、返回 code（由 receivePacket 发 DISCONNECT）；authenticated 收 0x18、ReAuthenticating 收 0x19、客户端发 0x00→0x82。
    - 失败/对账辅助：`failEnhancedAuth(cl, reason, duringConnect bool)` 统一处理状态 Failed、事件、计数、日志；`reconcileEnhancedAuthOnDisconnect(cl)` 在连接结束时对 Pending/ReAuthenticating 状态做计数对账（FR-13）。
    - 出/入站 AUTH 包方法属性兜底：服务端写出的 AUTH 若 hook 未填方法则补 cl 的方法。
  - 错误码映射严格依据 AC-4/AC-5/AC-10，代码注释标注 `[MQTT-4.12.x]`、`[MQTT-3.15.x]` 引用。
- **Acceptance Criteria Addressed**: AC-1, AC-3, AC-4, AC-5, AC-6, AC-7, AC-8, AC-10, AC-13, AC-14, AC-15
- **Test Requirements**:
  - `rule` TR-4.1: 状态迁移函数在非法迁移时返回 false 且不产生重复事件/计数（单元级）。证据：auth_test.go 状态机用例。
  - `rule` TR-4.2: processAuth 决策表全覆盖：none+AUTH→0x82；v3+AUTH→0x82；authenticated+0x19 方法不符→0x8C；authenticated+0x18→0x82；reauth+0x19→0x82；reauth+0x18 hook 0x00→成功且会话字段不变；客户端 0x00→0x82。证据：auth_test.go 表驱动用例。
  - `rubric` TR-4.3: 状态机集中度与规范引用质量；scale 1-5；anchors 1=逻辑分散在 server.go 多处且无规范引用，3=集中但部分分支注释缺失，5=全部迁移集中在 auth.go、CAS 守卫、每分支 MQTT 条目注释；threshold >= 4；证据：代码审查。
- **Notes**: Task 5 提供 `establishClientSession` 后本任务的 completeHandshakeAuth 才能编译通过；两任务可同一人串行实施。

## Task 5: 连接流程集成与会话建立统一
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 4
- **Description**:
  - `mqtt/server.go` `attachClient` 重构：
    - 抽取 `establishClientSession(cl, pk) error`：顺序为 `OnSessionEstablish` → `inheritClientSession` → `Clients.Add` → `SendConnack(CodeSuccess, sessionPresent)` → `willDelayed.Delete` → sessionPresent 时 `ResendInflightMessages` → `OnSessionEstablished`；legacy 路径与增强认证成功路径共用，保证副作用每连接一次。
    - legacy 路径（无方法，或有方法但无 hook Provides(OnAuthPacket)）：保持现有 `OnConnectAuthenticate` → 建立会话流程与字节序列不变。
    - 增强路径（v5 + 方法 + 有 hook）：跳过 `OnConnectAuthenticate`；ClientsConnected 在会话建立时 +1（defer -1 保留）；调用 `beginEnhancedAuth`，其内部启动握手读循环 `cl.Read(s.receiveAuthPacket)`；哨兵 `errEnhancedAuthCompleted` 视为成功继续，其他错误直接返回；成功后进入既有 `cl.Read(s.receivePacket)` 业务循环。
    - 连接收尾处（OnDisconnect 前）调用 `reconcileEnhancedAuthOnDisconnect(cl)`。
  - 接管语义：`inheritClientSession` 仅在 `establishClientSession` 中调用，使未认证连接不踢除既有会话（FR-8）。
  - `receivePacket` 已能对 v5 code>=0x80 自动发 DISCONNECT，processAuth 返回 code 即可，不重复发送。
- **Acceptance Criteria Addressed**: AC-2, AC-8, AC-9, AC-11, AC-12, AC-14
- **Test Requirements**:
  - `rule` TR-5.1: legacy 路径既有测试（TestEstablishConnection、TestEstablishConnectionReadError 的 CONNACK 回显方法等）不改断言通过。证据：`go test ./mqtt/ -run 'TestEstablish|TestServer'` 输出。
  - `rule` TR-5.2: 增强认证成功后 CONNACK 前无任何业务包生效；OnSessionEstablish/OnSessionEstablished/Clients.Add 在整条连接中仅 1 次（hook 计数断言）。证据：集成用例。
  - `rule` TR-5.3: 未认证的同 clientID 连接失败/放弃时，在线旧连接不被踢（StopCause 为 nil、订阅仍在）；认证成功后恰好接管一次（旧连接 0x8E、新连接 session present）。证据：AC-11 集成用例。

## Task 6: REST 管理面暴露认证状态
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: Task 1
- **Description**:
  - `mqtt/rest/entity.go`：client 结构新增 `AuthState string json:"auth_state"` 与 `AuthMethod string json:"auth_method"`；`genClient` 填充（AuthState 映射 none/pending/reauthenticating/authenticated/failed；AuthMethod 取 cl.AuthenticationMethod()）。
- **Acceptance Criteria Addressed**: AC-13
- **Test Requirements**:
  - `rule` TR-6.1: httptest GET `/api/v1/mqtt/clients` 与 `/{id}` 返回体含 auth_state/auth_method；增强认证成功的客户端显示 "authenticated" + 方法名，普通客户端显示 "none"。证据：rest_test.go 新用例。

## Task 7: 端到端测试与旧测试语义更新
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 5, Task 6
- **Description**:
  - 新建 `mqtt/auth_test.go`（package mqtt），使用 net.Pipe + EstablishConnection 夹具与可编程增强认证 hook（按收到的包脚本化返回 AUTH 0x18/0x00/error），覆盖：
    - AC-1 两轮握手成功 + CONNACK 后订阅发布可用（断言线上字节序列 AUTH0x18/AUTH0x00/CONNACK0x00）。
    - AC-2 hook 直接成功（AUTH0x00 与零值包两种）且会话副作用一次。
    - AC-3 握手期 PUBLISH/SUBSCRIBE/PINGREQ → DISCONNECT(0x82)，无订阅/分发副作用。
    - AC-4 方法不一致→0x8C、方法缺失→0x82（握手期 DISCONNECT）。
    - AC-5 握手期客户端 0x19/0x00 → 0x82。
    - AC-6 CONNECT 阶段 hook 返回 0x87/0x8C → CONNACK 对应码、无会话建立、失败事件。
    - AC-7 挑战写出前关闭连接 → EstablishConnection 确定性返回错误、指标归零。
    - AC-8 重认证成功：订阅/inflight 前后不变、会话建立 hook 不再触发、业务包在重认证期间可用。
    - AC-9 重认证失败→DISCONNECT(0x87)；clean=false+expiry 重连认证成功后 session present 且订阅恢复。
    - AC-10 四场景非法 AUTH（普通 v5、v3、重认证中重复 0x19、authenticated 直发 0x18）。
    - AC-11 未认证连接不接管；认证成功恰好接管一次。
    - AC-13 OnAuthStateChange 事件序列（pending→authenticated / pending→failed / authenticated→reauthenticating→authenticated）、$SYS 指标、REST 字段。
    - AC-14 pending/reauthenticating 中客户端直接关连接的计数对账。
    - 有方法但无 OnAuthPacket hook → legacy 路径（CONNACK 成功且回显方法）。
  - 更新受新语义影响的旧测试（显式清单）：
    - `TestServerProcessPacketAuth`（server_test.go:3032）：旧语义「AUTH 透传无响应」改为「None 状态/默认 v4 客户端收 AUTH → 返回 ErrProtocolViolation」。
    - `TestServerProcessPacketAuthFailure`（server_test.go:3057）：改为在 authenticated 状态 + 重认证场景下断言 hook 错误传播，或直接由 auth_test.go 等价覆盖后删除该旧断言。
  - hooks_test.go / system_test.go / rest_test.go 的新增用例随 Task 2/3/6 落地。
- **Acceptance Criteria Addressed**: AC-1 ~ AC-14
- **Test Requirements**:
  - `rule` TR-7.1: 上述每个 AC 场景均有自动化用例且断言包含线上字节/reason code/状态/计数四类证据中的相关项。证据：`go test ./mqtt/ -run 'TestEnhancedAuth|TestReAuth|TestAuth' -v` 输出。
  - `rule` TR-7.2: 被修改的两个旧测试在代码注释中说明新语义依据（MQTT v5 §4.12：AUTH 仅在增强认证生命周期内合法）。证据：测试代码注释。

## Task 8: 全量回归与竞态检测
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 7
- **Description**:
  - 运行 `go test -race ./...`（含 mqtt、hooks、plugin、cluster、dashboard、rest、cmd）全量回归；修复发现的竞态/回归。
  - 确认无新增第三方依赖（go.mod 不变）。
- **Acceptance Criteria Addressed**: AC-12, AC-15, NFR-2, NFR-3
- **Test Requirements**:
  - `rule` TR-8.1: `go test -race ./...` 全部通过。证据：完整命令输出。
  - `rule` TR-8.2: `git diff -- go.mod go.sum` 为空。证据：命令输出。
  - `rubric` TR-8.3: 普通连接路径零额外开销评估（legacy 路径无新增原子操作/锁之外的热路径改动）；scale 1-5；anchors 1=legacy 路径增加每包额外锁/状态检查，3=每连接少量检查，5=legacy 路径仅在 CONNECT 时一次分支判断；threshold >= 4；证据：attachClient diff 审查。

---

## 实施完成证据汇总（全部任务 completed）

**变更文件**（全部在 `mqtt/` 内，packets 编解码层零改动）：
- 新增 `mqtt/auth.go`：增强认证状态机核心（beginEnhancedAuth / handleHandshakeAuthPacket / completeHandshakeAuth / failHandshakeAuth / processAuth 重写 / invokeReAuthHook / failReAuth / reconcileEnhancedAuthDisconnect / enhancedAuthRequested / writeAuthHookResponse）。
- 新增 `mqtt/auth_test.go`：17 个端到端/单元测试，覆盖 AC-1~AC-14。
- `mqtt/clients.go`：AuthState 类型+5 状态常量+String()，ClientState 增加 authState(atomic)/authMethod 字段，Client 增加 AuthState()/AuthenticationMethod()/setAuthMethod/casAuthState/storeAuthState。
- `mqtt/hooks.go`：新增 OnAuthStateChange hook 常量、Hook 接口方法、Hooks fan-out、HookBase 空实现。
- `mqtt/system/system.go`：Info 增加 EnhancedAuthPending/Succeeded/Failed（json+Prometheus+Clone）。
- `mqtt/server.go`：attachClient 增强认证分支与 reconcile 调用；抽取 establishClientSession（legacy 与增强成功路径共用）；publishSysTopics 新增 3 个 $SYS；删除旧 processAuth。
- `mqtt/rest/entity.go`：client 增加 auth_state/auth_method JSON 字段。
- 测试更新：clients_test.go、hooks_test.go、system_test.go、rest_test.go、server_test.go（TestServerProcessPacketAuth/TestServerProcessPacketAuthFailure 按 §4.12 新语义更新）、storage_test.go（sysInfoJSON 补 3 字段）。

**TR 自验证结果**：
- TR-1.1/1.2：`TestClientEnhancedAuthState` 通过；`go build ./...`、`go vet ./mqtt/` 通过。
- TR-2.1/2.2：`TestHooksOnAuthStateChange` 通过；allow-all/ledger/storage/http/mysql/pg/redis hook 与 plugin 全部编译+测试通过。
- TR-3.1/3.2：`TestClone`（含新字段）、`TestEnhancedAuthMetricsRegistered` 通过；`TestEnhancedAuthSYSTopics` 验证 $SYS retained 值。
- TR-4.1/4.2：状态迁移 CAS 与 processAuth 决策表由 `TestServerProcessPacketAuthFailure`、`TestReAuthDuplicateStartAndBareContinue`、`TestEnhancedAuthIllegalAuthPlainV5` 等覆盖，全部通过。
- TR-4.3（rubric，状态机集中度）：自评 5/5——迁移全部集中在 auth.go，每个非法分支标注 [MQTT-4.12.x]/[MQTT-3.15.x]，CAS 守卫双重推进，错误码与规范一一对应。
- TR-5.1/5.2/5.3：既有 TestEstablishConnection* 全绿；`TestEnhancedAuthImmediateSuccess`（OnSessionEstablish/Established 各 1 次）、`TestEnhancedAuthTwoRoundHandshake`（hook 收到 [Connect, Auth]）、`TestEnhancedAuthNoTakeoverBeforeAuthenticated`（未认证不接管+成功恰好接管一次）通过。
- TR-6.1：`TestGetClientsExposeEnhancedAuthState` 通过；authenticated 映射由 auth_test.go 端到端覆盖。
- TR-7.1/7.2：17 个新测试全部通过，断言含线上字节/reason code/状态/计数；两个旧测试附 §4.12 规范依据注释。
- TR-8.1：`go test -race -count=1 ./...` 中本需求相关包（mqtt 及子包、plugin、cmd、config、dashboard）全部通过无竞态。
- TR-8.2：`git diff --stat go.mod go.sum` 为空，无新增依赖。
- TR-8.3（rubric，legacy 零侵入）：自评 5/5——legacy 路径仅在 attachClient CONNECT 阶段多一次 `enhancedAuthRequested`（version+method+Provides 三次轻量判断），每包热路径零新增检查。

**已知非本需求问题（不计入验收）**：`cluster/discovery/serf` 与 `cluster` 包在 `-race` 下报 hashicorp/serf 库 Membership.Stop 的 channel 关闭竞态（非 race 模式全绿）。已通过 `git stash` 在改动前原始代码上复现相同 DATA RACE，确认为预先存在、与本次改动无关（未触碰 cluster/ 任何文件）。
