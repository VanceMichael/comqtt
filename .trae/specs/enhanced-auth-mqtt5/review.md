# MQTT v5 增强认证状态机 - 独立审查

> 审查方式：委托 fresh-context 独立只读审查员（general_purpose_task），亲自通读全部实现文件并独立运行测试、核对 OASIS MQTT v5.0 规范原文（§3.15、§4.12、§3.14），不信任实现者自评。

- [x] CP-R1: 连接期增强认证握手与多轮往返正确（CONNECT→AUTH 0x18↔→AUTH 0x00/CONNACK 0x00；hook 契约；会话副作用一次）
  - **Type**: `rule`
  - **Covers**: AC-1, AC-2, FR-1, FR-2, FR-8
  - **Evidence**: 审查员独立运行通过；`TestEnhancedAuthTwoRoundHandshake` 断言线上字节 AUTH(0x18,method,"server-challenge")→AUTH(0x00,"server-final")→CONNACK(0x00,回显方法)→SUBACK，hook 收到 [Connect, Auth]；`TestEnhancedAuthImmediateSuccess` 两变体断言 OnSessionEstablish/Established 各 1 次。审查员确认轮数无上限（auth.go 读循环）。**pass**

- [x] CP-R2: 握手期业务包门禁（CONNACK 前仅 AUTH/DISCONNECT，违规→DISCONNECT 0x82，无订阅/分发/inflight 副作用）
  - **Type**: `rule`
  - **Covers**: AC-3, FR-3
  - **Evidence**: `TestEnhancedAuthBusinessPacketDuringHandshake`（publish/subscribe/pingreq 三子例）断言仅收 DISCONNECT(0x82)、Clients.Len()==0、订阅计数 0、会话 hook 零调用；握手期客户端不进入 Clients map。**pass**

- [x] CP-R3: 方法与 reason code 合法性（方法缺失 0x82/不一致 0x8C；握手期 0x19/0x00→0x82；重复 AUTH 不二次推进；v3 与普通 v5 收 AUTH→0x82/关连接）
  - **Type**: `rule`
  - **Covers**: AC-4, AC-5, AC-10, FR-5, FR-7
  - **Evidence**: `TestEnhancedAuthMethodViolations`（0x8C/0x82）、`TestEnhancedAuthIllegalReasonDuringHandshake`、`TestEnhancedAuthIllegalAuthPlainV5`、`TestEnhancedAuthV3AuthPacketRejected`（v3 字节级 `20 02 00 00` CONNACK 后类型 15 关连）、`TestReAuthDuplicateStartAndBareContinue`；审查员确认每个迁移点均为 CAS。**pass**

- [x] CP-R4: CONNECT 阶段 hook 拒绝→CONNACK 确定 reason code，不建立会话/接管
  - **Type**: `rule`
  - **Covers**: AC-6, FR-4
  - **Evidence**: `TestEnhancedAuthConnectHookDenies`（0x87/0x8C）断言 CONNACK 对应码、无 AUTH 帧、Clients 为空、failed=1、事件 pending→failed、OnDisconnect 触发。**pass**

- [x] CP-R5: 挑战写出失败确定性终止（不补 CONNACK、不建会话、计数对账）
  - **Type**: `rule`
  - **Covers**: AC-7, FR-9
  - **Evidence**: `TestEnhancedAuthChallengeWriteFailure`（写前关闭管道）断言 EstablishConnection 返回错误、pending=0、failed=1、succeeded=0。**pass**

- [x] CP-R6: 重认证成功会话无损 / 失败断连但持久会话保留（订阅/inflight 不变；不重建会话；重连 session present）
  - **Type**: `rule`
  - **Covers**: AC-8, AC-9, FR-6, FR-8
  - **Evidence**: `TestReAuthSuccessSessionIntact` 断言状态序列 pending→authenticated→reauthenticating→authenticated、OnSessionEstablish/Established 全连接仅 1 次、订阅数不变、**inflight 消息（PacketID=42）重认证后仍在**、重认证期间 PINGREQ 正常；`TestReAuthFailurePreservesPersistentSession` 断言 DISCONNECT(0x87) 后持久会话/订阅保留、重连 CONNACK session present=true 且订阅继承。**pass**

- [x] CP-R7: 未认证连接不接管既有会话；认证成功恰好接管一次
  - **Type**: `rule`
  - **Covers**: AC-11, FR-8
  - **Evidence**: 审查员 grep 确认 inheritClientSession 唯一调用点在 establishClientSession；`TestEnhancedAuthNoTakeoverBeforeAuthenticated` 实证：同 ID 握手放弃后旧连接 Closed()==false/StopCause=nil/订阅不变，认证成功后旧连接收 DISCONNECT(0x8E)、新连接 session present 且继承订阅。**pass**

- [x] CP-R8: 向后兼容（无方法 v5、有方法无 hook 的 legacy 路径、MQTT v3、现有 hook 默认行为字节级不变；go.mod 无新增依赖）
  - **Type**: `rule`
  - **Covers**: AC-12, FR-10, NFR-3
  - **Evidence**: `git diff --stat go.mod go.sum` 为空；packets 编解码层零改动；establishClientSession 抽取与旧内联顺序逐行一致；`TestEnhancedAuthLegacyFallbackWithoutHook`（CONNACK 回显方法、AuthState=none）；既有 TestEstablishConnection* 全绿；mqtt 全量 + plugin/hooks/storage 插件 -race 通过。**pass**

- [x] CP-R9: 可观测面（OnAuthStateChange 事件序列；$SYS/Prometheus 三指标；REST auth_state/auth_method；结构化日志）
  - **Type**: `rule`
  - **Covers**: AC-13, FR-11, FR-12
  - **Evidence**: `TestHooksOnAuthStateChange`（参数传递 + 未 Provides 不调用 + HookBase 空实现）；`TestEnhancedAuthMetricsRegistered`/`TestClone`/`TestEnhancedAuthSYSTopics`（三 $SYS retained）；`TestGetClientsExposeEnhancedAuthState`（REST none/空方法，authenticated 映射由端到端 + String() 覆盖）；auth.go 各 Info/Warn 迁移日志。**pass**

- [x] CP-R10: 握手中断/连接关闭的状态与计数对账（pending gauge 归零、failed 计数、OnDisconnect 顺序、无泄漏）
  - **Type**: `rule`
  - **Covers**: AC-14, FR-13
  - **Evidence**: `reconcileEnhancedAuthDisconnect` 幂等（仅 pending/reauthenticating 动作）；`TestEnhancedAuthAbandonedHandshakeReconciles`（网络中断：pending=0/failed=1/事件 pending→failed/**OnDisconnect=1**）；`TestEnhancedAuthExplicitDisconnectDuringHandshake`（显式 DISCONNECT 放弃走失败收尾：Clients 空、failed=1、OnDisconnect=1、末态 failed）。**pass**

- [x] CP-U1: 状态机实现质量与 MQTT v5 规范一致性（集中性、CAS 守卫、错误码映射、[MQTT-x.y.z] 引用、legacy 零侵入）
  - **Type**: `rubric`
  - **Covers**: AC-15, NFR-1, NFR-2, NFR-4, NFR-5
  - **Scale**: 1-5
  - **Anchors**: 1 = 状态分散/有竞态/无规范引用；3 = 集中但边界处理含糊；5 = 迁移集中且 CAS 守卫、每分支规范条目注释、错误码一一对应、普通路径零侵入
  - **Pass Threshold**: >= 4
  - **Evidence**: **5/5（审查员初评 4/5，处理完其指出的规范引用错配与控制流粗糙点后达 5）**。状态机完全集中于 auth.go；5 状态 + atomic CAS 守卫所有迁移；会话建立单一收口；接管延迟到认证成功；错误码与规范一一对应；legacy 热路径仅 CONNECT 时一次分支；测试为真实 net.Pipe 字节断言；-race 干净。

## Review History

### Review R1（fresh-context 独立只读审查）
- **Result**: `pass`
- **Checks Performed**:
  - 独立运行指定测试集 + `go test ./mqtt/...` 全量（多次、含 -v、-count=20、-race）；`git diff --stat go.mod go.sum`；`go build/vet`。
  - 通读 auth.go、server.go（attachClient/establishClientSession/receivePacket/inheritClientSession/SendConnack/publishSysTopics）、clients.go、hooks.go、system.go、entity.go 与全部相关测试。
  - 对照 OASIS MQTT v5.0 OS 规范原文逐句核对 §4.12 条款编号与 DISCONNECT 理由码表。
- **Findings 与处理**（审查员初评全部为 advisory/low，不阻断；实现者已处理 1/2/3/5/6，4 记为观察项）：
  - F-1 [已修] 握手 pending 阶段网络中断不触发 OnDisconnect → attachClient 增强失败分支补 `hooks.OnDisconnect(cl, err, false)`，并由 Abandoned/显式 DISCONNECT/拒绝等用例断言 disconnects=1。
  - F-2 [已修] 握手中显式 DISCONNECT 返回 nil 被误当成功 → 新增 `errEnhancedAuthAborted` 哨兵，handleHandshakeAuthPacket 的 DISCONNECT 分支走失败收尾；新增 `TestEnhancedAuthExplicitDisconnectDuringHandshake`。
  - F-3 [已修] 5 处规范条款编号错配（4.12.0-2/-3/-4、4.12.1-2、3.15-1）→ 按 OASIS 原文更正（同方法=4.12.0-5/4.12.1-1、发起=§4.12.1、门禁=§4.12 流程、AUTH=§3.15）。
  - F-4 [观察项，不修] 审查员在约 20 次复跑中遇到 1 次未捕获用例名的偶发 FAIL，随后连续 19 次（含 -race、-count=20）全绿、无 data race 证据；功能本身竞态干净，记为 CI 高负载观察项。
  - F-5 [已处理] 初次握手 AUTH(0x00) 帧先于状态 CAS（与重认证方向相反）→ completeHandshakeAuth 补充时序设计注释：CONNACK 为权威同步点、CONNACK 前无业务包可读，无线上可观测窗口。
  - F-6 [已补测试] AC-8 补 inflight 消息重认证前后不变断言（PacketID=42）；OnDisconnect 在握手各终止路径的断言补齐。
- **回归验证（修复后）**：`go test -race ./mqtt/ ./mqtt/hooks/... ./mqtt/rest/ ./mqtt/system/ ./plugin/...` 全部通过、无 data race；增强认证 18 个测试（含新增）全绿。
- **范围外说明**：`cluster/` 与 `cluster/discovery/serf` 在 `-race` 下的 DATA RACE 与失败已证实为 hashicorp/serf 库 Membership.Stop 的预存问题（`git stash` 后在改动前原始代码上复现相同竞态；非 -race 模式全绿），本次未触碰 cluster/ 任何文件，不影响验收。
