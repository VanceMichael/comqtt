# MQTT v5 增强认证（Enhanced Authentication）状态机 - 产品需求文档

## Overview
- **Summary**: 在 comqtt broker 中把 MQTT v5 增强认证（AUTH 包多轮挑战-响应，含连接期认证与连接中重认证）实现为连接生命周期的一等部分：CONNECT 声明 Authentication Method 后，broker 与认证 hook 之间可往返 AUTH(0x18 Continue Authentication)，仅在明确成功（AUTH 0x00 / CONNACK 0x00）后才放行订阅、发布等业务包；非法阶段、方法不一致、reason code 不合法或挑战写出失败时一次性返回确定的协议错误并结束连接；整个过程不破坏持久会话、订阅与 QoS 交付，且普通 MQTT v5 / MQTT 3 / 现有 hook 默认行为保持不变。
- **Purpose**: 支持 SCRAM-SHA-256、SCRAM-SHA-1、GSSAPI/Kerberos 等需要多轮凭据交换的 MQTT v5 客户端接入 broker，并为运维与插件提供认证进行中/成功/失败的可观测能力。
- **Target Users**: 需要多轮认证接入的 MQTT v5 客户端开发者；实现增强认证 hook 的插件开发者；需要观察认证状态的运维人员（REST 管理面、$SYS、日志）。

## Goals
- CONNECT 携带 Authentication Method 且存在提供 `OnAuthPacket` 的 hook 时，进入增强认证握手流程，支持任意轮数的 AUTH(0x18) 挑战往返。
- 认证明确成功前，连接只能收发 AUTH/DISCONNECT；SUBSCRIBE/PUBLISH/PINGREQ 等业务包被确定性拒绝并关闭连接。
- 支持连接建立后的重认证（re-authentication）：客户端发起 AUTH(0x19)，多轮 0x18 往返，服务端以 AUTH(0x00) 完成；重认证期间既有会话、订阅、inflight/QoS 交付不受影响。
- 所有非法情形（方法缺失/不一致、reason code 与阶段不符、重复/并发 AUTH、v3 发 AUTH、未声明方法却发 AUTH）映射到确定的 reason code（0x82 协议错误 / 0x8C 错误的认证方法 / 0x87 未授权等），且每条连接只结束一次、会话状态只推进一次。
- 认证成功才做会话接管/继承（未认证连接不得踢掉既有持久会话）；认证成功后持久会话、订阅、QoS inflight 重发与现有路径语义一致。
- hook、日志、$SYS 指标、REST 管理面可区分「进行中 / 成功 / 失败」三种状态。
- 无 Authentication Method 的普通 MQTT v5、MQTT 3.1/3.1.1、以及现有 hook（allow-all、auth-ledger、存储 hook、http/mysql/pg/redis 认证插件）默认行为完全不变。

## Non-Goals
- 不内置具体 SASL 机制实现（SCRAM/Kerberos 等由外部 hook 提供；broker 只提供状态机与传输契约）。
- 不实现服务端主动发起的重认证（客户端发起 0x19 才触发）。
- 不改动 dashboard HTML 页面（管理观察面以 REST API 字段 + $SYS 指标为准）。
- 不改动集群消息复制、存储 hook 的持久化格式。
- 不修改 MQTT v3 协议行为（v3 无 AUTH 包，类型 15 按协议违规处理）。

## Background & Context
- 代码库为 comqtt v2.4.0（mochi-mqtt 的 Go fork）。包编解码层已完整支持 AUTH 包：`packets.AuthEncode/AuthDecode/AuthValidate`（[packets.go](file:///Users/vance/project/mineProject/swe/090501/project-03/mqtt/packets/packets.go#L1137-L1181)），属性 21/22（AuthenticationMethod/Data）可在 CONNECT/CONNACK/AUTH 上编解码，reason code 0x18/0x19/0x8C 常量已存在（[codes.go](file:///Users/vance/project/mineProject/swe/090501/project-03/mqtt/packets/codes.go#L40-L105)）。
- 当前 `processAuth`（[server.go](file:///Users/vance/project/mineProject/swe/090501/project-03/mqtt/server.go#L1528-L1536)）仅把 AUTH 包透传给 `OnAuthPacket` hook 并丢弃返回包，无状态机、无阶段校验、无业务包门禁。
- 连接建立流程在 `attachClient`（[server.go](file:///Users/vance/project/mineProject/swe/090501/project-03/mqtt/server.go#L364-L445)）：读 CONNECT → `ParseConnect` → 黑名单 → `validateConnect` → `OnConnect` → `OnConnectAuthenticate`（bool）→ `OnSessionEstablish` → `inheritClientSession`（含同 ID 旧连接踢除）→ `Clients.Add` → `SendConnack` → inflight 重发 → `OnSessionEstablished` → `cl.Read(s.receivePacket)` 进入业务循环。
- hook 总线已存在 `OnAuthPacket(cl, pk) (packets.Packet, error)`（[hooks.go](file:///Users/vance/project/mineProject/swe/090501/project-03/mqtt/hooks.go#L291-L307)），HookBase 提供默认透传实现；`Provides(OnAuthPacket)` 可探测 hook 是否参与认证包处理。
- `SendConnack` 已在成功且 CONNECT 带 Authentication Method 时回显方法（[server.go](file:///Users/vance/project/mineProject/swe/090501/project-03/mqtt/server.go#L590-L592)）；现有测试夹具 `TConnectMqtt5` 携带 `SHA-1` 方法但无认证 hook，依赖此回显行为（TestEstablishConnectionReadError）。
- 单连接包处理是单 goroutine 串行的（`Client.Read` 循环），状态迁移只需在该 goroutine 上保证一次；会话建立副作用（`OnSessionEstablish/Established`、`Clients.Add`、inflight 重发）当前只在 `attachClient` 发生一次。
- 存储 hook（badger/bolt/redis）通过 `OnSessionEstablished` 持久化客户端；REST 管理面通过 `rest/entity.go` 的 `genClient` 暴露客户端信息；$SYS 指标在 `system.Info` + `publishSysTopics`。

## 功能需求（Functional Requirements）

- **FR-1 连接期增强认证握手**：MQTT v5 CONNECT 携带非空 Authentication Method 且至少一个 hook `Provides(OnAuthPacket)` 时，broker 进入增强认证握手：先以 CONNECT 包调用 `OnAuthPacket`；hook 返回 AUTH(0x18) 时向客户端写出挑战包并继续等待 AUTH；hook 返回 AUTH(0x00) 时写出成功 AUTH 并完成会话建立；hook 返回零值包（FixedHeader.Type=0）时不写 AUTH 直接完成；hook 返回 error（packets.Code 或普通 error）时以确定 reason code 拒绝连接。
- **FR-2 多轮往返**：握手期间客户端每发 AUTH(0x18)，broker 校验后调用 `OnAuthPacket`，hook 可再次返回 0x18（继续）或 0x00（完成）；轮数不限，直到成功或失败。
- **FR-3 业务包门禁**：握手未完成期间（CONNACK 成功发出前），除 AUTH 与 DISCONNECT 外的任何入站包（PUBLISH/SUBSCRIBE/UNSUBSCRIBE/PINGREQ/PUB* 等）一律导致 DISCONNECT(0x82 协议错误) 并结束连接，不产生任何订阅、消息分发或会话副作用。
- **FR-4 握手期拒绝语义**：CONNECT 阶段 hook 拒绝（或无 hook 处理方法时的 legacy 路径之外的失败）通过 CONNACK 返回 reason code（0x87/0x8C/0x80 等）后关闭连接；CONNECT 之后 AUTH 阶段的结构性违规（方法不一致、阶段错误、reason code 非法、非 AUTH 包）通过 DISCONNECT(reason) 关闭连接。
- **FR-5 方法与 reason code 合法性**：AUTH 包必须携带与 CONNECT 相同的 Authentication Method（缺失=0x82 协议错误，不一致=0x8C 错误的认证方法）；握手期客户端→服务端 AUTH 只允许 0x18（0x00 为服务端→客户端方向、0x19 仅用于重认证，违反=0x82）。
- **FR-6 重认证**：已通过增强认证的连接（状态 authenticated）收到 AUTH(0x19) 且方法一致时进入重认证；随后 0x18 往返同 FR-2；hook 返回 AUTH(0x00) 写出后状态回到 authenticated，不重建会话、不触碰订阅/inflight；hook 失败或违规时 DISCONNECT(reason) 结束连接。
- **FR-7 重认证门禁与并发**：未在增强认证中的连接（无方法/普通 v5/v3）收到任何 AUTH → 0x82；v3 连接收到类型 15 → 协议违规断连；重认证进行中再次收到 0x19 或在非重认证状态收到 0x18 → 0x82；状态迁移用 CAS 保证「重复/并发 AUTH」不会把会话或认证状态推进两次。重认证进行中允许普通业务包继续处理（既有认证仍然有效）。
- **FR-8 会话与 QoS 连续性**：认证成功路径复用统一的会话建立逻辑（OnSessionEstablish → inheritClientSession → Clients.Add → CONNACK(0) → willDelayed 清理 → inflight 重发 → OnSessionEstablished），且整条链路每连接只发生一次；会话接管（同 ClientID 踢旧连接）延迟到认证成功后执行，认证失败/中途放弃的连接不得删除或踢掉既有持久会话；重认证全程不调用会话建立/退订逻辑。
- **FR-9 挑战写出失败处理**：服务端写 AUTH 挑战/成功包失败时（连接已断），立即确定性终止握手与连接，不补发 CONNACK、不做会话建立、不残留计数器/状态。
- **FR-10 向后兼容**：CONNECT 无 Authentication Method 时走既有 `OnConnectAuthenticate` 路径，行为与字节序列不变；CONNECT 有方法但无 hook `Provides(OnAuthPacket)` 时保持既有 legacy 行为（普通认证路径 + CONNACK 回显方法）；MQTT 3.1/3.1.1 行为不变；HookBase 默认实现使现有外部 hook 无需改动即可编译运行。
- **FR-11 可观测性 - hook**：新增 hook 事件 `OnAuthStateChange(cl, state, method, reason)`，在进入进行中（握手 pending / 重认证 reauthenticating）、成功（authenticated）、失败（failed）时触发；Client 暴露 `AuthState()` 与 `AuthenticationMethod()` 访问器；状态常量 none/pending/reauthenticating/authenticated/failed。
- **FR-12 可观测性 - 日志/指标/管理面**：每次状态迁移输出结构化日志（client id、method、state、reason code、阶段）；`system.Info` 新增增强认证进行中 gauge 与成功/失败计数器，发布到 $SYS 并注册 Prometheus；REST `/api/v1/mqtt/clients` 与 `/{id}` 返回 `auth_state`、`auth_method` 字段。
- **FR-13 连接结束对账**：连接在 pending/reauthenticating 状态下断开（客户端放弃/网络断开）时，进行中 gauge 与失败计数被正确对账，OnDisconnect 与失败状态事件按序触发，无 goroutine/计数器泄漏。

## 非功能需求（Non-Functional Requirements）

- **NFR-1 协议合规**：实现遵循 MQTT v5.0 §3.15（AUTH 包）、§4.12（Enhanced Authentication，含重认证）、§3.1.4/§3.2.2（CONNECT/CONNACK 理由码）、§3.14（DISCONNECT 理由码）；每条规则在代码注释中标注 `[MQTT-x.x.x-x]` 引用。
- **NFR-2 串行安全**：单连接认证状态迁移无数据竞争（atomic CAS + 单 goroutine 处理）；`go test -race ./...` 通过。
- **NFR-3 回归安全**：既有测试套件全部通过（为适配新协议语义而必须调整的既有 AUTH 测试除外，调整点需在 tasks 中显式列出并说明新语义依据）。
- **NFR-4 性能**：普通连接（无方法）路径不增加额外锁/原子操作以外的开销；增强认证路径每轮仅一次 hook 调用与一次包写出。
- **NFR-5 可维护性**：认证状态机集中在独立文件，server.go 中连接流程改动最小化，hook 接口新增方法必须在 HookBase 提供默认实现以保持向后兼容。

## Constraints
- **Technical**: Go 模块 `github.com/wind-c/comqtt/v2`；不得引入新的第三方依赖；包编解码层（packets 包）原则上不改动（AUTH 编解码已完备），仅在 server/clients/hooks/system/rest 层扩展。
- **Business**: 现有部署（allow-all/ledger/http/mysql/pg/redis auth、badger/bolt/redis storage）升级后行为不变。
- **Dependencies**: 无新增依赖；测试使用现有 `net.Pipe` + `EstablishConnection` 测试夹具与 testify。

## Assumptions
- 增强认证 hook 通过既有 `OnAuthPacket` 接口参与：入参为 CONNECT 或 AUTH 包，返回待写出的 AUTH 包（0x18/0x00）或零值包表示无挑战直接成功，返回 error 表示失败；error 为 `packets.Code` 时使用其 reason code，否则映射为 0x87 Not Authorized（CONNECT 阶段）/ 对应 DISCONNECT code。
- 「方法存在但无 OnAuthPacket hook」维持 legacy 行为（而非规范严格的 0x8C 拒绝），以保证现有部署与测试夹具兼容；增强认证为 hook 显式 opt-in。
- 重认证期间业务包继续处理（MQTT v5 §4.12.2：旧认证在重认证完成前持续有效）；仅连接期握手（CONNACK 前）实施业务包门禁。
- 会话接管延迟到认证成功后执行（相对 mochi-mqtt 原行为更安全，且满足「认证失败不能丢失原有持久会话」）。

## Acceptance Criteria

### AC-1: 连接期多轮增强认证成功
- **Type**: `rule`
- **Given**: broker 挂载一个提供 `OnAuthPacket` 的测试 hook，模拟两轮挑战（CONNECT→AUTH 0x18；AUTH 0x18→AUTH 0x00）
- **When**: 客户端经 net.Pipe 发送带 Authentication Method 的 CONNECT，随后依次回复 hook 要求的 AUTH(0x18)
- **Then**: 服务端先写出 AUTH(0x18, 同方法, 挑战数据)，再写出 AUTH(0x00, 同方法, 完成数据)，随后写出 CONNACK(0x00, 回显方法)；CONNACK 后客户端 SUBSCRIBE/PUBLISH 正常工作
- **Pass Condition**: 线上字节序列依次为 AUTH(0x18)、AUTH(0x00)、CONNACK(0x00) 且后续 SUBACK 成功；hook 收到的包序列为 CONNECT、AUTH(0x18)
- **Evidence**: `go test -run TestEnhancedAuth` 中握手成功用例的断言与抓包字节

### AC-2: CONNECT 后 hook 直接成功
- **Type**: `rule`
- **Given**: hook 对 CONNECT 直接返回 AUTH(0x00)（或零值包）
- **When**: 客户端发送带方法的 CONNECT
- **Then**: （返回 AUTH(0x00) 时）写出 AUTH(0x00) 后 CONNACK(0x00)；（零值包时）直接 CONNACK(0x00)；会话建立副作用恰好一次
- **Pass Condition**: OnSessionEstablish/OnSessionEstablished 各被调用 1 次，Clients.Add 1 次，ClientsConnected 计数平衡
- **Evidence**: 测试 hook 调用计数 + CONNACK 字节断言

### AC-3: 握手期业务包被拒绝
- **Type**: `rule`
- **Given**: 增强认证握手进行中（服务端已发 AUTH 0x18 挑战，客户端尚未完成）
- **When**: 客户端发送 PUBLISH/SUBSCRIBE/PINGREQ 中任一包
- **Then**: 服务端写出 DISCONNECT(0x82) 并关闭连接；无订阅被创建、无消息被分发、无 inflight 变化
- **Pass Condition**: 线上仅收到 DISCONNECT(0x82)；`s.Topics` 无该客户端订阅；EstablishConnection 返回错误
- **Evidence**: 握手期发送 PUBLISH/SUBSCRIBE/PINGREQ 三个用例的字节与状态断言

### AC-4: 握手期认证方法不一致/缺失
- **Type**: `rule`
- **Given**: 增强认证握手中（CONNECT 方法为 "SCRAM-X"）
- **When**: 客户端发送 AUTH 携带不同方法（或不携带方法属性）
- **Then**: 方法不一致 → DISCONNECT(0x8C) 并关连接；方法缺失 → DISCONNECT(0x82) 并关连接
- **Pass Condition**: 返回 reason code 分别为 0x8C/0x82，连接关闭，失败状态事件各 1 次
- **Evidence**: 对应用例断言

### AC-5: 握手期非法 reason code
- **Type**: `rule`
- **Given**: 增强认证握手中
- **When**: 客户端发送 AUTH(0x19)（重认证码出现在握手期）或 AUTH(0x00)（客户端发服务端方向码）
- **Then**: DISCONNECT(0x82) 并关闭连接
- **Pass Condition**: 两种情形均返回 0x82 且连接结束
- **Evidence**: 对应用例断言

### AC-6: CONNECT 阶段 hook 拒绝
- **Type**: `rule`
- **Given**: hook 对 CONNECT 返回 packets.Code 错误（如 ErrNotAuthorized 0x87 / ErrBadAuthenticationMethod 0x8C）
- **When**: 客户端发送带方法 CONNECT
- **Then**: 服务端写出 CONNACK(对应 code) 并关闭连接；不发生会话建立/接管；OnAuthStateChange(failed) 触发
- **Pass Condition**: 线上为 CONNACK(0x87 或 0x8C) 且无 AUTH 包；s.Clients 中无该客户端；失败计数 +1
- **Evidence**: 对应用例断言

### AC-7: 挑战写出失败确定性结束
- **Type**: `rule`
- **Given**: hook 返回 AUTH(0x18) 挑战，但底层连接在写出前/中已关闭
- **When**: broker 尝试写出挑战
- **Then**: attachClient 返回错误，连接结束；不写出 CONNACK、不建立会话、进行中 gauge 被对账为 0
- **Evidence**: 写失败用例断言 EstablishConnection 返回 error 且指标归零

### AC-8: 重认证成功且会话无损
- **Type**: `rule`
- **Given**: 客户端已通过增强认证建立连接，已有订阅与 inflight 消息
- **When**: 客户端发送 AUTH(0x19, 同方法) → 服务端 AUTH(0x18) → 客户端 AUTH(0x18) → 服务端 AUTH(0x00)
- **Then**: 重认证完成；订阅仍存在、inflight 不被清空、重认证期间与之后 PUBLISH/SUBSCRIBE 正常；OnSessionEstablish/Established 不再被调用
- **Pass Condition**: 状态迁移 pending 计数回 0、成功计数 +1；`cl.State.Subscriptions.Len()` 与 inflight 在重认证前后不变
- **Evidence**: 重认证用例断言

### AC-9: 重认证失败断连但持久会话保留
- **Type**: `rule`
- **Given**: 客户端以 clean start=false、Session Expiry Interval>0 通过增强认证连接并有订阅
- **When**: 重认证中 hook 返回 0x87
- **Then**: 服务端写出 DISCONNECT(0x87) 并关闭连接；会话按持久会话保留（不退订、不删客户端会话记录）；该客户端重连（再走增强认证成功）后 session present=true、订阅恢复
- **Pass Condition**: DISCONNECT(0x87) 字节；重连后 CONNACK session present=1 且订阅仍在
- **Evidence**: 对应用例断言

### AC-10: 非法/重复 AUTH 被拒且状态不二次推进
- **Type**: `rule`
- **Given**: 多种非法场景
- **When**: (a) 普通 v5 连接（CONNECT 无方法）发 AUTH；(b) v3 连接发类型 15；(c) 重认证进行中再发 AUTH(0x19)；(d) authenticated 状态直接发 AUTH(0x18)（无 0x19 前置）
- **Then**: 均返回确定协议错误（0x82，v3 直接关连接）并结束/不推进状态；OnAuthStateChange 序列中成功/失败状态不重复出现
- **Pass Condition**: 四个子场景断言 reason code 与状态事件序列
- **Evidence**: 对应用例断言

### AC-11: 未认证连接不得接管既有会话
- **Type**: `rule`
- **Given**: clientID "x" 的持久会话已在线（有订阅）
- **When**: 另一连接以相同 clientID + 认证方法发起 CONNECT 但认证失败/中途放弃（关闭连接）
- **Then**: 既有连接不被踢、会话与订阅保留；随后第三个连接以相同 clientID 认证成功时，恰好发生一次接管（旧连接收到 0x8E），新连接 session present=true、订阅/inflight 继承
- **Pass Condition**: 失败尝试期间旧连接仍在线；成功后旧连接 StopCause=ErrSessionTakenOver，新连接继承订阅
- **Evidence**: 对应用例断言

### AC-12: 向后兼容 - 普通 v5 / v3 / 默认 hook
- **Type**: `rule`
- **Given**: 既有测试套件与默认 hook（AllowHook/auth-ledger）
- **When**: 无方法 CONNECT（v5/v3）、有方法但无 OnAuthPacket hook 的 CONNECT、存储 hook 持久化流程
- **Then**: 全部既有行为与字节序列不变（既有测试不改断言通过；因新语义必须调整的旧 AUTH 单测除外，调整清单在 tasks 中列出）
- **Pass Condition**: `go test ./...` 全绿（除 tasks 中显式列出的语义更新测试）
- **Evidence**: 全量测试输出

### AC-13: 可观测面区分三种状态
- **Type**: `rule`
- **When**: 增强认证握手进行中、成功、失败三种时刻
- **Then**: (a) 记录事件的测试 hook 按序收到 pending→authenticated 或 pending→failed 的 OnAuthStateChange；(b) `system.Info` 的进行中/成功/失败计数正确迁移并发布到 $SYS；(c) REST clients 接口返回 `auth_state`（none/pending/reauthenticating/authenticated/failed）与 `auth_method`
- **Pass Condition**: hook 事件序列、指标值、REST JSON 字段三处断言均符合
- **Evidence**: hook 记录 + $SYS 订阅/REST httptest 用例

### AC-14: 连接中途断开对账
- **Type**: `rule`
- **Given**: 握手 pending 或重认证 reauthenticating 状态
- **When**: 客户端直接关闭连接（不发 DISCONNECT）
- **Then**: 进行中 gauge 减回、失败计数增加、OnDisconnect 触发、连接 goroutine 退出无泄漏
- **Pass Condition**: 指标对账正确且 EstablishConnection 返回
- **Evidence**: 对应用例断言

### AC-15: 实现质量与规范一致性
- **Type**: `rubric`
- **Dimension**: 状态机实现与 MQTT v5 规范的契合度、代码可维护性
- **Scale**: 1-5
- **Anchors**: 1 = 状态散落多处、有竞态/双重推进风险、无规范引用；3 = 状态机集中但部分边界（重复 AUTH、写失败、v3）处理含糊；5 = 状态迁移集中且 CAS 守卫、每个分支标注 MQTT 规范条目、错误码与规范一一对应、普通路径零侵入
- **Pass Threshold**: >= 4
- **Evidence**: 独立审查对 server/clients/hooks 改动的代码审查结论

## Open Questions
- 无（关键设计决策已在 Assumptions 中确定：hook 契约、opt-in 激活方式、重认证期间业务包放行、接管延迟到认证成功）。
