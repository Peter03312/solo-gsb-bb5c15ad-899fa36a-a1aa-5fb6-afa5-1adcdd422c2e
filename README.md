# 样本交接链核验服务（Chain of Custody）

面向环境检测机构的样本保管链核验服务：登记样本采集信息，接收**移交 / 签收 / 封签检查 / 实验室入库**四类带唯一事件号的事件（支持同一交接车次多份样本的**批量移交**：全局批次号、统一移交时间与交出/接收人、每份样本各自的事件号与移交单号，整批原子提交），自动按发生时间重算证据链，输出**当前保管人**与 **有效（valid）/ 待补证（pending）/ 无效（invalid）**结论及原因；核定入库前支持对当前有效事件提交**作废 / 替换纠错**（永久留痕、仅有效版本参与核验）；仅有效链可核定入库，核定后历史冻结。

每次实际提交（登记、单事件上报、车次批量移交、作废/替换纠错、入库核定）都会在**同一写事务**内为每个受影响样本持久化一条**核验结论变更记录**（全局递增游标、样本编号、操作类型及业务编号、提交后结论/原因/当前保管人/核定信息），供实验室对接系统按游标**增量获取**结论变更；旧库升级时为现存样本各补一条当前状态**基线**，不伪造历史操作。

技术栈：**Python 3.11 + FastAPI + SQLite（标准库 sqlite3）**，无外部服务依赖。

---

## 一、核验规则

| 场景 | 判定 |
|---|---|
| 样本登记 | 记录采集时间、采集人（=初始保管人）、封签号；样本编号唯一 |
| 移交（transfer） | 必须由**当前保管人**发起；**移交至签收期间不得再次移交**，否则无效 |
| 签收（receive） | 必须同时满足：匹配在途**移交单号**、**接收人**与移交单一致、**封签号**与登记一致、签收时间**晚于**移交时间 |
| 重复签收 | 同一移交单被签收两次 → 无效（无效事件不可被后续补录抵消） |
| 封签检查（seal_check） | 检查封签号与登记不符，或封签破损（`seal_intact=false`）→ 无效 |
| 缺签收 | 移交后没有合格签收 → **待补证** |
| 缺入库 | 没有实验室入库记录，或入库缺失前序移交-签收链 → **待补证** |
| 保管人断链 | 移交人不是当前保管人 / 签收无对应在途移交 → **无效** |
| 结论优先级 | **无效优先**：同时存在无效原因与待补证事项时，结论为无效（原因中附注待补证事项） |
| 乱序补录 | 事件可任意顺序上报，每次查询/核定都按 `occurred_at`（发生时间，非录入时间）整体重算 |
| 事件幂等 | 同一 `event_id` + 同内容（SHA-256 指纹）重试 → 幂等返回 HTTP 200；同 `event_id` 异内容 → HTTP 409 冲突（核定后亦保持此兼容行为） |
| 核定前纠错 | 核定前可按样本编号 + 当前有效事件号提交**作废（void）/替换（replace）**，附全局唯一纠错号、操作人、原因；替换须给完整新事件内容与**未占用**的新事件号。原事件与纠错关系永久留痕，核验仅纳入有效版本并按发生时间重算结论/原因/当前保管人；被替换记录可经链查询追溯。纠错**不得跨样本**；同纠错号同内容重试幂等（200）、异内容冲突（409）；并发针对同一有效版本仅一次成功，失败不改链。核定入库后拒绝纠错 |
| 纠错与核定并发 | 写事务以 `BEGIN IMMEDIATE` 串行：先提交者生效，后到者看到其已提交结果——作废先提交则核定基于最终有效链（非 valid 则 422），核定先提交则纠错被 `history_locked` 拒绝 |
| 入库核定 | 仅 `valid` 可核定；核定后该样本登记与事件全部**拒绝改写**（HTTP 409），重复核定幂等 |
| 车次批量移交 | 同一交接车次可含多份样本：提交全局唯一**批次号**、统一移交时间、交出人、接收人，及每份样本的编号/事件号/移交单号，各份作为既有 transfer 事件**一次原子提交** |
| 批量失败回滚 | 批内样本编号或事件号重复（422）、任一样本不存在或已核定、任一事件号已占用 → **整批失败**，不留下任何事件或批次 |
| 批量幂等/冲突 | 同批次号同内容重试幂等（200，**样本已核定亦可重试**）；同批次号异内容 409 `batch_conflict`（逐份排列顺序不同仍视为同内容）；批次内事件经既有单事件接口同号同内容重试仍 200、异内容仍 409 |
| 批量归属追溯 | 批次查询返回逐份样本的**当前核验结论**与**原事件是否仍有效**；后续作废/替换纠错或核定只改变有效链与查询结论，各份原事件的**批次原始归属永久保留**（证据链时间线的 `batch_id` 可追溯），替换产生的新事件不属于原批次 |
| 批量并发 | 批量提交与核定、批量提交之间均以 `BEGIN IMMEDIATE` 串行，按**已提交顺序**判定：先到批次落库则核定基于批次后最终链，核定先落库则批次因含已核定样本整批失败；两批次抢同一事件号时落败批次无残留 |
| 变更记录 | 上述每次**实际提交**在同一写事务内为每个受影响样本写一条变更记录：全局递增游标、样本编号、操作类型与业务编号、**提交后**结论/原因/当前保管人/核定信息。即使结论类别未变，只要有效链或核定信息变化也记录；迟到事件按发生时间**重算后再记结果** |
| 不产生记录 | 同内容幂等重试（登记/事件/纠错/批次）、重复核定、失败请求（4xx/回滚）均**不生成**变更记录；整批失败零记录 |
| 游标顺序 | 游标为自增主键，写事务 `BEGIN IMMEDIATE` 串行提交：游标顺序即事务提交可见顺序；同批多份记录与批次同事务、游标连续 |
| 旧库升级 | 对含现存样本但无变更记录的旧库，启动迁移时为每条样本补一条 `baseline` 当前状态基线（已核定者带核定信息），不伪造登记/事件/核定历史；升级幂等，再次启动不重复写 |

> 说明：任何事件发生时间早于样本采集时间，直接记为无效（采集前不可能发生保管动作）。

---

## 二、一键启动

### 方式 A：Docker Compose（推荐，自动初始化依赖、建表、写入演示数据）

```bash
docker compose up -d --build
docker compose logs -f          # 看到 uvicorn 启动日志即可
```

- 首次启动自动：安装 Python 依赖（镜像构建阶段）→ 建表 → 写入 3 条演示样本（有效 / 待补证 / 无效）。
- SQLite 数据持久化在宿主机 `./data/chain.db`，容器重建不丢数据；已有数据时演示数据自动跳过。
- 不需要演示数据：把 `docker-compose.yml` 中 `SEED_ON_START` 改为 `"false"`（只建空库）。

停止 / 清理：

```bash
docker compose down             # 停止（保留数据）
docker compose down && rm -f data/chain.db*   # 停止并清空数据
```

### 方式 B：本地 Python 运行

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python -m app.seed              # 可选：初始化演示数据（幂等，可重复执行）
python -m uvicorn app.main:app --host 0.0.0.0 --port 8080
```

---

## 三、服务地址与端口

| 用途 | 地址 |
|---|---|
| 服务根地址 | http://localhost:8080/ |
| 健康检查 | http://localhost:8080/health |
| Swagger 交互文档（可直接调试） | http://localhost:8080/docs |
| OpenAPI 规范 | http://localhost:8080/openapi.json |
| 业务 API 前缀 | `/api/v1` |

快速验证：

```bash
curl http://localhost:8080/health
curl http://localhost:8080/api/v1/samples        # 查看 3 条演示样本的结论
```

演示样本：

| 样本编号 | 结论 | 说明 |
|---|---|---|
| `DEMO-VALID-001` | valid | 移交→签收→封签检查→入库完整（事件故意乱序写入，演示自动重算） |
| `DEMO-PENDING-002` | pending | 已移交未签收、未入库 |
| `DEMO-INVALID-003` | invalid | 封签检查封签号不符且破损（无效优先于入库事实） |

---

## 四、配置项（环境变量）

| 变量 | 默认值 | 说明 |
|---|---|---|
| `HOST` | `0.0.0.0` | 监听地址 |
| `PORT` | `8080` | 监听端口 |
| `DB_PATH` | Docker 内 `/data/chain.db`；本地 `./data/chain.db` | SQLite 文件路径 |
| `DATA_DIR` | Docker 内 `/data`；本地 `./data` | 数据目录（仅在未显式设置 `DB_PATH` 时用于拼默认路径） |
| `SEED_ON_START` | `true` | 启动时空库是否自动写入演示数据 |
| `LOCK_AFTER_APPROVAL` | `true` | 核定入库后是否冻结历史（按业务要求应始终为 true） |
| `DB_TIMEOUT` | `10` | SQLite 写锁等待秒数 |

---

## 五、HTTP API

所有时间均使用 ISO 8601（建议 UTC，如 `2026-09-30T02:00:00Z`；带时区偏移亦可，内部统一转 UTC）。

### 5.1 样本登记

`POST /api/v1/samples`

```json
{
  "sample_code": "S-001",
  "collected_at": "2026-09-30T01:00:00Z",
  "collector": "王采集",
  "seal_no": "SEAL-001"
}
```

- `201` 新建；同编号同内容重试 `200`（`idempotent: true`）；同编号异内容 `409 sample_conflict`。
- 响应中 `evaluation` 即时给出当前核验结论。

### 5.2 事件上报（统一入口）

`POST /api/v1/events`

事件类型与各自必填字段：

| event_type | 必填字段 | 含义 |
|---|---|---|
| `transfer` | `transfer_no`、`from_custodian`、`to_custodian` | 移交单号、交出方（须当前保管人）、接收方 |
| `receive` | `transfer_no`、`receiver`、`seal_no` | 匹配单号、签收人（须等于接收方）、签收封签号 |
| `seal_check` | `checked_seal_no`（`seal_intact` 默认 true） | 检查读取的封签号、是否完好 |
| `storage` | 无（建议 `custodian`、`storage_location`） | 实验室入库，`custodian` 默认“实验室” |

公共字段：`event_id`（全局唯一）、`sample_code`、`occurred_at`、可选 `note`（备注参与幂等指纹）。

```bash
# 移交
curl -X POST http://localhost:8080/api/v1/events -H 'Content-Type: application/json' -d '{
  "event_id":"EVT-T1","event_type":"transfer","sample_code":"S-001",
  "occurred_at":"2026-09-30T02:00:00Z",
  "transfer_no":"TR-1","from_custodian":"王采集","to_custodian":"李运输"}'

# 签收（单号/接收人/封签号三匹配，且晚于移交）
curl -X POST http://localhost:8080/api/v1/events -H 'Content-Type: application/json' -d '{
  "event_id":"EVT-R1","event_type":"receive","sample_code":"S-001",
  "occurred_at":"2026-09-30T04:00:00Z",
  "transfer_no":"TR-1","receiver":"李运输","seal_no":"SEAL-001"}'

# 封签检查
curl -X POST http://localhost:8080/api/v1/events -H 'Content-Type: application/json' -d '{
  "event_id":"EVT-C1","event_type":"seal_check","sample_code":"S-001",
  "occurred_at":"2026-09-30T04:30:00Z",
  "checked_seal_no":"SEAL-001","seal_intact":true}'

# 实验室入库
curl -X POST http://localhost:8080/api/v1/events -H 'Content-Type: application/json' -d '{
  "event_id":"EVT-S1","event_type":"storage","sample_code":"S-001",
  "occurred_at":"2026-09-30T05:00:00Z",
  "custodian":"实验室","storage_location":"冷库A-01"}'
```

- 新事件 `201`；同 `event_id` 同内容重试 `200`（`idempotent: true`）；异内容 `409 event_conflict`。
- 响应中 `evaluation.status` / `reasons` / `current_custodian` 为写入后重算结果。

### 5.3 查询

| 方法与路径 | 说明 |
|---|---|
| `GET /api/v1/samples` | 全部样本核验摘要（结论、原因、当前保管人） |
| `GET /api/v1/samples/{sample_code}` | 单样本登记信息 + 核验摘要 |
| `GET /api/v1/samples/{sample_code}/chain` | **完整证据链**：按发生时间排列的事件时间线、当前保管人、在途移交、结论与全部原因 |

```bash
curl http://localhost:8080/api/v1/samples/S-001/chain
```

`chain` 响应要点：

```json
{
  "sample": {"sample_code": "S-001", "collector": "王采集", "seal_no": "SEAL-001", "...": "..."},
  "status": "valid",
  "reasons": ["保管链完整：采集 王采集 至当前保管人 实验室，封签一致，已入库"],
  "current_custodian": "实验室",
  "pending_transfer": null,
  "has_storage": true,
  "timeline": [
    {"event_id": "EVT-T1", "event_type": "transfer", "occurred_at": "...", "recorded_at": "...",
     "payload": {"...": "..."}, "replaces_event_id": null, "batch_id": null}
  ],
  "corrections": [
    {"correction_id": "FIX-1", "target_event_id": "EVT-C1", "action": "replace",
     "operator": "质控员", "reason": "……", "new_event_id": "EVT-C2", "created_at": "..."}
  ],
  "superseded_events": [
    {"event_id": "EVT-C1", "event_type": "seal_check", "occurred_at": "...", "payload": {"...": "..."},
     "superseded_by": "FIX-1", "replaces_event_id": null}
  ]
}
```

- `timeline` 仅含**当前有效版本**事件（`replaces_event_id` 标记该事件是否由替换产生、指向原事件号；`batch_id` 标记事件是否为车次批量移交产生及其原始批次号，非批量事件为 `null`）；
- `superseded_events` 为被作废/替换的原始事件，仅用于追溯，不参与核验（同样带 `replaces_event_id` 与 `batch_id`：被纠错的批量移交原事件仍保留其批次归属，替换产生的新事件 `batch_id` 为 `null`）；
- `corrections` 为该样本全部纠错留痕（永久保留，核定后仍可查）。

### 5.4 入库核定

`POST /api/v1/samples/{sample_code}/approve`

```bash
curl -X POST http://localhost:8080/api/v1/samples/S-001/approve \
  -H 'Content-Type: application/json' -d '{"approver":"质量主管","note":"准予入库"}'
```

- `valid` → `200` 核定成功并冻结；`pending`/`invalid` → `422 approval_rejected`（响应体带回完整核验结果）。
- 核定后再上报事件或改写登记 → `409 history_locked`；重复核定幂等返回。
- 核定与纠错并发时，核定只可能基于**最终已提交的有效链**判定。

### 5.5 事件纠错（核定前：作废 / 替换）

`POST /api/v1/corrections`

公共字段：`correction_id`（全局唯一纠错号）、`sample_code`、`target_event_id`（被纠错的**当前有效**事件号）、`action`（`void` / `replace`）、`operator`、`reason`；`replace` 时必须提供 `new_event`（字段集与 5.2 事件完全一致，但**不含 `sample_code`**——纠错不得跨样本）。

```bash
# 作废：错误签收作废，核验随即只按剩余有效版本重算
curl -X POST http://localhost:8080/api/v1/corrections -H 'Content-Type: application/json' -d '{
  "correction_id":"FIX-0001","sample_code":"S-001","target_event_id":"EVT-R1",
  "action":"void","operator":"质控员","reason":"封签号录入错误，作废重报"}'

# 替换：给出完整新事件内容与未占用的新事件号
curl -X POST http://localhost:8080/api/v1/corrections -H 'Content-Type: application/json' -d '{
  "correction_id":"FIX-0002","sample_code":"S-001","target_event_id":"EVT-C1",
  "action":"replace","operator":"质控员","reason":"检查时扫码识别错误，整体替换",
  "new_event":{
    "event_id":"EVT-C2","event_type":"seal_check",
    "occurred_at":"2026-09-30T04:30:00Z",
    "checked_seal_no":"SEAL-001","seal_intact":true}}'
```

响应（`201` 新纠错；`200` 同纠错号同内容幂等重试）：

```json
{
  "correction": {
    "correction_id": "FIX-0002", "target_event_id": "EVT-C1", "action": "replace",
    "operator": "质控员", "reason": "检查时扫码识别错误，整体替换",
    "new_event_id": "EVT-C2", "created_at": "2026-09-30T05:10:00Z"
  },
  "evaluation": {"status": "valid", "reasons": ["..."], "current_custodian": "实验室", "...": "..."}
}
```

规则与语义：

| 规则 | 行为 |
|---|---|
| 准入 | 仅核定入库前可纠错；目标必须属于该样本且仍是当前有效版本 |
| 作废 `void` | 原事件标记失效（行不删除），不得携带 `new_event` |
| 替换 `replace` | 原事件标记失效，写入完整新事件；新事件号必须全局未占用（旧号永久留痕不可复用） |
| 重算 | 仅有效版本按 `occurred_at` 重算结论、原因、当前保管人；可连续替换（替换产生的新版本仍可再被纠错） |
| 幂等/冲突 | 同 `correction_id` 同内容重试 → `200`；异内容（含动作不同）→ `409 correction_conflict` |
| 并发 | 针对同一有效版本仅一次成功：落败方 `409 event_not_effective` 且事务回滚，链不变 |
| 核定后 | `409 history_locked`（包括与核定并发落败的纠错） |

纠错审计查询：

```bash
curl http://localhost:8080/api/v1/samples/S-001/corrections
# {"sample_code":"S-001","total":2,"items":[{"correction_id":"FIX-0002", ...}]}
```

被替换记录通过 `GET /api/v1/samples/{sample_code}/chain` 的 `superseded_events` / `corrections` 字段追溯（见 5.3）。

### 5.6 车次批量移交（同一车次多份样本，一次原子提交）

`POST /api/v1/transfer-batches`

同一交接车次可能涉及多份样本。提交全局唯一**批次号**、**统一移交时间**、**交出人**、**接收人**，以及每份样本的**编号、该份移交的事件号、移交单号**；服务端把各份作为既有 `transfer` 事件（内容与 5.2 完全相同）在一个事务内一次提交。

```bash
curl -X POST http://localhost:8080/api/v1/transfer-batches \
  -H 'Content-Type: application/json' -d '{
    "batch_id":"BATCH-20260930-01",
    "handed_over_at":"2026-09-30T02:00:00Z",
    "from_custodian":"王采集",
    "to_custodian":"李运输",
    "items":[
      {"sample_code":"S-001","event_id":"EVT-B001-T","transfer_no":"TR-B001"},
      {"sample_code":"S-002","event_id":"EVT-B002-T","transfer_no":"TR-B002"}
    ]}'
```

字段说明：

| 字段 | 说明 |
|---|---|
| `batch_id` | 全局唯一批次号（同一交接车次），同号重试幂等 |
| `handed_over_at` | 本批统一移交时间，作为各份 transfer 事件的发生时间 |
| `from_custodian` / `to_custodian` | 本批统一交出人 / 接收人（交出人非某样本当前保管人时，该份核验判无效） |
| `items[]` | 每份一项：`sample_code`、`event_id`（全局唯一事件号）、`transfer_no`（移交单号，按样本各自核验，批内不要求唯一）；至少 1 份 |

响应（`201` 新批次；`200` 同批次号同内容幂等重试）：

```json
{
  "batch": {
    "batch_id": "BATCH-20260930-01",
    "handed_over_at": "2026-09-30T02:00:00Z",
    "from_custodian": "王采集",
    "to_custodian": "李运输",
    "created_at": "2026-09-30T02:05:00Z"
  },
  "total": 2,
  "idempotent": false,
  "items": [
    {"sample_code": "S-001", "event_id": "EVT-B001-T", "transfer_no": "TR-B001",
     "event_effective": true, "superseded_by": null,
     "evaluation": {"status": "pending", "reasons": ["……"], "current_custodian": "王采集", "...": "..."}},
    {"sample_code": "S-002", "...": "..."}
  ]
}
```

规则：

| 规则 | 行为 |
|---|---|
| 整批原子性 | 批内样本编号重复 / 事件号重复 / 空批次 → `422`；任一样本不存在 → `404 sample_not_found`；任一样本已核定 → `409 history_locked`；任一事件号已占用 → `409 event_conflict`。任一不满足则**整批回滚，不留事件或批次** |
| 幂等 | 同 `batch_id` 同内容重试 → `200`（`idempotent: true`），**即使批次内样本已核定仍幂等**；逐份排列顺序不同但内容一致也视为同内容 |
| 冲突 | 同 `batch_id` 异内容（时间/交出人/接收人/份数/任一份的编号/事件号/移交单号不同）→ `409 batch_conflict` |
| 事件同源 | 批次写入的就是普通 `transfer` 事件：经 `POST /events` 同号同内容重试仍 `200`、异内容仍 `409 event_conflict`；反之单事件接口已占用的事件号，批次同样不得复用 |
| 并发 | 写事务串行，按已提交顺序判定：批次与核定并发、两个批次并发抢同一事件号，均只可能有一个写入方成功，落败方无任何残留 |

批次查询：

| 方法与路径 | 说明 |
|---|---|
| `GET /api/v1/transfer-batches` | 全部批次概览：批次号、统一移交时间、交出/接收人、份数 |
| `GET /api/v1/transfer-batches/{batch_id}` | **批次详情**：批次头 + 逐份样本的当前核验结论与原事件是否仍有效；未知批次 `404 batch_not_found` |

```bash
curl http://localhost:8080/api/v1/transfer-batches/BATCH-20260930-01
```

批次详情要点：

- `items[].evaluation` 为**查询时刻**重算的逐份结论（结论/原因/当前保管人/核定时间），后续签收、入库、纠错、核定都会反映进来；
- `items[].event_effective` 表示该份**原移交事件**当前是否仍在有效链上；被作废/替换后为 `false`，`superseded_by` 指向纠错号；
- 纠错（作废/替换）与核定**只改变有效链和上述查询结果**：批次头与各份原事件的批次归属永久保留。证据链（5.3）的 `timeline` 与 `superseded_events` 每个事件都带 `batch_id` 字段——被作废/替换的原批次事件仍可在 `superseded_events` 中按 `batch_id` 追溯；替换纠错产生的新事件 `batch_id` 为 `null`（不冒充原归属），批次份数不因替换增加。

### 5.7 核验结论变更（游标增量）

`GET /api/v1/changes?after={游标}&limit={条数}`

实验室对接系统按**全局递增游标**增量拉取每次实际提交后各受影响样本的结论快照。游标不按事件发生时间排序，而按**服务端事务提交可见顺序**递增（并发提交先提交者游标更小）。

| 参数 | 规则 |
|---|---|
| `after` | 起始游标，返回游标**严格大于**它的记录；首次拉取传 `0`。非负整数，非法（非数字/负数）→ `400 invalid_cursor`。传入尚不存在的大游标不算非法：返回空列表与原游标 |
| `limit` | 返回条数，默认 `50`、最大 `1000`；非负、≥1，非法 → `400 invalid_limit` |

```bash
curl "http://localhost:8080/api/v1/changes?after=0&limit=50"
# 下一页以返回的 next_cursor 作为 after
curl "http://localhost:8080/api/v1/changes?after=50&limit=50"
```

响应：

```json
{
  "after": 0,
  "limit": 50,
  "items": [
    {
      "cursor": 1,
      "sample_code": "S-001",
      "operation_type": "register",
      "business_ref": "S-001",
      "ref_event_type": null,
      "status": "pending",
      "reasons": ["缺少实验室入库记录"],
      "current_custodian": "王采集",
      "detail": {},
      "approval": null,
      "committed_at": "2026-09-30T01:05:00Z"
    },
    {
      "cursor": 2,
      "sample_code": "S-001",
      "operation_type": "event",
      "business_ref": "EVT-T1",
      "ref_event_type": "transfer",
      "status": "pending",
      "reasons": ["移交单 TR-1 已移交但缺少签收（……）", "缺少实验室入库记录"],
      "current_custodian": "王采集",
      "detail": {"event_id": "EVT-T1", "transfer_no": "TR-1"},
      "approval": null,
      "committed_at": "2026-09-30T02:05:00Z"
    }
  ],
  "next_cursor": 2,
  "has_more": false
}
```

字段说明：

| 字段 | 含义 |
|---|---|
| `cursor` | 本记录全局递增游标（整数，从 1 开始） |
| `operation_type` | `register`（登记）/ `event`（单事件上报）/ `batch_transfer`（车次批量移交，批内每份各一条）/ `correction`（作废/替换纠错）/ `approval`（入库核定）/ `baseline`（旧库升级基线） |
| `business_ref` | 业务编号：登记与核定为样本编号，事件为事件号，纠错为纠错号，批量移交为批次号（`detail.event_id` 区分批内份内事件号），基线为样本编号 |
| `ref_event_type` | 事件类操作的事件类型（`transfer`/`receive`/`seal_check`/`storage`），其余为 `null` |
| `status` / `reasons` / `current_custodian` | **提交后**按发生时间重算的结论、原因、当前保管人（迟到事件亦先重算后记录） |
| `detail` | 操作附加明细：事件带 `event_id`（transfer 另带 `transfer_no`）；批量带份内 `event_id`/`transfer_no`；纠错带 `action`/`target_event_id`/`new_event_id`；核定带 `approver` |
| `approval` | 提交后核定信息快照（`approved_storage_at`/`approved_by`/`note`），未核定或核定后未变操作为 `null` |
| `committed_at` | 提交时间（记录与业务改动同一事务） |
| `next_cursor` | 下一页起始游标（本页最后一条的游标）；**无后续记录时原样返回 `after`**，且 `items` 为空、`has_more=false` |
| `has_more` | 是否还有后续记录（据此或空列表结束增量循环） |

记录规则：

- 每次**实际提交成功**才写记录；同内容幂等重试、重复核定、失败请求（含 404/409/422 与整批回滚）不写。
- **即使结论类别未变**（如连续多个 `pending` 事件），只要有效链变化（新事件进入有效链、作废/替换改变有效版本）或核定信息变化，也逐条记录；无效事件（如重复签收）也会进入有效链重算并影响原因，故同样产生记录。
- 批量提交的 N 份记录与批次在**同一事务**原子可见，游标连续；查询批次内多份样本时按其游标即可区分同批先后。
- 游标顺序与提交可见顺序一致：`BEGIN IMMEDIATE` 串行化写事务，任何已读到游标 N 的客户端，之后都能读到全部游标 ≤ N 的记录。

### 错误响应统一格式

```json
{"error": {"code": "event_conflict", "message": "……", "details": {}}}
```

| HTTP | code | 触发 |
|---|---|---|
| 404 | `sample_not_found` / `event_not_found` / `batch_not_found` | 样本不存在 / 纠错目标事件不存在 / 批次不存在 |
| 409 | `sample_conflict` / `event_conflict` | 同编号异内容 / 同事件号异内容（批量提交时含已占用事件号亦为后者，整批回滚） |
| 409 | `batch_conflict` | 同批次号异内容（时间/交出接收人/份数/任一份编号、事件号、移交单号不同） |
| 409 | `history_locked` | 已核定入库后改写历史、纠错，或批量提交含已核定样本（整批回滚） |
| 409 | `correction_conflict` | 同纠错号异内容（含动作 void↔replace 不一致） |
| 409 | `correction_cross_sample` | 纠错目标事件不属于提交的样本（禁止跨样本） |
| 409 | `event_not_effective` | 目标事件已被作废/替换，或并发下被抢先纠错（非当前有效版本） |
| 409 | `new_event_id_occupied` | 替换所用新事件号已被占用 |
| 422 | `approval_rejected` | 非有效链尝试核定（或 Pydantic 字段校验失败） |
| 400 | `invalid_cursor` / `invalid_limit` | 变更查询 `after` 非非负整数 / `limit` 非 1~1000 的整数（`GET /api/v1/changes`） |

---

## 六、Python 调用示例

仓库内置一个**仅用标准库**（无需安装 requests）的端到端示例脚本：

```bash
python examples/call_api.py
```

脚本依次演示：登记 → 移交 → 签收 → 封签检查 → 入库 → 幂等重试 → 冲突 → 查询证据链 → **纠错替换 / 同号幂等 / 链追溯 / 作废演示 / 纠错审计查询** → 核定 → 核定后改写与纠错被拒 → **车次批量移交（提交/幂等/异内容冲突/批次查询/批次列表/对批内原移交纠错后的归属追溯）** → **核验结论变更游标增量（分页拉取/空尾原游标/非法参数报错）**。也可直接在 http://localhost:8080/docs 的 Swagger UI 中点击调用。

---

## 七、自测

```bash
python -m tests.test_chain          # 既有规则：42 项断言
python -m tests.test_corrections    # 纠错：65 项断言（含并发）
python -m tests.test_batches        # 车次批量移交：83 项断言（含并发）
python -m tests.test_changes        # 本轮游标变更记录：116 项断言（含并发与旧库升级）
```

- `test_chain` 覆盖 14 类场景：登记幂等/冲突、四类无效情形（非保管人移交、在途再移交、签收四要素不符、重复签收、封签不符/破损）、待补证两情形、乱序补录重算、核定准入与冻结、事件早于采集、无效优先等。
- `test_corrections` 覆盖：作废后仅有效版本重算（结论/原因/保管人）、替换生效与多跳替换、原事件与纠错关系留痕及链追溯、审计查询、同纠错号幂等/异内容冲突、跨样本拒绝、非有效版本拒绝、新事件号占用、replace/void 载荷校验、核定后拒绝纠错、同事件号 200/409 兼容、同有效版本并发仅一次成功、纠错与核定并发两种合法结局（可重复运行）。
- `test_batches` 覆盖：批内样本/事件号重复与空批次/多余字段 422、样本不存在与事件号占用整批回滚无残留、正常提交逐份结论与批次归属、同批次号幂等（含排列顺序无关）与六类异内容冲突、校验失败不占用批次号、批次事件与单事件接口互相遵守 200/409、含已核定样本整批失败但旧批次核定后仍幂等、作废/替换/连续纠错后批次查询的逐份结论与原事件有效性及归属追溯、批次列表与 404、批次与核定并发两种合法结局、两批次抢同一事件号恰一批成功且落败方无残留、既有接口字段冒烟。
- `test_changes` 覆盖：五类操作每次实际提交的记录字段（游标/编号/操作类型/业务编号/提交后结论/原因/保管人/核定信息）、结论类别未变仍记录、迟到事件按发生时间重算后再记、幂等重试/重复核定/失败请求/整批失败零记录、批量逐份同事务游标连续与批次号归因、作废/替换记录明细、游标分页/`next_cursor`/空尾原游标/非法游标与条数 400、并发提交游标连续且与提交可见顺序一致、旧库升级现存样本各一条 `baseline`（含已核定者核定信息、不伪造历史、重复初始化幂等、升级后新操作正常记录）。

---

## 九、旧库升级行为

服务启动（`init_db`，容器入口与本地 uvicorn 启动均会执行）时自动：

1. 幂等创建 `change_record` 变更记录表（并补齐既往版本的增量列与索引），**不影响既有表数据**；
2. 若库中已有样本但尚无任何变更记录（判定为本轮功能上线前的旧库），在独立 `BEGIN IMMEDIATE` 事务内为**每条现存样本**写入一条当前状态基线：
   - `operation_type=baseline`、`business_ref=样本编号`、`committed_at=升级时刻`；
   - `status`/`reasons`/`current_custodian` 为按现存数据即时重算的当前结论（含只按有效版本参与核验的纠错语义）；
   - 已核定样本的 `approval` 带历史核定信息（时间/操作人/备注）；
   - **不伪造**任何登记、事件、纠错、核定的历史操作记录；
3. 空库不写基线（随后的演示数据播种会产生真实的 register/event 记录）；已升级过的库重启不重复写基线（幂等，多进程同时启动也仅一个事务写入）；
4. 升级后所有新提交按 5.7 节规则正常产生变更记录；基线游标记为升级时刻的“已知当前状态”，对接系统以基线为起点继续增量即可。

---

## 十、目录结构

```
.
├── app/
│   ├── main.py       # FastAPI 路由、异常映射、启动初始化（含 /changes 游标查询）
│   ├── service.py    # 核心领域逻辑：幂等指纹、保管链重算、纠错（作废/替换）、核定冻结、车次批量移交、变更记录与基线
│   ├── schemas.py    # 请求模型：事件、纠错、批次（TransferBatchIn 批内去重校验）
│   ├── db.py         # SQLite 连接、建表与旧库迁移（change_record 表 + 现存样本基线）、IMMEDIATE 事务
│   ├── seed.py       # 演示数据初始化（幂等）
│   └── config.py     # 环境变量配置
├── examples/call_api.py   # 零依赖调用示例（含纠错、车次批量移交与游标增量拉取全流程）
├── tests/test_chain.py    # 既有规则端到端自测（42 项断言）
├── tests/test_corrections.py  # 纠错端到端自测（65 项断言，含并发）
├── tests/test_batches.py  # 车次批量移交端到端自测（83 项断言，含并发与整批回滚）
├── tests/test_changes.py  # 游标变更记录端到端自测（116 项断言，含并发与旧库升级基线）
├── entrypoint.sh     # 容器入口：建表 → 演示数据 → 启动
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```
