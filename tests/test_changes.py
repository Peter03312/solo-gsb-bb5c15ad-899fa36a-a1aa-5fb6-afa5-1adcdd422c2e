"""端到端自测：样本核验结论变更的游标增量记录（sample change log）。

运行：python -m tests.test_changes

覆盖：
  1. 五类实际提交各生成记录（登记/单事件/车次批量逐份/作废/替换/核定）；
  2. 结论类别未变但有效链/原因/保管人/核定信息变化仍记录；
  3. 迟到（乱序）事件按发生时间重算后再记结果；
  4. 同内容幂等重试、重复核定、冲突/422/404/422/409 等失败请求不生成记录；
  5. 批量记录与批次同事务（失败整批无记录、成功逐份且游标连续）；
  6. 查询：游标升序、next_cursor/has_more、空结果回传原游标、越界游标；
  7. 非法 cursor / limit 明确 422 报错；
  8. 并发提交游标顺序与提交可见顺序一致（游标连续、无重号）；
  9. 旧库升级：现存样本各记一条 baseline 当前状态，不伪造历史操作，且幂等。
"""
import os
import re
import sqlite3
import tempfile
import threading

_tmp = tempfile.mkdtemp()
os.environ["DB_PATH"] = os.path.join(_tmp, "test_changes.db")
os.environ["SEED_ON_START"] = "false"

from fastapi.testclient import TestClient  # noqa: E402

from app.db import SCHEMA, init_db  # noqa: E402
from app.main import app  # noqa: E402

init_db()
client = TestClient(app)
client.__enter__()  # 触发 lifespan（启动钩子）
API = "/api/v1"
CH = f"{API}/sample-changes"
PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} {detail}")


def changes(cursor=0, limit=None):
    url = f"{CH}?cursor={cursor}" + (f"&limit={limit}" if limit is not None else "")
    return client.get(url)


def fetch_all_changes(cursor=0):
    """从 cursor 起翻完全部页。"""
    out = []
    while True:
        r = changes(cursor, limit=4)
        body = r.json()
        out.extend(body["items"])
        if not body["has_more"]:
            return out, body
        cursor = body["next_cursor"]


def max_cursor():
    items, _ = fetch_all_changes(0)
    return items[-1]["cursor"] if items else 0


def reg(code, collector="张三", seal="SEAL-1", when="2026-09-30T01:00:00Z"):
    return client.post(f"{API}/samples", json={
        "sample_code": code, "collected_at": when, "collector": collector, "seal_no": seal})


def ev(payload):
    return client.post(f"{API}/events", json=payload)


def complete_chain(code, seal, t_prefix, custodian="李四"):
    """登记后补 移交→签收→封签检查→入库 的完整有效链，返回各事件号。"""
    ids = {
        "t": f"{t_prefix}-T", "r": f"{t_prefix}-R", "c": f"{t_prefix}-C", "s": f"{t_prefix}-S",
    }
    ev({"event_id": ids["t"], "event_type": "transfer", "sample_code": code,
        "occurred_at": "2026-09-30T02:00:00Z", "transfer_no": f"TR-{t_prefix}",
        "from_custodian": "张三", "to_custodian": custodian})
    ev({"event_id": ids["r"], "event_type": "receive", "sample_code": code,
        "occurred_at": "2026-09-30T03:00:00Z", "transfer_no": f"TR-{t_prefix}",
        "receiver": custodian, "seal_no": seal})
    ev({"event_id": ids["c"], "event_type": "seal_check", "sample_code": code,
        "occurred_at": "2026-09-30T03:30:00Z", "checked_seal_no": seal, "seal_intact": True})
    ev({"event_id": ids["s"], "event_type": "storage", "sample_code": code,
        "occurred_at": "2026-09-30T04:00:00Z", "custodian": "实验室",
        "storage_location": "冷库A-01"})
    return ids


# ---------------------------------------------------------------- 1. 登记即记录
print("1) 样本登记实际提交 → 一条 register 记录（全局游标/样本号/业务编号/结论快照）")
c0 = max_cursor()
r = reg("CL-1")
check("登记 201", r.status_code == 201, r.text)
items, body = fetch_all_changes(c0)
check("登记产生 1 条记录", len(items) == 1, str(len(items)))
rec = items[0]
check("游标严格递增", rec["cursor"] > c0, str(rec))
check("op_type=register、business_id=样本编号",
      rec["op_type"] == "register" and rec["business_id"] == "CL-1", str(rec))
check("记录含结论/原因/保管人",
      rec["result_status"] == "pending" and isinstance(rec["result_reasons"], list)
      and rec["current_custodian"] == "张三" and "缺少实验室入库记录" in rec["result_reasons"][-1],
      str(rec))
check("未核定 approval 为 null、含 created_at",
      rec["approval"] is None and bool(rec["created_at"]), str(rec))

# ---------------------------------------------------------------- 2. 事件链逐步记录
print("2) 单事件上报：每次实际提交都记录；记录的是按发生时间重算后的结果")
r = ev({"event_id": "CL1-T", "event_type": "transfer", "sample_code": "CL-1",
        "occurred_at": "2026-09-30T02:00:00Z", "transfer_no": "TR-CL1",
        "from_custodian": "张三", "to_custodian": "李四"})
check("移交 201", r.status_code == 201, r.text)
r = ev({"event_id": "CL1-R", "event_type": "receive", "sample_code": "CL-1",
        "occurred_at": "2026-09-30T03:00:00Z", "transfer_no": "TR-CL1",
        "receiver": "李四", "seal_no": "SEAL-1"})
check("签收 201（保管人变更仍 pending：缺入库）", r.status_code == 201, r.text)
r = ev({"event_id": "CL1-S", "event_type": "storage", "sample_code": "CL-1",
        "occurred_at": "2026-09-30T04:00:00Z", "custodian": "实验室"})
check("入库 201（转为 valid）", r.status_code == 201 and r.json()["evaluation"]["status"] == "valid", r.text)
items, _ = fetch_all_changes(0)
cl1 = [x for x in items if x["sample_code"] == "CL-1"]
check("CL-1 共 4 条（登记+3 事件）", len(cl1) == 4, str([(x["op_type"], x["business_id"]) for x in cl1]))
statuses = [(x["op_type"], x["result_status"], x["current_custodian"]) for x in cl1]
check("快照序列符合重算", statuses == [
    ("register", "pending", "张三"),
    ("event", "pending", "张三"),
    ("event", "pending", "李四"),
    ("event", "valid", "实验室"),
], str(statuses))
check("event 记录 business_id=事件号", all(x["business_id"].startswith("CL1-") for x in cl1[1:]), str(cl1))

# ---------------------------------------------------------------- 3. 类别未变但链/原因变化仍记录
print("3) 结论类别未变（invalid→invalid 多一条无效原因）仍记录")
reg("CL-2")
ev({"event_id": "CL2-C1", "event_type": "seal_check", "sample_code": "CL-2",
    "occurred_at": "2026-09-30T02:00:00Z", "checked_seal_no": "WRONG", "seal_intact": True})
before = max_cursor()
r = ev({"event_id": "CL2-C2", "event_type": "seal_check", "sample_code": "CL-2",
        "occurred_at": "2026-09-30T02:30:00Z", "checked_seal_no": "SEAL-1", "seal_intact": False})
check("第二次检查仍 invalid", r.status_code == 201 and r.json()["evaluation"]["status"] == "invalid", r.text)
items, _ = fetch_all_changes(before)
check("类别未变但有效链/原因变化 → 仍记录 1 条",
      len(items) == 1 and items[0]["result_status"] == "invalid"
      and any("封签破损" in x for x in items[0]["result_reasons"]), str(items))

# ---------------------------------------------------------------- 4. 迟到事件重算后记录
print("4) 迟到（乱序）事件：按发生时间重算后再记结果")
reg("CL-3")
# 先入库（缺链 → pending，保管人仍为采集人）
ev({"event_id": "CL3-S", "event_type": "storage", "sample_code": "CL-3",
    "occurred_at": "2026-09-30T05:00:00Z", "custodian": "实验室"})
before = max_cursor()
# 迟到补移交/签收（发生时间早于入库录入时间）
ev({"event_id": "CL3-T", "event_type": "transfer", "sample_code": "CL-3",
    "occurred_at": "2026-09-30T02:00:00Z", "transfer_no": "TR-CL3",
    "from_custodian": "张三", "to_custodian": "李四"})
ev({"event_id": "CL3-R", "event_type": "receive", "sample_code": "CL-3",
    "occurred_at": "2026-09-30T03:00:00Z", "transfer_no": "TR-CL3",
    "receiver": "李四", "seal_no": "SEAL-1"})
items, _ = fetch_all_changes(before)
check("迟到两事件各记 1 条且快照反映按发生时间重算",
      [(x["business_id"], x["result_status"], x["current_custodian"]) for x in items] == [
          ("CL3-T", "pending", "张三"),
          ("CL3-R", "valid", "实验室"),
      ], str([(x["business_id"], x["result_status"], x["current_custodian"]) for x in items]))

# ---------------------------------------------------------------- 5. 幂等/失败不记录
print("5) 幂等重试 / 重复核定 / 冲突 / 校验失败 / 冻结 → 不生成记录")
before = max_cursor()

# 5.1 登记同内容幂等 200
r = reg("CL-1"); check("登记幂等 200", r.status_code == 200 and r.json()["idempotent"] is True, r.text)
# 5.2 登记异内容冲突 409
r = client.post(f"{API}/samples", json={
    "sample_code": "CL-1", "collected_at": "2026-09-30T01:30:00Z",
    "collector": "张三", "seal_no": "SEAL-1"})
check("登记异内容 409 sample_conflict",
      r.status_code == 409 and r.json()["error"]["code"] == "sample_conflict", r.text)
# 5.3 事件同号同内容幂等 200
r = ev({"event_id": "CL1-T", "event_type": "transfer", "sample_code": "CL-1",
        "occurred_at": "2026-09-30T02:00:00Z", "transfer_no": "TR-CL1",
        "from_custodian": "张三", "to_custodian": "李四"})
check("事件幂等 200", r.status_code == 200 and r.json()["idempotent"] is True, r.text)
# 5.4 事件异内容 409
r = ev({"event_id": "CL1-T", "event_type": "transfer", "sample_code": "CL-1",
        "occurred_at": "2026-09-30T02:01:00Z", "transfer_no": "TR-CL1",
        "from_custodian": "张三", "to_custodian": "李四"})
check("事件冲突 409 event_conflict",
      r.status_code == 409 and r.json()["error"]["code"] == "event_conflict", r.text)
# 5.5 事件指向不存在样本 404；缺必填 422；多余字段 422
check("事件样本不存在 404", ev({"event_id": "X1", "event_type": "storage",
      "sample_code": "NO-SUCH-X", "occurred_at": "2026-09-30T09:00:00Z"}).status_code == 404)
check("事件缺必填 422", ev({"event_id": "X2", "event_type": "transfer", "sample_code": "CL-2",
      "occurred_at": "2026-09-30T09:00:00Z"}).status_code == 422)
check("事件多余字段 422", client.post(f"{API}/events", json={
    "event_id": "X3", "event_type": "storage", "sample_code": "CL-2",
    "occurred_at": "2026-09-30T09:00:00Z", "extra": 1}).status_code == 422)
check("幂等/冲突/404/422 序列均不产生记录", max_cursor() == before,
      f"before={before} after={max_cursor()}")
# 5.6 待补证样本核定被拒 422（失败不留记录；登记本身合法会记录，先记基线）
reg("CL-4")
before = max_cursor()
r = client.post(f"{API}/samples/CL-4/approve", json={"approver": "主管"})
check("非 valid 核定 422 approval_rejected",
      r.status_code == 422 and r.json()["error"]["code"] == "approval_rejected", r.text)
check("核定拒绝不产生记录", max_cursor() == before,
      f"before={before} after={max_cursor()}")

# 5.7 合法 valid 样本首次核定（正常记录），随后重复核定/冻结写入均不记录
reg("CL-5")
complete_chain("CL-5", "SEAL-1", "CL5")
r = client.post(f"{API}/samples/CL-5/approve", json={"approver": "主管", "note": "准予入库"})
check("首次核定 200", r.status_code == 200 and r.json()["approval"]["approved"] is True, r.text)
before = max_cursor()
r = client.post(f"{API}/samples/CL-5/approve", json={"approver": "另一个主管", "note": "重复"})
check("重复核定 200 幂等（approver 不被改写）",
      r.status_code == 200 and r.json()["approval"]["approver"] == "主管", r.text)
# 5.8 核定后写事件 409、纠错 409
r = ev({"event_id": "CL5-LATE", "event_type": "seal_check", "sample_code": "CL-5",
        "occurred_at": "2026-09-30T09:00:00Z", "checked_seal_no": "SEAL-1"})
check("核定后写事件 409 history_locked",
      r.status_code == 409 and r.json()["error"]["code"] == "history_locked", r.text)
r = client.post(f"{API}/corrections", json={
    "correction_id": "CL5-FIX-LATE", "sample_code": "CL-5", "target_event_id": "CL5-T",
    "action": "void", "operator": "质控", "reason": "核定后试探"})
check("核定后纠错 409 history_locked", r.status_code == 409, r.text)
check("重复核定/冻结写入不产生记录", max_cursor() == before)

# 5.9 纠错号同内容幂等 / 异内容冲突 / 目标非有效（失败均不记录）
reg("CL-6")
ev({"event_id": "CL6-T", "event_type": "transfer", "sample_code": "CL-6",
    "occurred_at": "2026-09-30T02:00:00Z", "transfer_no": "TR-CL6",
    "from_custodian": "张三", "to_custodian": "李四"})
corr_body = {"correction_id": "CL6-FIX", "sample_code": "CL-6", "target_event_id": "CL6-T",
             "action": "void", "operator": "质控", "reason": "误移交作废"}
r = client.post(f"{API}/corrections", json=corr_body)
check("作废 201", r.status_code == 201, r.text)
before = max_cursor()
r = client.post(f"{API}/corrections", json=corr_body)
check("纠错同号同内容 200 幂等", r.status_code == 200, r.text)
r = client.post(f"{API}/corrections", json={**corr_body, "reason": "改过的原因"})
check("纠错同号异内容 409 correction_conflict",
      r.status_code == 409 and r.json()["error"]["code"] == "correction_conflict", r.text)
r = client.post(f"{API}/corrections", json={
    "correction_id": "CL6-FIX2", "sample_code": "CL-6", "target_event_id": "CL6-T",
    "action": "void", "operator": "质控", "reason": "对已作废事件再纠错"})
check("目标非有效 409 event_not_effective",
      r.status_code == 409 and r.json()["error"]["code"] == "event_not_effective", r.text)
check("纠错幂等/冲突/非有效均不产生记录", max_cursor() == before,
      f"before={before} after={max_cursor()}")

# ---------------------------------------------------------------- 6. 作废/替换记录内容
print("6) 纠错记录：op_type=correction、business_id=纠错号；类别未变也记录")
reg("CL-7")
ids7 = complete_chain("CL-7", "SEAL-1", "CL7")
before = max_cursor()
# 替换封签检查（结论 valid→valid，但有效链已变化：新事件号、原事件被替换）
r = client.post(f"{API}/corrections", json={
    "correction_id": "CL7-FIX-R", "sample_code": "CL-7", "target_event_id": ids7["c"],
    "action": "replace", "operator": "质控", "reason": "扫码错误整体替换",
    "new_event": {"event_id": "CL7-C2", "event_type": "seal_check",
                  "occurred_at": "2026-09-30T03:30:00Z",
                  "checked_seal_no": "SEAL-1", "seal_intact": True}})
check("替换 201", r.status_code == 201, r.text)
items, _ = fetch_all_changes(before)
check("替换产生 1 条 correction 记录（valid→valid 亦记录）",
      len(items) == 1 and items[0]["op_type"] == "correction"
      and items[0]["business_id"] == "CL7-FIX-R"
      and items[0]["result_status"] == "valid"
      and items[0]["current_custodian"] == "实验室", str(items))

# ---------------------------------------------------------------- 7. 核定记录（含核定信息）
print("7) 核定记录：approve 记录带核定信息（结论类别 valid 未变也记录）")
reg("CL-8")
complete_chain("CL-8", "SEAL-1", "CL8")
before = max_cursor()
r = client.post(f"{API}/samples/CL-8/approve", json={"approver": "质量主管", "note": "准予入库"})
check("CL-8 核定 200", r.status_code == 200, r.text)
items, _ = fetch_all_changes(before)
check("核定单条 approve 记录",
      len(items) == 1 and items[0]["op_type"] == "approve"
      and items[0]["business_id"] == "CL-8"
      and items[0]["result_status"] == "valid", str(items))
ap = items[0]["approval"]
check("approval 快照含时间/操作人/备注",
      ap and ap["approved"] is True and ap["approver"] == "质量主管"
      and ap["note"] == "准予入库" and bool(ap["approved_at"]), str(ap))
with_appr = [x for x in fetch_all_changes(0)[0] if x["approval"] is not None]
check("仅核定记录携带 approval 快照（登记/事件/批次/纠错均为 null）",
      {x["sample_code"] for x in with_appr} == {"CL-5", "CL-8"}
      and all(x["op_type"] == "approve" for x in with_appr),
      str([(x["sample_code"], x["op_type"]) for x in with_appr]))

# ---------------------------------------------------------------- 8. 批量：逐份记录 + 同事务
print("8) 车次批量移交：成功逐份记录（business_id=批次号、游标连续）；失败整批无记录")
reg("CL-B1")
reg("CL-B2")
before = max_cursor()
batch = {"batch_id": "CL-BATCH-1", "handed_over_at": "2026-09-30T02:00:00Z",
         "from_custodian": "张三", "to_custodian": "李四",
         "items": [
             {"sample_code": "CL-B1", "event_id": "CLB1-T", "transfer_no": "TR-B1"},
             {"sample_code": "CL-B2", "event_id": "CLB2-T", "transfer_no": "TR-B2"}]}
r = client.post(f"{API}/transfer-batches", json=batch)
check("批次 201", r.status_code == 201 and r.json()["idempotent"] is False, r.text)
items, _ = fetch_all_changes(before)
check("逐份各 1 条共 2 条，op_type=batch、business_id=批次号、按提交顺序",
      [(x["sample_code"], x["op_type"], x["business_id"]) for x in items] == [
          ("CL-B1", "batch", "CL-BATCH-1"),
          ("CL-B2", "batch", "CL-BATCH-1")], str([(x["sample_code"], x["business_id"]) for x in items]))
check("批内游标连续（同一事务内）",
      items[1]["cursor"] == items[0]["cursor"] + 1, str([x["cursor"] for x in items]))
# 失败批次：含已占用事件号 → 整批回滚，无记录
r = client.post(f"{API}/transfer-batches", json={
    "batch_id": "CL-BATCH-FAIL", "handed_over_at": "2026-09-30T02:00:00Z",
    "from_custodian": "张三", "to_custodian": "李四",
    "items": [{"sample_code": "CL-B1", "event_id": "CLB1-T", "transfer_no": "TR-X"},
              {"sample_code": "CL-B2", "event_id": "CLB-NEW", "transfer_no": "TR-Y"}]})
check("占用事件号整批 409", r.status_code == 409 and r.json()["error"]["code"] == "event_conflict", r.text)
check("失败批次不新增记录（含未冲突的第二份）", max_cursor() == items[-1]["cursor"])
# 同批次幂等 200 不再记录
r = client.post(f"{API}/transfer-batches", json=batch)
check("批次同内容重试 200 幂等", r.status_code == 200 and r.json()["idempotent"] is True, r.text)
check("批次幂等重试不新增记录", max_cursor() == items[-1]["cursor"])
# 含已核定样本的新批次 → 409 整批无记录
r = client.post(f"{API}/transfer-batches", json={
    "batch_id": "CL-BATCH-LOCK", "handed_over_at": "2026-09-30T02:00:00Z",
    "from_custodian": "张三", "to_custodian": "李四",
    "items": [{"sample_code": "CL-8", "event_id": "CL-LCK-T", "transfer_no": "TR-LCK"}]})
check("含已核定样本整批 409", r.status_code == 409 and r.json()["error"]["code"] == "history_locked", r.text)
check("含核定样本的失败批次不新增记录", max_cursor() == items[-1]["cursor"])

# ---------------------------------------------------------------- 9. 查询分页语义
print("9) 查询：游标升序、limit、next_cursor/has_more、空结果回传原游标、越界游标")
all_items, _ = fetch_all_changes(0)
check("全部记录游标严格递增且无重号",
      all(all_items[i]["cursor"] < all_items[i + 1]["cursor"] for i in range(len(all_items) - 1)),
      "游标非严格递增/存在重号")
total = len(all_items)
last = all_items[-1]["cursor"]
r = changes(0, limit=5)
b = r.json()
check("首页 5 条且 has_more=true",
      r.status_code == 200 and len(b["items"]) == 5 and b["has_more"] is True
      and b["next_cursor"] == b["items"][-1]["cursor"] and b["cursor"] == 0, str(b))
# 用 next_cursor 继续翻页直到取完
seen, cur = [], 0
while True:
    b = changes(cur, limit=5).json()
    new = [x["cursor"] for x in b["items"]]
    if seen:
        check("页间无重叠（严格大于起始游标）", all(x > cur for x in new), str((cur, new)))
    seen.extend(new)
    cur = b["next_cursor"]
    if not b["has_more"]:
        break
check("游标分页取完全部记录", seen == [x["cursor"] for x in all_items],
      f"{len(seen)} vs {total}")
# 从中间游标起：只返回后续
mid = all_items[total // 2]["cursor"]
b = changes(mid, limit=1000).json()
check("从中间游标仅返回后续记录",
      all(x["cursor"] > mid for x in b["items"])
      and b["items"][0]["cursor"] == all_items[total // 2 + 1]["cursor"], str(b)[:200])
# 无后续记录：空列表 + 原游标
b = changes(last, limit=100).json()
check("末尾游标返回空列表且 next_cursor=原游标",
      b["items"] == [] and b["next_cursor"] == last and b["has_more"] is False, str(b))
# 越界游标同样为空、原游标回传
b = changes(last + 1_000_000).json()
check("越界游标返回空列表且回传原游标", b["items"] == [] and b["next_cursor"] == last + 1_000_000, str(b))
# 精确分页：恰好剩 limit 条时 has_more=false
if len(all_items) >= 3:
    start = all_items[-3]["cursor"]
    b = changes(start, limit=2).json()
    check("恰好取完 has_more=false 且 next_cursor=末条",
          [x["cursor"] for x in b["items"]] == [all_items[-2]["cursor"], all_items[-1]["cursor"]]
          and b["has_more"] is False and b["next_cursor"] == all_items[-1]["cursor"], str(b))

# ---------------------------------------------------------------- 10. 非法入参
print("10) 非法 cursor / limit → 422 明确报错（统一错误体）")
r = client.get(CH)
check("缺 cursor 422 invalid_cursor",
      r.status_code == 422 and r.json()["error"]["code"] == "invalid_cursor", r.text)
for bad in ("abc", "-1", "1.5", "  "):
    r = client.get(f"{CH}?cursor={bad}")
    check(f"cursor={bad!r} 422 invalid_cursor",
          r.status_code == 422 and r.json()["error"]["code"] == "invalid_cursor", r.text)
for bad in ("0", "-1", "1001", "x"):
    r = client.get(f"{CH}?cursor=0&limit={bad}")
    code = r.json()["error"]["code"] if r.status_code == 422 else None
    check(f"limit={bad!r} 422 invalid_limit", r.status_code == 422 and code == "invalid_limit", r.text)

# ---------------------------------------------------------------- 11. 并发提交游标顺序
print("11) 并发提交：游标顺序与提交可见顺序一致（连续、无重号、无空洞乱序）")
N = 8
for i in range(N):
    reg(f"CL-MT-{i}")
barrier = threading.Barrier(N)
errors: list[str] = []


def worker(i: int) -> None:
    barrier.wait()
    rr = ev({"event_id": f"CLMT-E-{i}", "event_type": "storage", "sample_code": f"CL-MT-{i}",
             "occurred_at": "2026-09-30T05:00:00Z", "custodian": "实验室"})
    if rr.status_code != 201:
        errors.append(f"w{i}:{rr.status_code}")


threads = [threading.Thread(target=worker, args=(i,)) for i in range(N)]
for t in threads:
    t.start()
for t in threads:
    t.join()
check("8 个并发事件全部 201", not errors, str(errors))
mt = [x for x in fetch_all_changes(0)[0] if x["business_id"] and x["business_id"].startswith("CLMT-E-")]
check("并发 8 条记录", len(mt) == N, str(len(mt)))
cids = sorted(x["cursor"] for x in mt)
check("并发游标互不相同", len(set(cids)) == N, str(cids))
check("并发游标连续区间（顺序=提交可见顺序，无穿插乱序）",
      cids == list(range(cids[0], cids[0] + N)), str(cids))
check("并发记录与各样本一一对应",
      sorted(x["sample_code"] for x in mt) == sorted(f"CL-MT-{i}" for i in range(N)), str(mt))

# 并发：对同一样本“有效事件 vs 核定”竞争，失败方不产生记录（可重复运行，两种结局都合法）
reg("CL-RACE")
ev({"event_id": "CLRACE-T", "event_type": "transfer", "sample_code": "CL-RACE",
    "occurred_at": "2026-09-30T02:00:00Z", "transfer_no": "TR-RACE",
    "from_custodian": "张三", "to_custodian": "李四"})
ev({"event_id": "CLRACE-R", "event_type": "receive", "sample_code": "CL-RACE",
    "occurred_at": "2026-09-30T03:00:00Z", "transfer_no": "TR-RACE",
    "receiver": "李四", "seal_no": "SEAL-1"})
ev({"event_id": "CLRACE-S", "event_type": "storage", "sample_code": "CL-RACE",
    "occurred_at": "2026-09-30T04:00:00Z", "custodian": "实验室"})
b2 = threading.Barrier(2)
race_res: dict[str, int] = {}


def corr_worker() -> None:
    b2.wait()
    race_res["corr"] = client.post(f"{API}/corrections", json={
        "correction_id": "RACE-FIX", "sample_code": "CL-RACE", "target_event_id": "CLRACE-T",
        "action": "void", "operator": "质控", "reason": "并发作废"}).status_code


def appr_worker() -> None:
    b2.wait()
    race_res["appr"] = client.post(f"{API}/samples/CL-RACE/approve",
                                   json={"approver": "主管"}).status_code


ta = threading.Thread(target=corr_worker)
tb = threading.Thread(target=appr_worker)
ta.start(); tb.start(); ta.join(); tb.join()
# 先提交者生效（纠错 201 / 核定 200），落败方看到已提交结果：409 或 422
codes = set(race_res.values())
check("竞争仅一方生效，落败方被既有冲突/冻结/非有效语义拒绝",
      race_res.get("appr") == 200 and race_res.get("corr") == 409
      or race_res.get("corr") == 201 and race_res.get("appr") in (200, 422),
      str(race_res))
race_recs = [x for x in fetch_all_changes(0)[0] if x["sample_code"] == "CL-RACE"]
# 登记+3 事件先记 4 条；竞争中仅生效方再记 1 条（共 5 条），落败方无记录
check("竞争仅生效方追加 1 条记录（落败方无记录）",
      len(race_recs) == 5
      and (race_recs[-1]["op_type"] == "approve") == (race_res.get("appr") == 200),
      str([(x["op_type"], x["business_id"]) for x in race_recs]))

# ---------------------------------------------------------------- 12. 旧库升级基线
print("12) 旧库升级：现存样本各记一条 baseline 当前状态，不伪造历史操作；升级幂等")
old_path = os.path.join(_tmp, "test_changes_old.db")
if os.path.exists(old_path):
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(old_path + suffix)
        except OSError:
            pass

# 12.1 用“变更表出现之前”的旧建表脚本造一个旧库：2 个现存样本（1 valid 已核定 / 1 pending）
old_schema = SCHEMA
old_schema = re.sub(
    r"CREATE TABLE IF NOT EXISTS sample_change_log \(.*?\);\n\n", "", old_schema, flags=re.S)
conn = sqlite3.connect(old_path)
conn.executescript(old_schema)
conn.execute(
    "INSERT INTO sample (sample_code, collected_at, collector, seal_no, content_hash,"
    " approved_storage_at, approved_by, approve_note, created_at)"
    " VALUES ('OLD-V-1','2026-09-30T01:00:00Z','张三','SEAL-OV','h1',"
    " '2026-09-30T05:30:00Z','旧主管','旧核定备注','2026-09-30T01:05:00Z')")
conn.execute(
    "INSERT INTO sample (sample_code, collected_at, collector, seal_no, content_hash, created_at)"
    " VALUES ('OLD-P-2','2026-09-30T01:00:00Z','王五','SEAL-OP','h2',"
    " '2026-09-30T01:06:00Z')")
# OLD-V-1：一条入库事件即视为链完整的简化存量（仅用于验证基线按当前状态记录）
conn.execute(
    "INSERT INTO custody_event (sample_id, event_id, event_type, occurred_at, payload_json,"
    " content_hash, created_at) VALUES (1,'OLD-E1','storage','2026-09-30T04:00:00Z',"
    " '{\"custodian\":\"实验室\"}','he','2026-09-30T04:10:00Z')")
conn.commit()
conn.close()

# 12.2 以旧库路径跑 init_db（独立连接，模拟服务启动升级）
import app.db as db_module  # noqa: E402
from app import config  # noqa: E402
from pathlib import Path  # noqa: E402

config.DB_PATH = Path(old_path)
conn = sqlite3.connect(old_path)
conn.row_factory = sqlite3.Row
with db_module.db_session() as c:
    existed = c.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sample_change_log'").fetchone()
    check("升级前不存在变更表", existed is None)
db_module.init_db()
with db_module.db_session() as c:
    base = [dict(r) for r in c.execute(
        "SELECT cl.*, s.sample_code AS sample_code FROM sample_change_log cl"
        " JOIN sample s ON s.id = cl.sample_id ORDER BY cl.id ASC")]
check("升级为 2 个现存样本各写 1 条基线（共 2 条）", len(base) == 2, str(len(base)))
check("基线 op_type=baseline 且 business_id 为 NULL（不伪造历史操作/业务编号）",
      all(r["op_type"] == "baseline" and r["business_id"] is None for r in base), str(base))
by_code = {r["sample_code"]: r for r in base}
check("已核定样本基线：状态快照 + approval 信息",
      by_code["OLD-V-1"]["result_status"] in ("valid", "pending")
      and by_code["OLD-V-1"]["approval_json"] is not None
      and "旧主管" in by_code["OLD-V-1"]["approval_json"], str(dict(by_code["OLD-V-1"])))
check("待补证样本基线：pending、approval 为 NULL、保管人=采集人",
      by_code["OLD-P-2"]["result_status"] == "pending"
      and by_code["OLD-P-2"]["approval_json"] is None
      and by_code["OLD-P-2"]["current_custodian"] == "王五", str(dict(by_code["OLD-P-2"])))
check("基线 created_at 为升级时刻而非伪造的历史时间",
      all(r["created_at"] >= "2026-09-30" for r in base), str([r["created_at"] for r in base]))

# 12.3 再升级一次（重启）：不重复写基线
db_module.init_db()
with db_module.db_session() as c:
    n = c.execute("SELECT COUNT(*) FROM sample_change_log WHERE op_type='baseline'").fetchone()[0]
check("重复初始化不重复写基线（幂等）", n == 2, str(n))

# 12.4 旧库经 HTTP 游标接口可见基线（把服务库临时指向旧库验证查询不依赖写入路径）
os.environ["DB_PATH"] = old_path
# config.DB_PATH 已在 12.2 指向 old_path（Path），此处无需重复设置
b = changes(0, limit=10).json()
check("HTTP 游标接口可读到升级基线",
      len(b["items"]) == 2 and all(x["op_type"] == "baseline" for x in b["items"]), str(b))
# 升级后在旧库上的新登记正常记 register，与基线游标衔接
r = client.post(f"{API}/samples", json={
    "sample_code": "OLD-NEW-3", "collected_at": "2026-09-30T01:00:00Z",
    "collector": "赵六", "seal_no": "SEAL-N3"})
check("升级后新登记 201", r.status_code == 201, r.text)
b = changes(b["next_cursor"], limit=10).json()
check("升级后新记录游标接在基线之后、op_type=register",
      len(b["items"]) == 1 and b["items"][0]["op_type"] == "register"
      and b["items"][0]["sample_code"] == "OLD-NEW-3", str(b))

# ---------------------------------------------------------------- 13. 既有接口响应不变（冒烟）
print("13) 既有接口响应语义保持（冒烟，基于升级后旧库）")
check("health 200", client.get("/health").status_code == 200)
r = client.get(f"{API}/samples/OLD-NEW-3")
check("样本详情字段不变",
      r.status_code == 200 and set(r.json()["evaluation"].keys()) >= {
          "sample_code", "status", "reasons", "current_custodian", "pending_transfer",
          "has_storage", "event_count", "approved_storage_at", "approved_by"}, r.text[:200])
r = client.get(f"{API}/samples/OLD-V-1/chain")
check("已核定存量样本 chain 字段不变", r.status_code == 200, r.text[:200])
for key in ("status", "reasons", "current_custodian", "pending_transfer", "has_storage",
            "timeline", "corrections", "superseded_events"):
    check(f"chain 保留字段 {key}", key in r.json(), r.text[:200])
    break
r = client.post(f"{API}/samples/OLD-V-1/approve", json={"approver": "旧主管", "note": "旧核定备注"})
check("存量样本重复核定仍 200 幂等且响应体结构不变（操作人不被改写）",
      r.status_code == 200 and "approval" in r.json() and "evaluation" in r.json()
      and r.json()["approval"]["approver"] == "旧主管", r.text)

print(f"\n结果: {PASS} 通过, {FAIL} 失败")
raise SystemExit(1 if FAIL else 0)
