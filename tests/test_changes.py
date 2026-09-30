"""端到端自测：样本核验结论变更游标增量（change records）。

覆盖：
  1. 登记/单事件/批量移交/作废替换纠错/入库核定每次实际提交都记录游标、样本编号、
     操作类型及业务编号、提交后结论/原因/当前保管人/核定信息；
  2. 结论类别未变但有效链变化仍记录；迟到事件按发生时间重算后再记结果；
  3. 同内容幂等重试、重复核定、失败请求不生成记录；
  4. 批量记录与批次同事务（逐份一条、游标连续，整批失败零记录）；
  5. 游标增量查询：升序、分页 next_cursor、无后续返回空列表与原游标、非法游标/条数 400；
  6. 并发提交的游标顺序与提交可见顺序一致；
  7. 旧库升级：现存样本各记一条 baseline 当前状态（含已核定者的核定信息），
     不伪造历史操作，重复初始化幂等。

运行：python -m tests.test_changes
"""
import json
import os
import sqlite3
import tempfile
import threading
from pathlib import Path

_tmp = tempfile.mkdtemp()
os.environ["DB_PATH"] = os.path.join(_tmp, "test_changes.db")
os.environ["SEED_ON_START"] = "false"

from fastapi.testclient import TestClient  # noqa: E402

from app import config  # noqa: E402
from app.db import db_session, init_db  # noqa: E402
from app.main import app  # noqa: E402
from app.service import query_changes  # noqa: E402

init_db()
client = TestClient(app)
client.__enter__()  # 触发 lifespan（启动钩子）
API = "/api/v1"
PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} {detail}")


def feed(after=0, limit=1000):
    return client.get(f"{API}/changes", params={"after": after, "limit": limit}).json()


def records_for(code):
    return [x for x in feed()["items"] if x["sample_code"] == code]


def max_cursor():
    items = feed()["items"]
    return items[-1]["cursor"] if items else 0


def reg(code, collector="张三", seal="SEAL-1", when="2026-09-30T01:00:00Z"):
    return client.post(f"{API}/samples", json={
        "sample_code": code, "collected_at": when, "collector": collector, "seal_no": seal})


def ev(payload):
    return client.post(f"{API}/events", json=payload)


def valid_chain(code, seal, prefix, collector="张三", to="李四"):
    """移交→签收→封签检查→入库（事件号带 prefix 防全局冲突）。"""
    ev({"event_id": f"{prefix}-T", "event_type": "transfer", "sample_code": code,
        "occurred_at": "2026-09-30T02:00:00Z", "transfer_no": f"{prefix}-TR",
        "from_custodian": collector, "to_custodian": to})
    ev({"event_id": f"{prefix}-R", "event_type": "receive", "sample_code": code,
        "occurred_at": "2026-09-30T03:00:00Z", "transfer_no": f"{prefix}-TR",
        "receiver": to, "seal_no": seal})
    ev({"event_id": f"{prefix}-C", "event_type": "seal_check", "sample_code": code,
        "occurred_at": "2026-09-30T03:30:00Z", "checked_seal_no": seal, "seal_intact": True})
    ev({"event_id": f"{prefix}-S", "event_type": "storage", "sample_code": code,
        "occurred_at": "2026-09-30T04:00:00Z", "custodian": "实验室", "storage_location": "冷库A-01"})


REQUIRED_KEYS = {"cursor", "sample_code", "operation_type", "business_ref", "ref_event_type",
                 "status", "reasons", "current_custodian", "detail", "approval", "committed_at"}

# ---------------------------------------------------------------- 1. 登记即记录
print("1) 样本登记：记录游标/编号/操作类型/业务编号/提交后结论与保管人")
r = reg("CH-1")
check("登记 201", r.status_code == 201, r.text)
recs = records_for("CH-1")
check("登记产生 1 条变更", len(recs) == 1, str(recs))
rec = recs[0]
check("记录字段完整", REQUIRED_KEYS <= set(rec.keys()), str(rec.keys()))
check("operation_type=register", rec["operation_type"] == "register", str(rec))
check("business_ref=样本编号", rec["business_ref"] == "CH-1", str(rec))
check("ref_event_type 为空", rec["ref_event_type"] is None, str(rec))
check("登记后 pending（缺入库）", rec["status"] == "pending", str(rec))
check("原因是非空列表", isinstance(rec["reasons"], list) and rec["reasons"], str(rec["reasons"]))
check("当前保管人=采集人", rec["current_custodian"] == "张三", str(rec))
check("未核定 approval=null", rec["approval"] is None, str(rec))
check("detail 为空对象", rec["detail"] == {}, str(rec))
check("committed_at 非空", bool(rec["committed_at"]))
first_cursor = rec["cursor"]

# ---------------------------------------------------------------- 2. 单事件链：每次提交都记录
print("2) 单事件链：每次实际提交都记录；连续同类结论也记录")
before = len(feed()["items"])
valid_chain("CH-1", "SEAL-1", "CH1")
after = len(feed()["items"])
check("4 个事件 4 条记录（无论结论类别是否变化）", after - before == 4, f"{before}->{after}")
recs = records_for("CH-1")
statuses = [(x["operation_type"], x["business_ref"], x["status"], x["ref_event_type"]) for x in recs]
check("操作序列与业务编号正确",
      [s[0] for s in statuses] == ["register", "event", "event", "event", "event"],
      str(statuses))
check("业务编号=各事件号",
      [s[1] for s in statuses[1:]] == ["CH1-T", "CH1-R", "CH1-C", "CH1-S"], str(statuses))
check("ref_event_type 随事件类型",
      [s[3] for s in statuses[1:]] == ["transfer", "receive", "seal_check", "storage"], str(statuses))
# transfer→receive→seal_check 期间结论均为 pending（类别未变也逐条记录）
check("移交/签收/检查阶段均为 pending（类别未变仍记录）",
      all(s[2] == "pending" for s in statuses[1:4]), str(statuses))
check("入库后 valid", statuses[4][2] == "valid", str(statuses))
check("游标严格递增", all(recs[i]["cursor"] < recs[i + 1]["cursor"] for i in range(len(recs) - 1)))
check("保管人随签收变为李四、入库变为实验室",
      [x["current_custodian"] for x in recs][-3:] == ["李四", "李四", "实验室"],
      str([(x["business_ref"], x["current_custodian"]) for x in recs]))
transfer_rec = next(x for x in recs if x["business_ref"] == "CH1-T")
check("transfer 明细带 event_id/transfer_no",
      transfer_rec["detail"] == {"event_id": "CH1-T", "transfer_no": "CH1-TR"}, str(transfer_rec["detail"]))
storage_rec = next(x for x in recs if x["business_ref"] == "CH1-S")
check("非 transfer 事件明细仅 event_id", storage_rec["detail"] == {"event_id": "CH1-S"}, str(storage_rec["detail"]))

# ---------------------------------------------------------------- 3. 迟到事件按发生时间重算后记录
print("3) 乱序/迟到事件：记录的是按发生时间重算后的结果")
reg("CH-2")
# 先报入库（链缺失 → pending，且原因指出入库缺前序链）
ev({"event_id": "CH2-S", "event_type": "storage", "sample_code": "CH-2",
    "occurred_at": "2026-09-30T04:00:00Z", "custodian": "实验室"})
late = [x for x in records_for("CH-2") if x["business_ref"] == "CH2-S"][0]
check("迟到入库记为 pending（按发生时间重算）", late["status"] == "pending", str(late))
check("原因指出入库缺前序链", any("缺少此前完整的移交-签收链" in r for r in late["reasons"]), str(late["reasons"]))
# 再补移交（发生时间更早）：仍 pending（缺签收）
ev({"event_id": "CH2-T", "event_type": "transfer", "sample_code": "CH-2",
    "occurred_at": "2026-09-30T02:00:00Z", "transfer_no": "CH2-TR",
    "from_custodian": "张三", "to_custodian": "李四"})
# 后补签收，链闭合：重算为 valid
ev({"event_id": "CH2-R", "event_type": "receive", "sample_code": "CH-2",
    "occurred_at": "2026-09-30T03:00:00Z", "transfer_no": "CH2-TR",
    "receiver": "李四", "seal_no": "SEAL-1"})
last = records_for("CH-2")[-1]
check("补签后记录重算为 valid（迟到事件按发生时间重算）", last["status"] == "valid", str(last))
check("游标顺序=提交顺序而非发生时间",
      [x["business_ref"] for x in records_for("CH-2")] == ["CH-2", "CH2-S", "CH2-T", "CH2-R"])

# ---------------------------------------------------------------- 4. 幂等重试/重复核定/失败请求不记录
print("4) 幂等重试、重复核定与失败请求不生成记录")
n0 = len(feed()["items"])
# 登记同内容幂等 200
r = reg("CH-1")
check("登记幂等 200", r.status_code == 200 and r.json()["idempotent"] is True, r.text)
# 事件同内容幂等 200
r = ev({"event_id": "CH1-T", "event_type": "transfer", "sample_code": "CH-1",
        "occurred_at": "2026-09-30T02:00:00Z", "transfer_no": "CH1-TR",
        "from_custodian": "张三", "to_custodian": "李四"})
check("事件幂等 200", r.status_code == 200 and r.json()["idempotent"] is True, r.text)
check("幂等重试不新增记录", len(feed()["items"]) == n0)

# 失败请求：事件异内容冲突 409
r = ev({"event_id": "CH1-T", "event_type": "transfer", "sample_code": "CH-1",
        "occurred_at": "2026-09-30T02:30:00Z", "transfer_no": "CH1-TR",
        "from_custodian": "张三", "to_custodian": "李四"})
check("异内容冲突 409", r.status_code == 409, r.text)
# 失败请求：向不存在样本上报 404
r = ev({"event_id": "CH-NOPE", "event_type": "storage", "sample_code": "NO-SUCH-CODE",
        "occurred_at": "2026-09-30T04:00:00Z", "custodian": "实验室"})
check("样本不存在 404", r.status_code == 404, r.text)
# 失败请求：结构非法 422
r = ev({"event_id": "", "event_type": "transfer", "sample_code": "CH-1",
        "occurred_at": "2026-09-30T02:00:00Z"})
check("结构校验 422", r.status_code == 422)
check("失败请求不新增记录", len(feed()["items"]) == n0)

# 核定成功记录一次；非有效链核定 422 不记录；重复核定不记录
r = client.post(f"{API}/samples/CH-1/approve", json={"approver": "质量主管", "note": "准予入库"})
check("有效链核定 200", r.status_code == 200, r.text)
recs1 = records_for("CH-1")
appr = recs1[-1]
check("核定记录 operation_type=approval", appr["operation_type"] == "approval", str(appr))
check("核定 business_ref=样本编号", appr["business_ref"] == "CH-1", str(appr))
check("核定后 status=valid", appr["status"] == "valid", str(appr))
check("核定信息快照完整",
      appr["approval"] and appr["approval"]["approved_by"] == "质量主管"
      and appr["approval"]["note"] == "准予入库" and appr["approval"]["approved_storage_at"],
      str(appr["approval"]))
check("核定明细带 approver", appr["detail"] == {"approver": "质量主管"}, str(appr["detail"]))
n1 = len(feed()["items"])
r = client.post(f"{API}/samples/CH-1/approve", json={"approver": "质量主管", "note": "重复核定"})
check("重复核定仍 200（幂等）", r.status_code == 200 and r.json()["approval"]["approved"] is True, r.text)
check("重复核定不新增记录", len(feed()["items"]) == n1)

reg("CH-BAD")
ev({"event_id": "CHB-T", "event_type": "transfer", "sample_code": "CH-BAD",
    "occurred_at": "2026-09-30T02:00:00Z", "transfer_no": "CHB-TR",
    "from_custodian": "张三", "to_custodian": "李四"})
n2 = len(feed()["items"])
r = client.post(f"{API}/samples/CH-BAD/approve", json={"approver": "质量主管"})
check("非有效链核定 422", r.status_code == 422 and r.json()["error"]["code"] == "approval_rejected", r.text)
check("核定被拒不新增记录", len(feed()["items"]) == n2)
# 核定后改写被拒且不记录
r = ev({"event_id": "CH1-LATE", "event_type": "seal_check", "sample_code": "CH-1",
        "occurred_at": "2026-09-30T05:00:00Z", "checked_seal_no": "SEAL-1"})
check("核定后写事件 409 history_locked",
      r.status_code == 409 and r.json()["error"]["code"] == "history_locked", r.text)
check("冻结后失败请求不新增记录", len(feed()["items"]) == n2)

# ---------------------------------------------------------------- 5. 批量移交：与批次同事务逐份记录
print("5) 车次批量移交：逐样本记录、business_ref=批次号、与批次同事务")
reg("CH-B1")
reg("CH-B2")
base = max_cursor()
batch_body = {
    "batch_id": "CH-BATCH-1", "handed_over_at": "2026-09-30T02:00:00Z",
    "from_custodian": "张三", "to_custodian": "李四",
    "items": [
        {"sample_code": "CH-B1", "event_id": "CHB1-E", "transfer_no": "CHB1-TR"},
        {"sample_code": "CH-B2", "event_id": "CHB2-E", "transfer_no": "CHB2-TR"},
    ],
}
r = client.post(f"{API}/transfer-batches", json=batch_body)
check("批次提交 201", r.status_code == 201, r.text)
new_items = [x for x in feed()["items"] if x["cursor"] > base]
check("批内每份恰一条、共 2 条", len(new_items) == 2, str(new_items))
check("游标连续（同事务逐份）",
      [x["cursor"] for x in new_items] == [base + 1, base + 2], str([x["cursor"] for x in new_items]))
check("operation_type=batch_transfer",
      all(x["operation_type"] == "batch_transfer" for x in new_items), str(new_items))
check("business_ref=批次号", all(x["business_ref"] == "CH-BATCH-1" for x in new_items), str(new_items))
check("ref_event_type=transfer", all(x["ref_event_type"] == "transfer" for x in new_items))
check("按提交顺序归属样本",
      [x["sample_code"] for x in new_items] == ["CH-B1", "CH-B2"], str(new_items))
check("明细带份内事件号/移交单号",
      [x["detail"] for x in new_items]
      == [{"event_id": "CHB1-E", "transfer_no": "CHB1-TR"},
          {"event_id": "CHB2-E", "transfer_no": "CHB2-TR"}],
      str([x["detail"] for x in new_items]))
check("提交后结论 pending（缺签收）", all(x["status"] == "pending" for x in new_items))

# 幂等重试不新增
n3 = len(feed()["items"])
r = client.post(f"{API}/transfer-batches", json=batch_body)
check("批次幂等 200", r.status_code == 200 and r.json()["idempotent"] is True, r.text)
check("批次幂等重试不新增记录", len(feed()["items"]) == n3)
# 整批失败（事件号占用 + 含已核定样本两类）不新增任何记录
bad = {
    "batch_id": "CH-BATCH-FAIL", "handed_over_at": "2026-09-30T02:00:00Z",
    "from_custodian": "张三", "to_custodian": "李四",
    "items": [
        {"sample_code": "CH-B1", "event_id": "CHB1-E", "transfer_no": "X-1"},
    ],
}
r = client.post(f"{API}/transfer-batches", json=bad)
check("占用事件号整批 409", r.status_code == 409, r.text)
check("整批失败零记录（无批次头亦无变更）", len(feed()["items"]) == n3
      and client.get(f"{API}/transfer-batches/CH-BATCH-FAIL").status_code == 404)

# ---------------------------------------------------------------- 6. 作废/替换纠错记录
print("6) 作废/替换纠错：按最终有效链重算后逐次记录")
# CH-B2 目前仅有批量移交（pending）；作废该移交 → 回到缺入库，保管人变采集人
r = client.post(f"{API}/corrections", json={
    "correction_id": "CH-FIX-1", "sample_code": "CH-B2", "target_event_id": "CHB2-E",
    "action": "void", "operator": "质控员", "reason": "误移交，作废"})
check("作废 201", r.status_code == 201, r.text)
vrec = records_for("CH-B2")[-1]
check("作废记录 operation_type=correction", vrec["operation_type"] == "correction", str(vrec))
check("business_ref=纠错号", vrec["business_ref"] == "CH-FIX-1", str(vrec))
check("作废明细完整",
      vrec["detail"] == {"action": "void", "target_event_id": "CHB2-E", "new_event_id": None},
      str(vrec["detail"]))
check("作废后保管人回到采集人", vrec["current_custodian"] == "张三", str(vrec))
check("作废后 pending", vrec["status"] == "pending", str(vrec))
# 同纠错号同内容重试 200 不记录
n4 = len(feed()["items"])
r = client.post(f"{API}/corrections", json={
    "correction_id": "CH-FIX-1", "sample_code": "CH-B2", "target_event_id": "CHB2-E",
    "action": "void", "operator": "质控员", "reason": "误移交，作废"})
check("纠错幂等 200", r.status_code == 200, r.text)
check("纠错幂等不新增记录", len(feed()["items"]) == n4)

# 替换：CH-B1 补一条封签检查再替换（保持可纠错：B1 未核定）
ev({"event_id": "CHB1-C", "event_type": "seal_check", "sample_code": "CH-B1",
    "occurred_at": "2026-09-30T03:30:00Z", "checked_seal_no": "WRONG", "seal_intact": False})
before_bad = records_for("CH-B1")[-1]
check("错误封签检查后 invalid", before_bad["status"] == "invalid", str(before_bad))
r = client.post(f"{API}/corrections", json={
    "correction_id": "CH-FIX-2", "sample_code": "CH-B1", "target_event_id": "CHB1-C",
    "action": "replace", "operator": "质控员", "reason": "扫码错误，整体替换",
    "new_event": {"event_id": "CHB1-C2", "event_type": "seal_check",
                  "occurred_at": "2026-09-30T03:30:00Z",
                  "checked_seal_no": "SEAL-1", "seal_intact": True}})
check("替换 201", r.status_code == 201, r.text)
rrec = records_for("CH-B1")[-1]
check("替换记录 business_ref=纠错号", rrec["business_ref"] == "CH-FIX-2", str(rrec))
check("替换明细带 new_event_id",
      rrec["detail"] == {"action": "replace", "target_event_id": "CHB1-C", "new_event_id": "CHB1-C2"},
      str(rrec["detail"]))
check("替换后按有效版本重算（不再 invalid）", rrec["status"] != "invalid", str(rrec["status"]))

# ---------------------------------------------------------------- 7. 查询接口：分页/空尾/非法参数
print("7) 游标增量查询：升序、next_cursor、空尾返回原游标、参数校验")
r = client.get(f"{API}/changes", params={"after": 0, "limit": 3})
check("查询 200", r.status_code == 200, r.text)
page1 = r.json()
check("首页 3 条", len(page1["items"]) == 3, str(len(page1["items"])))
check("游标升序", [x["cursor"] for x in page1["items"]] == sorted(x["cursor"] for x in page1["items"]))
check("首页游标 1..3", [x["cursor"] for x in page1["items"]] == [1, 2, 3], str(page1["items"]))
check("next_cursor=本页末条", page1["next_cursor"] == 3, str(page1))
check("has_more=true", page1["has_more"] is True)
check("回显 after/limit", page1["after"] == 0 and page1["limit"] == 3)
r = client.get(f"{API}/changes", params={"after": 3, "limit": 3})
page2 = r.json()
check("第二页游标严格大于 3 且连续",
      [x["cursor"] for x in page2["items"]][:3] == [4, 5, 6], str(page2["items"]))
# 全量翻页到尾部
tail = max_cursor()
r = client.get(f"{API}/changes", params={"after": tail, "limit": 50})
empty = r.json()
check("无后续：空列表", empty["items"] == [], str(empty))
check("无后续：next_cursor=原游标", empty["next_cursor"] == tail, str(empty))
check("无后续：has_more=false", empty["has_more"] is False)
# after 超过最大游标（游标号本身合法，只是尚无记录）→ 空 + 原游标
r = client.get(f"{API}/changes", params={"after": 999_999_999, "limit": 10})
check("超前游标返回空与原游标",
      r.json()["items"] == [] and r.json()["next_cursor"] == 999_999_999, r.text)
# 默认参数可用（first page）
r = client.get(f"{API}/changes")
check("默认参数 200 且默认 50 条上限", r.status_code == 200 and len(r.json()["items"]) <= 50, r.text)

for bad_after in ("abc", "-1", "1.5"):
    r = client.get(f"{API}/changes", params={"after": bad_after, "limit": 10})
    check(f"非法 after={bad_after} → 400 invalid_cursor",
          r.status_code == 400 and r.json()["error"]["code"] == "invalid_cursor", r.text)
for bad_limit, code in [("abc", "invalid_limit"), ("0", "invalid_limit"),
                        ("-2", "invalid_limit"), ("1001", "invalid_limit")]:
    r = client.get(f"{API}/changes", params={"after": 0, "limit": bad_limit})
    check(f"非法 limit={bad_limit} → 400 {code}",
          r.status_code == 400 and r.json()["error"]["code"] == code, r.text)
check("错误响应为统一信封",
      set(r.json()["error"].keys()) == {"code", "message", "details"}, r.text)

# ---------------------------------------------------------------- 8. 并发：游标顺序与提交可见顺序一致
print("8) 并发提交：游标顺序与提交可见顺序一致（可见集严格嵌套）")
reg("CC-A")
reg("CC-B")
start = max_cursor()
THREAD_N, ROUNDS = 2, 6
barrier = threading.Barrier(THREAD_N)
lock = threading.Lock()
views_by_tag: dict[str, list[set[str]]] = {"A": [], "B": []}
own_cursors: dict[str, list[int]] = {}


def conc_worker(tag, code):
    barrier.wait()
    mine: list[int] = []
    local_views: list[set[str]] = []
    for i in range(ROUNDS):
        eid = f"CC-E-{tag}-{i}"
        rr = ev({"event_id": eid, "event_type": "storage", "sample_code": code,
                 "occurred_at": f"2026-09-30T{5 + i:02d}:00:00Z", "custodian": "实验室"})
        assert rr.status_code in (201, 200), rr.text
        page = client.get(f"{API}/changes", params={"after": start, "limit": 1000}).json()["items"]
        visible = {x["business_ref"] for x in page
                   if x["operation_type"] == "event" and x["sample_code"] in ("CC-A", "CC-B")}
        cur = next(x["cursor"] for x in page if x["business_ref"] == eid)
        mine.append(cur)
        local_views.append(set(visible))
    with lock:
        own_cursors[tag] = mine
        views_by_tag[tag] = local_views


threads = [threading.Thread(target=conc_worker, args=("A", "CC-A")),
           threading.Thread(target=conc_worker, args=("B", "CC-B"))]
for t in threads:
    t.start()
for t in threads:
    t.join()
all_new = [x for x in feed()["items"]
           if x["cursor"] > start and x["sample_code"] in ("CC-A", "CC-B")
           and x["operation_type"] == "event"]
check(f"并发共 {THREAD_N * ROUNDS} 条事件记录，不多不少",
      len(all_new) == THREAD_N * ROUNDS, f"{len(all_new)}")
cursors = [x["cursor"] for x in all_new]
check("全局游标连续无空洞（提交即占号、失败不占号）",
      cursors == list(range(min(cursors), max(cursors) + 1)), str(cursors))
for tag in ("A", "B"):
    check(f"线程 {tag} 自有游标随其提交次序严格递增",
          own_cursors[tag] == sorted(own_cursors[tag]) and len(set(own_cursors[tag])) == ROUNDS,
          str(own_cursors[tag]))
# 同一线程在各提交完成时刻读到的“已提交可见集”必须严格嵌套（游标序=提交可见序，
# 后提交的游标不会从可见序中消失）；已提交游标亦必对后续读可见。
for tag in ("A", "B"):
    vs = views_by_tag[tag]
    nested = all(vs[i].issubset(vs[i + 1]) for i in range(len(vs) - 1))
    own = {f"CC-E-{tag}-{i}" for i in range(ROUNDS)}
    check(f"线程 {tag} 各时刻已提交可见集严格嵌套且自身提交均可见",
          nested and own <= vs[-1], str([len(v) for v in vs]))
# 跨线程：所有游标两两可比（连续整数），即存在全局一致的提交顺序
check("两线程游标无重叠（全局唯一提交点）",
      set(own_cursors["A"]).isdisjoint(set(own_cursors["B"])))
# 每条已提交记录最终都对后续读可见
check("最终所有并发事件均可见",
      {f"CC-E-{t}-{i}" for t in ("A", "B") for i in range(ROUNDS)}
      <= {x["business_ref"] for x in all_new})

# ---------------------------------------------------------------- 9. 旧库升级：当前状态基线
print("9) 旧库升级：现存样本各记一条 baseline，不伪造历史操作")
old_path = os.path.join(_tmp, "test_changes_old.db")
if os.path.exists(old_path):
    os.remove(old_path)

# 直接按“旧库”结构建表：无 change_record / event_correction / transfer_batch，
# custody_event 也没有纠错/批次补列（模拟本轮功能上线前的库）。
legacy = sqlite3.connect(old_path)
legacy.executescript("""
CREATE TABLE sample (
    id INTEGER PRIMARY KEY AUTOINCREMENT, sample_code TEXT NOT NULL UNIQUE,
    collected_at TEXT NOT NULL, collector TEXT NOT NULL, seal_no TEXT NOT NULL,
    content_hash TEXT NOT NULL, approved_storage_at TEXT, approved_by TEXT,
    approve_note TEXT, created_at TEXT NOT NULL
);
CREATE TABLE custody_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT, sample_id INTEGER NOT NULL,
    event_id TEXT NOT NULL UNIQUE, event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL, payload_json TEXT NOT NULL,
    content_hash TEXT NOT NULL, created_at TEXT NOT NULL
);
""")
legacy.execute(
    "INSERT INTO sample (sample_code, collected_at, collector, seal_no, content_hash,"
    " approved_storage_at, approved_by, approve_note, created_at)"
    " VALUES ('OLD-1','2026-09-30T01:00:00Z','张三','SEAL-O1','h1',"
    " '2026-09-29T10:00:00Z','老质量主管','旧核定备注','2026-09-29T09:00:00Z')")
legacy.execute(
    "INSERT INTO sample (sample_code, collected_at, collector, seal_no, content_hash, created_at)"
    " VALUES ('OLD-2','2026-09-30T01:00:00Z','王五','SEAL-O2','h2','2026-09-29T09:00:00Z')")
old_events = [
    (1, "OLD-T", "transfer", "2026-09-30T02:00:00Z",
     {"transfer_no": "OLD-TR", "from_custodian": "张三", "to_custodian": "李四"}),
    (1, "OLD-R", "receive", "2026-09-30T03:00:00Z",
     {"transfer_no": "OLD-TR", "receiver": "李四", "seal_no": "SEAL-O1"}),
    (1, "OLD-S", "storage", "2026-09-30T04:00:00Z", {"custodian": "实验室"}),
]
for sid, eid, et, at, payload in old_events:
    legacy.execute(
        "INSERT INTO custody_event (sample_id, event_id, event_type, occurred_at,"
        " payload_json, content_hash, created_at) VALUES (?,?,?,?,?,?,?)",
        (sid, eid, et, at, json.dumps(payload, ensure_ascii=False), "h", at))
legacy.commit()
legacy.close()

# 把服务切到旧库执行升级（init_db 建表 + 补列 + 写基线）
config.DB_PATH = Path(old_path)
init_db()
try:
    with db_session() as conn:
        page = query_changes(conn, 0, 1000)
    baselines = page["items"]
    check("现存 2 条样本各 1 条基线", len(baselines) == 2, str(baselines))
    check("全部 operation_type=baseline（不伪造登记/事件/核定历史）",
          all(x["operation_type"] == "baseline" for x in baselines), str(baselines))
    check("business_ref=各样本编号",
          [x["business_ref"] for x in baselines] == ["OLD-1", "OLD-2"], str(baselines))
    check("游标按样本 id 升序 1,2", [x["cursor"] for x in baselines] == [1, 2], str(baselines))
    b1, b2 = baselines
    check("OLD-1 基线结论 valid（按现存数据重算）", b1["status"] == "valid", str(b1))
    check("OLD-1 基线带历史核定信息",
          b1["approval"] == {"approved_storage_at": "2026-09-29T10:00:00Z",
                              "approved_by": "老质量主管", "note": "旧核定备注"},
          str(b1["approval"]))
    check("OLD-1 基线保管人=实验室", b1["current_custodian"] == "实验室", str(b1))
    check("OLD-2 基线 pending（无事件）", b2["status"] == "pending" and b2["approval"] is None, str(b2))
    check("基线原因非空", all(x["reasons"] for x in baselines), str([x["reasons"] for x in baselines]))

    # 重复升级不重复写基线
    init_db()
    with db_session() as conn:
        again = query_changes(conn, 0, 1000)["items"]
    check("再次初始化幂等（仍只有 2 条基线）", len(again) == 2, str(len(again)))

    # 升级后的库正常工作：新登记追加 register 而非 baseline
    from app.schemas import SampleIn
    from app.service import register_sample
    with db_session(immediate=True) as conn:
        register_sample(conn, SampleIn(
            sample_code="OLD-3", collected_at="2026-09-30T01:00:00Z",
            collector="赵六", seal_no="SEAL-O3"))
    with db_session() as conn:
        last = query_changes(conn, 2, 10)["items"]
    check("升级后新操作记为 register",
          len(last) == 1 and last[0]["operation_type"] == "register"
          and last[0]["business_ref"] == "OLD-3", str(last))
finally:
    config.DB_PATH = Path(os.environ["DB_PATH"])

# 切回主库后 API 仍正常
check("升级校验后切回主库接口正常", client.get("/health").status_code == 200)
check("主库无 baseline 混入（新库由真实操作产生记录）",
      all(x["operation_type"] != "baseline" for x in feed()["items"]))

# ---------------------------------------------------------------- 10. 既有接口响应不变（冒烟）
print("10) 既有接口响应冒烟")
r = client.get(f"{API}/samples/CH-1")
check("样本详情保留 evaluation 字段集",
      r.status_code == 200 and {"status", "reasons", "current_custodian", "approved_storage_at"}
      <= set(r.json()["evaluation"].keys()), r.text)
r = client.get(f"{API}/samples/CH-1/chain")
check("chain 字段不变",
      {"status", "reasons", "current_custodian", "timeline", "corrections", "superseded_events"}
      <= set(r.json().keys()), r.text)
r = client.get(f"{API}/transfer-batches/CH-BATCH-1")
check("批次详情结构不变",
      r.status_code == 200 and {"batch", "total", "items"} <= set(r.json().keys())
      and "idempotent" not in r.json(), r.text)

print(f"\n结果: {PASS} 通过, {FAIL} 失败")
raise SystemExit(1 if FAIL else 0)
