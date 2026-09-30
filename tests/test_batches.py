"""端到端自测：同一交接车次多份样本批量移交（transfer batch）。

运行：python -m tests.test_batches
"""
import os
import tempfile
import threading

_tmp = tempfile.mkdtemp()
os.environ["DB_PATH"] = os.path.join(_tmp, "test_batches.db")
os.environ["SEED_ON_START"] = "false"

from fastapi.testclient import TestClient  # noqa: E402

from app.db import init_db  # noqa: E402
from app.main import app  # noqa: E402

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


def reg(code, collector="张三", seal="SEAL-1", when="2026-09-30T01:00:00Z"):
    return client.post(f"{API}/samples", json={
        "sample_code": code, "collected_at": when, "collector": collector, "seal_no": seal
    })


def ev(payload):
    return client.post(f"{API}/events", json=payload)


def chain(code):
    return client.get(f"{API}/samples/{code}/chain").json()


def batch_body(bid, items, when="2026-09-30T02:00:00Z", frm="张三", to="李四"):
    return {
        "batch_id": bid,
        "handed_over_at": when,
        "from_custodian": frm,
        "to_custodian": to,
        "items": [{"sample_code": c, "event_id": e, "transfer_no": no} for c, e, no in items],
    }


def submit(bid, items, **kw):
    return client.post(f"{API}/transfer-batches", json=batch_body(bid, items, **kw))


def getb(bid):
    return client.get(f"{API}/transfer-batches/{bid}")


def complete_valid(code, seal, t, r, s, custodian="李四"):
    """补 移交→签收→入库（transfer_no 复用既有批次移交单由调用方给定）。"""
    ev({"event_id": f"{r}-R", "event_type": "receive", "sample_code": code,
        "occurred_at": "2026-09-30T03:00:00Z", "transfer_no": t,
        "receiver": custodian, "seal_no": seal})
    ev({"event_id": f"{s}-S", "event_type": "storage", "sample_code": code,
        "occurred_at": "2026-09-30T04:00:00Z", "custodian": "实验室"})


# ---------------------------------------------------------------- 1. 结构校验
print("1) 批内样本/事件号重复、空批次、多余字段 → 422，不落数据")
r = submit("B-DUP-S", [("BS-1", "E1", "TR-1"), ("BS-1", "E2", "TR-2")])
check("批内样本重复 422", r.status_code == 422, r.text)
r = submit("B-DUP-E", [("BS-1", "E1", "TR-1"), ("BS-2", "E1", "TR-2")])
check("批内事件号重复 422", r.status_code == 422, r.text)
r = client.post(f"{API}/transfer-batches", json={
    "batch_id": "B-EMPTY", "handed_over_at": "2026-09-30T02:00:00Z",
    "from_custodian": "张三", "to_custodian": "李四", "items": []})
check("空批次 422", r.status_code == 422, r.text)
r = client.post(f"{API}/transfer-batches", json={
    "batch_id": "B-X", "handed_over_at": "2026-09-30T02:00:00Z",
    "from_custodian": "张三", "to_custodian": "李四",
    "items": [{"sample_code": "BS-1", "event_id": "E1", "transfer_no": "TR-1", "extra": 1}]})
check("条目多余字段 422", r.status_code == 422, r.text)
r = client.post(f"{API}/transfer-batches", json={
    "batch_id": "B-X", "handed_over_at": "2026-09-30T02:00:00Z",
    "from_custodian": "张三", "to_custodian": "李四",
    "items": [{"sample_code": "BS-1", "event_id": "E1", "transfer_no": "TR-1"}],
    "unexpected": True})
check("批次多余字段 422", r.status_code == 422, r.text)
r = client.get(f"{API}/transfer-batches")
check("422 不落任何批次", r.status_code == 200 and r.json()["total"] == 0, r.text)

# ---------------------------------------------------------------- 2. 样本不存在：整批失败
print("2) 样本不存在 → 404，整批不留事件/批次")
reg("BS-1")
r = submit("B-MISS", [("BS-1", "MISS-E1", "TR-M1"), ("NO-SUCH", "MISS-E2", "TR-M2")])
check("404 sample_not_found", r.status_code == 404 and r.json()["error"]["code"] == "sample_not_found", r.text)
check("错误详情列出缺失样本", "NO-SUCH" in r.json()["error"]["details"]["missing_sample_codes"], r.text)
r = getb("B-MISS")
check("批次不可查（未留痕）", r.status_code == 404, r.text)
c = chain("BS-1")
check("同批其他样本未落事件", all(e["event_id"] != "MISS-E1" for e in c["timeline"]), str(c["timeline"]))

# ---------------------------------------------------------------- 3. 事件号已占用：整批失败
print("3) 任一事件号已占用 → 409 event_conflict，整批回滚")
reg("BS-2")
ev({"event_id": "OCC-1", "event_type": "storage", "sample_code": "BS-1",
    "occurred_at": "2026-09-30T05:00:00Z", "custodian": "实验室"})
r = submit("B-OCC", [("BS-1", "OCC-1", "TR-O1"), ("BS-2", "OCC-E2", "TR-O2")])
check("409 event_conflict", r.status_code == 409 and r.json()["error"]["code"] == "event_conflict", r.text)
check("详情带占用事件号", "OCC-1" in r.json()["error"]["details"]["occupied_event_ids"], r.text)
check("批次未留痕", getb("B-OCC").status_code == 404)
c = chain("BS-2")
check("同批其他事件未落库", all(e["event_id"] != "OCC-E2" for e in c["timeline"]), str(c["timeline"]))

# ---------------------------------------------------------------- 4. 正常提交
print("4) 正常批次：逐份 transfer 落库，返回逐份当前结论与归属")
r = submit("B-OK", [("BS-1", "OK-E1", "TR-OK1"), ("BS-2", "OK-E2", "TR-OK2")])
check("提交 201", r.status_code == 201, r.text)
b = r.json()
check("批次头正确",
      b["batch"]["batch_id"] == "B-OK" and b["batch"]["from_custodian"] == "张三"
      and b["batch"]["to_custodian"] == "李四" and b["total"] == 2, str(b["batch"]))
check("idempotent=false", b["idempotent"] is False)
statuses = {i["sample_code"]: i["evaluation"]["status"] for i in b["items"]}
check("两份均 pending（已移交未签收/未入库）",
      statuses == {"BS-1": "pending", "BS-2": "pending"}, str(statuses))
check("逐份带原事件号/移交单号/有效标记",
      {(i["sample_code"], i["event_id"], i["transfer_no"], i["event_effective"]) for i in b["items"]}
      == {("BS-1", "OK-E1", "TR-OK1", True), ("BS-2", "OK-E2", "TR-OK2", True)},
      str(b["items"]))
check("逐份原因含缺签收",
      all(any("缺少签收" in x for x in i["evaluation"]["reasons"]) for i in b["items"]),
      str([i["evaluation"]["reasons"] for i in b["items"]]))
# 证据链中是既有 transfer 事件且带批次归属
c = chain("BS-1")
t1 = [e for e in c["timeline"] if e["event_id"] == "OK-E1"][0]
check("事件类型为 transfer 且 payload 为移交三字段",
      t1["event_type"] == "transfer"
      and t1["payload"] == {"transfer_no": "TR-OK1", "from_custodian": "张三", "to_custodian": "李四"},
      str(t1))
check("时间线事件带 batch_id=B-OK", t1["batch_id"] == "B-OK", str(t1.get("batch_id")))
check("统一移交时间", t1["occurred_at"] == "2026-09-30T02:00:00Z")

# ---------------------------------------------------------------- 5. 幂等与冲突
print("5) 同批次号同内容重试 200；异内容 409 batch_conflict")
r = submit("B-OK", [("BS-1", "OK-E1", "TR-OK1"), ("BS-2", "OK-E2", "TR-OK2")])
check("同内容重试 200 且 idempotent", r.status_code == 200 and r.json()["idempotent"] is True, r.text)
r = submit("B-OK", [("BS-1", "OK-E1", "TR-OK1"), ("BS-2", "OK-E3", "TR-OK3")])
check("换事件号 409", r.status_code == 409 and r.json()["error"]["code"] == "batch_conflict", r.text)
r = submit("B-OK", [("BS-1", "OK-E1", "TR-OK1"), ("BS-2", "OK-E2", "TR-OK2")], to="王五")
check("换接收人 409", r.status_code == 409 and r.json()["error"]["code"] == "batch_conflict", r.text)
r = submit("B-OK", [("BS-1", "OK-E1", "TR-OK1"), ("BS-2", "OK-E2", "TR-OK2")],
           when="2026-09-30T02:30:00Z")
check("换移交时间 409", r.status_code == 409 and r.json()["error"]["code"] == "batch_conflict", r.text)
r = submit("B-OK", [("BS-1", "OK-E1", "TR-OK1")])
check("份数不同 409", r.status_code == 409 and r.json()["error"]["code"] == "batch_conflict", r.text)
r = submit("B-OK", [("BS-1", "OK-E1", "TR-OK1"), ("BS-2", "OK-E2", "TR-DIFF")])
check("换移交单号 409", r.status_code == 409 and r.json()["error"]["code"] == "batch_conflict", r.text)
r = submit("B-OK", [("BS-2", "OK-E2", "TR-OK2"), ("BS-1", "OK-E1", "TR-OK1")])
check("提交顺序不同（内容相同）仍 200 幂等", r.status_code == 200 and r.json()["idempotent"] is True, r.text)
# 冲突/422 不占用批次号：此前 422 的 B-DUP-S 号可重新以合法内容提交（样本先补齐）
reg("BS-3")
r = submit("B-DUP-S", [("BS-3", "DS-E1", "TR-DS1")])
check("此前校验失败的批次号可重新使用 201", r.status_code == 201, r.text)

# ---------------------------------------------------------------- 6. 既有单事件接口兼容性
print("6) 批次内事件经既有单事件接口：同内容 200、异内容 409（行为不变）")
r = ev({"event_id": "OK-E1", "event_type": "transfer", "sample_code": "BS-1",
        "occurred_at": "2026-09-30T02:00:00Z", "transfer_no": "TR-OK1",
        "from_custodian": "张三", "to_custodian": "李四"})
check("同 event_id 同内容 200", r.status_code == 200 and r.json()["idempotent"] is True, r.text)
r = ev({"event_id": "OK-E1", "event_type": "transfer", "sample_code": "BS-1",
        "occurred_at": "2026-09-30T02:01:00Z", "transfer_no": "TR-OK1",
        "from_custodian": "张三", "to_custodian": "李四"})
check("同 event_id 异内容 409 event_conflict",
      r.status_code == 409 and r.json()["error"]["code"] == "event_conflict", r.text)

# 反向：单事件接口已写的事件，批次同号同内容也视为占用（批次不允许复用既有事件号）
reg("BS-4")
ev({"event_id": "PRE-1", "event_type": "transfer", "sample_code": "BS-4",
    "occurred_at": "2026-09-30T02:00:00Z", "transfer_no": "TR-PRE",
    "from_custodian": "张三", "to_custodian": "李四"})
r = submit("B-PRE", [("BS-4", "PRE-1", "TR-PRE")])
check("批次复用单事件接口已写事件号 → 409 event_conflict（整批）",
      r.status_code == 409 and r.json()["error"]["code"] == "event_conflict", r.text)
check("被拒批次未留痕", getb("B-PRE").status_code == 404)

# ---------------------------------------------------------------- 7. 已核定样本
print("7) 已核定：新批次整批失败；既有批次同内容重试仍幂等，异内容仍冲突")
complete_valid("BS-2", "SEAL-1", "TR-OK2", "OK-E2", "BS2")
r = client.post(f"{API}/samples/BS-2/approve", json={"approver": "质量主管"})
check("BS-2 核定 200", r.status_code == 200, r.text)
r = submit("B-LATE", [("BS-2", "LATE-E1", "TR-L1"), ("BS-3", "LATE-E2", "TR-L2")])
check("含已核定样本 → 409 history_locked 整批失败",
      r.status_code == 409 and r.json()["error"]["code"] == "history_locked", r.text)
check("失败批次未留痕", getb("B-LATE").status_code == 404)
check("失败批次事件未落库",
      all(e["event_id"] != "LATE-E2" for e in chain("BS-3")["timeline"]))
# B-OK 含已核定的 BS-2：同内容重试仍 200（即使样本已核定仍幂等）
r = submit("B-OK", [("BS-1", "OK-E1", "TR-OK1"), ("BS-2", "OK-E2", "TR-OK2")])
check("核定后同批次同内容重试 200", r.status_code == 200 and r.json()["idempotent"] is True, r.text)
b = r.json()
bs2 = [i for i in b["items"] if i["sample_code"] == "BS-2"][0]
check("幂等重试逐份返回最新结论 valid", bs2["evaluation"]["status"] == "valid", str(bs2["evaluation"]["status"]))
check("核定后原事件仍有效", bs2["event_effective"] is True)
check("逐份结论带核定时间", bs2["evaluation"]["approved_storage_at"] is not None)
r = submit("B-OK", [("BS-1", "OK-E1", "TR-OK1"), ("BS-2", "OK-E2", "TR-OK2")], frm="赵六")
check("核定后同批次异内容仍 409", r.status_code == 409 and r.json()["error"]["code"] == "batch_conflict", r.text)

# ---------------------------------------------------------------- 8. 批次查询随纠错/核定变化，原始归属可追溯
print("8) 批次查询返回逐份当前结论与原事件是否仍有效；纠错只改变有效链")
reg("BS-5", seal="SEAL-5")
submit("B-FIX", [("BS-5", "FX-E1", "TR-FX1")])
# 作废批次产生的移交
r = client.post(f"{API}/corrections", json={
    "correction_id": "FIX-1", "sample_code": "BS-5", "target_event_id": "FX-E1",
    "action": "void", "operator": "质控员", "reason": "作废批量移交事件"})
check("作废批次事件 201", r.status_code == 201, r.text)
b = getb("B-FIX").json()
it = b["items"][0]
check("批次查询 event_effective=False、superseded_by 指向纠错",
      it["event_effective"] is False and it["superseded_by"] == "FIX-1", str(it))
check("作废后逐份结论 pending（无有效事件）", it["evaluation"]["status"] == "pending")
c = chain("BS-5")
check("有效时间线不含原事件", all(e["event_id"] != "FX-E1" for e in c["timeline"]))
sup = [e for e in c["superseded_events"] if e["event_id"] == "FX-E1"][0]
check("被作废事件仍带批次归属 batch_id=B-FIX", sup["batch_id"] == "B-FIX", str(sup.get("batch_id")))
check("纠错留痕在证据链可查", [x["correction_id"] for x in c["corrections"]] == ["FIX-1"])

# 替换批次事件：新事件不属于批次，原事件归属保留；再作废新事件不影响批次条目数
reg("BS-6", seal="SEAL-6")
submit("B-RP", [("BS-6", "RP-E1", "TR-RP1")])
r = client.post(f"{API}/corrections", json={
    "correction_id": "FIX-2", "sample_code": "BS-6", "target_event_id": "RP-E1",
    "action": "replace", "operator": "质控员", "reason": "移交时间修正",
    "new_event": {"event_id": "RP-E2", "event_type": "transfer",
                  "occurred_at": "2026-09-30T02:10:00Z",
                  "transfer_no": "TR-RP1", "from_custodian": "张三", "to_custodian": "李四"}})
check("替换批次事件 201", r.status_code == 201, r.text)
b = getb("B-RP").json()
check("批次仍 1 份、原事件已失效",
      b["total"] == 1 and b["items"][0]["event_id"] == "RP-E1"
      and b["items"][0]["event_effective"] is False, str(b))
c = chain("BS-6")
new_ev = [e for e in c["timeline"] if e["event_id"] == "RP-E2"][0]
old_ev = [e for e in c["superseded_events"] if e["event_id"] == "RP-E1"][0]
check("替换新事件无批次归属", new_ev["batch_id"] is None, str(new_ev.get("batch_id")))
check("替换新事件带 replaces_event_id", new_ev["replaces_event_id"] == "RP-E1")
check("原事件归属仍为 B-RP", old_ev["batch_id"] == "B-RP", str(old_ev.get("batch_id")))
# 再作废替换产生的新事件：批次查询仍指向原事件（失效），批次头不变
r = client.post(f"{API}/corrections", json={
    "correction_id": "FIX-3", "sample_code": "BS-6", "target_event_id": "RP-E2",
    "action": "void", "operator": "质控员", "reason": "连新版本一并作废"})
check("作废替换产生的新事件 201", r.status_code == 201, r.text)
b = getb("B-RP").json()
check("连续纠错后批次原始归属仍可查（1 份、原号）",
      b["total"] == 1 and b["items"][0]["event_id"] == "RP-E1"
      and b["items"][0]["event_effective"] is False, str(b["items"]))

# 核定后批次查询仍可见
r = client.post(f"{API}/corrections", json={
    "correction_id": "FIX-4", "sample_code": "BS-5", "target_event_id": "FX-E1",
    "action": "void", "operator": "质控员", "reason": "核定后试探"})
# 该事件已作废 → event_not_effective（非 history_locked），仅验证不产生新留痕
check("对已失效事件再纠错 409", r.status_code == 409, r.text)

# ---------------------------------------------------------------- 9. 批次列表与 404
print("9) 批次列表 / 404")
r = client.get(f"{API}/transfer-batches")
lst = r.json()
ids = {x["batch_id"] for x in lst["items"]}
check("列表含全部成功批次",
      {"B-OK", "B-FIX", "B-RP", "B-DUP-S"} <= ids
      and not ({"B-MISS", "B-OCC", "B-PRE", "B-LATE"} & ids), str(ids))
bok = [x for x in lst["items"] if x["batch_id"] == "B-OK"][0]
check("列表条目带份数与批次头",
      bok["item_count"] == 2 and bok["handed_over_at"] == "2026-09-30T02:00:00Z"
      and bok["from_custodian"] == "张三" and bok["to_custodian"] == "李四", str(bok))
r = getb("NO-SUCH-BATCH")
check("未知批次 404 batch_not_found",
      r.status_code == 404 and r.json()["error"]["code"] == "batch_not_found", r.text)

# ---------------------------------------------------------------- 10. 并发：批次提交 vs 核定按提交顺序
print("10) 并发：批次提交与核定按已提交顺序判定")
for code, seal in [("CC-1", "SEAL-C1"), ("CC-2", "SEAL-C2")]:
    reg(code, seal=seal)
# CC-1 先做成 valid（核定方可成功）
ev({"event_id": "CC-T0", "event_type": "transfer", "sample_code": "CC-1",
    "occurred_at": "2026-09-30T02:00:00Z", "transfer_no": "TR-CC0",
    "from_custodian": "张三", "to_custodian": "李四"})
ev({"event_id": "CC-R0", "event_type": "receive", "sample_code": "CC-1",
    "occurred_at": "2026-09-30T03:00:00Z", "transfer_no": "TR-CC0",
    "receiver": "李四", "seal_no": "SEAL-C1"})
ev({"event_id": "CC-S0", "event_type": "storage", "sample_code": "CC-1",
    "occurred_at": "2026-09-30T04:00:00Z", "custodian": "实验室"})

outcomes: dict[str, int] = {}
barrier = threading.Barrier(2)


def batch_worker():
    barrier.wait()
    outcomes["batch"] = submit("B-RACE", [("CC-1", "RC-E1", "TR-RC1"), ("CC-2", "RC-E2", "TR-RC2")]).status_code


def approve_worker():
    barrier.wait()
    outcomes["approve"] = client.post(f"{API}/samples/CC-1/approve",
                                      json={"approver": "质量主管"}).status_code


t1 = threading.Thread(target=batch_worker)
t2 = threading.Thread(target=approve_worker)
t1.start(); t2.start(); t1.join(); t2.join()
# 批次先提交 → 201（CC-1 出现新在途移交 → 非 valid），核定基于最终链 422；
# 核定先提交 → 200 冻结，批次见已核定样本 409。
if outcomes.get("batch") == 201:
    check("批次先提交：201", True)
    check("核定基于批次后最终链拒绝 422", outcomes.get("approve") == 422, str(outcomes))
    check("最终链非 valid", chain("CC-1")["status"] != "valid"
          and chain("CC-1")["status"] != "valid")
    # 批次可查、两份都在
    b = getb("B-RACE").json()
    check("批次已落库 2 份", b["total"] == 2, str(b["total"]))
else:
    check("核定先提交：200", outcomes.get("approve") == 200, str(outcomes))
    check("批次含已核定样本被拒 409 history_locked", outcomes.get("batch") == 409, str(outcomes))
    check("落败批次不留痕", getb("B-RACE").status_code == 404)
    check("CC-1 已核定冻结", chain("CC-1")["approved_storage_at"] is not None)

# ---------------------------------------------------------------- 11. 并发：两个批次抢同一事件号
print("11) 并发：两批次抢同一事件号，恰一批成功、落败方无残留")
for code, seal in [("CC-3", "SEAL-C3"), ("CC-4", "SEAL-C4")]:
    reg(code, seal=seal)
results: dict[str, int] = {}
barrier2 = threading.Barrier(2)


def race_batch(tag, bid, eids):
    barrier2.wait()
    results[tag] = submit(bid, [("CC-3", eids[0], f"TR-{eids[0]}"), ("CC-4", eids[1], f"TR-{eids[1]}")]).status_code


ta = threading.Thread(target=race_batch, args=("A", "B-CX1", ("CX-E0", "CX-E1")))
tb = threading.Thread(target=race_batch, args=("B", "B-CX2", ("CX-E0", "CX-E2")))
ta.start(); tb.start(); ta.join(); tb.join()
check("恰一批 201、另一批 409", sorted(results.values()) == [201, 409], str(results))
r = client.get(f"{API}/transfer-batches")
wins = {x["batch_id"] for x in r.json()["items"]} & {"B-CX1", "B-CX2"}
check("仅一个批次头留痕", wins in ({"B-CX1"}, {"B-CX2"}), str(wins))
b = getb(next(iter(wins))).json()
check("成功批次两份事件齐全", {i["event_id"] for i in b["items"]} in
      ({"CX-E0", "CX-E1"}, {"CX-E0", "CX-E2"}), str([i["event_id"] for i in b["items"]]))
loser_ev = "CX-E2" if wins == {"B-CX1"} else "CX-E1"
check("落败批次独有事件无残留",
      all(e["event_id"] != loser_ev for e in chain("CC-4")["timeline"]), loser_ev)

# ---------------------------------------------------------------- 12. 既有接口行为不变（冒烟）
print("12) 既有接口冒烟：登记/事件/纠错/核定行为不变")
check("health 200", client.get("/health").status_code == 200)
r = client.get(f"{API}/samples")
check("样本列表可查", r.status_code == 200 and r.json()["total"] >= 10, str(r.json().get("total")))
check("未知样本仍 404", client.get(f"{API}/samples/NOPE").status_code == 404)
c = chain("BS-1")
for key in ("status", "reasons", "current_custodian", "pending_transfer", "has_storage",
            "approved_storage_at", "timeline", "corrections", "superseded_events"):
    check(f"chain 保留既有字段 {key}", key in c)
# 时间线条目新增 batch_id 但旧字段全保留
for e in c["timeline"]:
    for key in ("event_id", "event_type", "occurred_at", "recorded_at", "payload", "replaces_event_id"):
        check(f"timeline 条目保留字段 {key}", key in e)
        break

print(f"\n结果: {PASS} 通过, {FAIL} 失败")
raise SystemExit(1 if FAIL else 0)
