"""零依赖（仅标准库）的 HTTP API 调用示例：完整演示登记→移交→签收→检查→入库→查询→核定。

使用前：
    pip install 无额外要求（urllib 为 Python 标准库）
    先启动服务：docker compose up -d   或   python -m uvicorn app.main:app --port 8080
运行：
    python examples/call_api.py
"""
import json
import urllib.error
import urllib.request
import uuid

BASE = "http://127.0.0.1:8080/api/v1"


def call(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        BASE + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode("utf-8"))


def show(title: str, status: int, resp: dict, *keys: str) -> None:
    print(f"\n--- {title} [HTTP {status}] ---")
    out = resp
    for k in keys:
        out = out.get(k, {}) if isinstance(out, dict) else {}
    print(json.dumps(out, ensure_ascii=False, indent=2))


def call_via_query(path: str) -> tuple[int, dict]:
    """GET 带查询串的小工具（path 已含查询参数）。"""
    req = urllib.request.Request(BASE + path)
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode("utf-8"))


def main() -> None:
    # 每次运行使用独立编号，脚本可任意重复执行
    run_id = uuid.uuid4().hex[:8]
    code = f"API-DEMO-{run_id}"

    # 1) 样本登记：采集时间、采集人（初始保管人）、封签号
    s, r = call("POST", "/samples", {
        "sample_code": code,
        "collected_at": "2026-09-30T01:00:00Z",
        "collector": "王采集",
        "seal_no": "SEAL-DEMO-1",
    })
    show("样本登记", s, r, "evaluation")

    # 2) 移交：必须由当前保管人发起，指定移交单号与接收人
    s, r = call("POST", "/events", {
        "event_id": f"API-EVT-T1-{run_id}", "event_type": "transfer", "sample_code": code,
        "occurred_at": "2026-09-30T02:00:00Z",
        "transfer_no": "TR-DEMO-1", "from_custodian": "王采集", "to_custodian": "李运输",
    })
    show("移交（签收前=待补证）", s, r, "evaluation")

    # 3) 签收：单号/接收人/封签号三匹配，且晚于移交
    s, r = call("POST", "/events", {
        "event_id": f"API-EVT-R1-{run_id}", "event_type": "receive", "sample_code": code,
        "occurred_at": "2026-09-30T04:00:00Z",
        "transfer_no": "TR-DEMO-1", "receiver": "李运输", "seal_no": "SEAL-DEMO-1",
    })
    show("签收", s, r, "evaluation")

    # 4) 封签检查
    s, r = call("POST", "/events", {
        "event_id": f"API-EVT-C1-{run_id}", "event_type": "seal_check", "sample_code": code,
        "occurred_at": "2026-09-30T04:30:00Z",
        "checked_seal_no": "SEAL-DEMO-1", "seal_intact": True,
    })
    show("封签检查", s, r, "evaluation")

    # 5) 实验室入库（缺此节点结论为待补证）
    s, r = call("POST", "/events", {
        "event_id": f"API-EVT-S1-{run_id}", "event_type": "storage", "sample_code": code,
        "occurred_at": "2026-09-30T05:00:00Z",
        "custodian": "实验室", "storage_location": "冷库A-01",
    })
    show("实验室入库（此时有效）", s, r, "evaluation")

    # 6) 幂等重试：同事件号同内容 → 200
    s, r = call("POST", "/events", {
        "event_id": f"API-EVT-T1-{run_id}", "event_type": "transfer", "sample_code": code,
        "occurred_at": "2026-09-30T02:00:00Z",
        "transfer_no": "TR-DEMO-1", "from_custodian": "王采集", "to_custodian": "李运输",
    })
    show("同事件号同内容重试（幂等）", s, r, "idempotent")

    # 7) 冲突：同事件号异内容 → 409
    s, r = call("POST", "/events", {
        "event_id": f"API-EVT-T1-{run_id}", "event_type": "transfer", "sample_code": code,
        "occurred_at": "2026-09-30T02:30:00Z",  # 时间被改动
        "transfer_no": "TR-DEMO-1", "from_custodian": "王采集", "to_custodian": "李运输",
    })
    show("同事件号异内容（冲突）", s, r)

    # 8) 查询完整证据链（按发生时间排列）
    s, r = call("GET", f"/samples/{code}/chain")
    print(f"\n--- 证据链 [HTTP {s}] ---")
    print("结论:", r["status"], "| 当前保管人:", r["current_custodian"])
    for e in r["timeline"]:
        print(" ", e["occurred_at"], e["event_type"], e["event_id"])

    # 8.1) 纠错（替换）：用新事件号替换封签检查，结论按仅有效版本重算
    s, r = call("POST", "/corrections", {
        "correction_id": f"API-FIX-C-{run_id}",
        "sample_code": code,
        "target_event_id": f"API-EVT-C1-{run_id}",
        "action": "replace",
        "operator": "质控员",
        "reason": "示例：封签检查记录需补备注，整体替换",
        "new_event": {
            "event_id": f"API-EVT-C2-{run_id}",
            "event_type": "seal_check",
            "occurred_at": "2026-09-30T04:30:00Z",
            "checked_seal_no": "SEAL-DEMO-1",
            "seal_intact": True,
            "note": "纠错替换：复核无误",
        },
    })
    show("纠错-替换（同纠错号同内容重试幂等）", s, r, "correction")
    print("  替换后结论:", r["evaluation"]["status"], "| 保管人:", r["evaluation"]["current_custodian"])

    # 8.2) 同纠错号同内容重试 → 200 幂等；异内容 → 409（此处仅演示幂等）
    s, r = call("POST", "/corrections", {
        "correction_id": f"API-FIX-C-{run_id}",
        "sample_code": code,
        "target_event_id": f"API-EVT-C1-{run_id}",
        "action": "replace",
        "operator": "质控员",
        "reason": "示例：封签检查记录需补备注，整体替换",
        "new_event": {
            "event_id": f"API-EVT-C2-{run_id}",
            "event_type": "seal_check",
            "occurred_at": "2026-09-30T04:30:00Z",
            "checked_seal_no": "SEAL-DEMO-1",
            "seal_intact": True,
            "note": "纠错替换：复核无误",
        },
    })
    show("同纠错号同内容重试", s, r, "correction")

    # 8.3) 链查询可追溯被替换记录与纠错关系
    s, r = call("GET", f"/samples/{code}/chain")
    print(f"\n--- 纠错后证据链 [HTTP {s}] ---")
    print("有效时间线:", [e["event_id"] for e in r["timeline"]])
    print("被替换记录:", [e["event_id"] for e in r["superseded_events"]])
    print("纠错留痕:", [(c["correction_id"], c["action"], c["new_event_id"]) for c in r["corrections"]])

    # 9) 核定入库（仅 valid 可核定；核定后冻结，纠错也一并拒绝）
    s, r = call("POST", f"/samples/{code}/approve", {"approver": "质量主管", "note": "准予入库"})
    show("核定入库", s, r, "approval")

    # 9.1) 核定后纠错 → 409 history_locked
    s, r = call("POST", "/corrections", {
        "correction_id": f"API-FIX-LATE-{run_id}",
        "sample_code": code,
        "target_event_id": f"API-EVT-T1-{run_id}",
        "action": "void",
        "operator": "质控员",
        "reason": "核定后试探作废",
    })
    show("核定后纠错（拒绝）", s, r)

    # 10) 核定后再写事件 → 409 history_locked
    s, r = call("POST", "/events", {
        "event_id": f"API-EVT-LATE-{run_id}", "event_type": "seal_check", "sample_code": code,
        "occurred_at": "2026-09-30T06:00:00Z", "checked_seal_no": "SEAL-DEMO-1",
    })
    show("核定后改写（拒绝）", s, r)

    # 11) 作废演示（独立样本）：作废后核验只纳入有效版本，结论即时重算
    code2 = f"API-VOID-{run_id}"
    call("POST", "/samples", {
        "sample_code": code2,
        "collected_at": "2026-09-30T01:00:00Z",
        "collector": "王采集",
        "seal_no": "SEAL-DEMO-2",
    })
    call("POST", "/events", {
        "event_id": f"API-EVT2-T1-{run_id}", "event_type": "transfer", "sample_code": code2,
        "occurred_at": "2026-09-30T02:00:00Z",
        "transfer_no": "TR-DEMO-2", "from_custodian": "王采集", "to_custodian": "李运输",
    })
    call("POST", "/events", {
        "event_id": f"API-EVT2-R1-{run_id}", "event_type": "receive", "sample_code": code2,
        "occurred_at": "2026-09-30T03:00:00Z",
        "transfer_no": "TR-DEMO-2", "receiver": "李运输", "seal_no": "WRONG-SEAL",
    })
    s, r = call("POST", "/corrections", {
        "correction_id": f"API-FIX-V-{run_id}",
        "sample_code": code2,
        "target_event_id": f"API-EVT2-R1-{run_id}",
        "action": "void",
        "operator": "质控员",
        "reason": "误报，作废封签号错误的签收",
    })
    show("纠错-作废（原记录留痕、核验剔除）", s, r, "evaluation", "status")
    s, r = call("GET", f"/samples/{code2}/corrections")
    show("纠错审计查询", s, r)

    # 12) 车次批量移交：同一车次多份样本一次提交，各份作为既有 transfer 事件原子落库
    codes = [f"API-BATCH-{i}-{run_id}" for i in (1, 2)]
    for i, bc in enumerate(codes, start=1):
        call("POST", "/samples", {
            "sample_code": bc,
            "collected_at": "2026-09-30T01:00:00Z",
            "collector": "王采集",
            "seal_no": f"SEAL-BATCH-{i}",
        })
    batch_id = f"API-BATCH-{run_id}"
    batch_body = {
        "batch_id": batch_id,
        "handed_over_at": "2026-09-30T02:00:00Z",   # 统一移交时间
        "from_custodian": "王采集",                  # 交出人
        "to_custodian": "李运输",                    # 接收人
        "items": [
            # 每份样本：编号 + 该份的事件号（全局唯一）+ 移交单号
            {"sample_code": codes[0], "event_id": f"API-BE-T1-{run_id}", "transfer_no": "TR-BATCH-1"},
            {"sample_code": codes[1], "event_id": f"API-BE-T2-{run_id}", "transfer_no": "TR-BATCH-2"},
        ],
    }
    s, r = call("POST", "/transfer-batches", batch_body)
    show("车次批量移交（201；逐份返回当前核验结论）", s, r)
    for it in r.get("items", []):
        print(f"  样本 {it['sample_code']}: 事件 {it['event_id']} 原事件有效={it['event_effective']}"
              f" 当前结论={it['evaluation']['status']} 保管人={it['evaluation']['current_custodian']}")

    # 12.1) 同批次号同内容重试 → 200 幂等（即使之后样本已核定也仍幂等）
    s, r = call("POST", "/transfer-batches", batch_body)
    show("同批次号同内容重试（200 幂等）", s, r, "idempotent")

    # 12.2) 同批次号异内容 → 409 batch_conflict（整批失败，不留事件或批次）
    bad_body = json.loads(json.dumps(batch_body, ensure_ascii=False))
    bad_body["items"][1]["event_id"] = f"API-BE-T2-X-{run_id}"
    s, r = call("POST", "/transfer-batches", bad_body)
    show("同批次号异内容（409 batch_conflict）", s, r)

    # 12.3) 批次查询：逐份样本的当前核验结论 + 原事件是否仍有效
    s, r = call("GET", f"/transfer-batches/{batch_id}")
    show("批次查询", s, r)

    # 12.4) 批次列表概览
    s, r = call("GET", "/transfer-batches")
    print(f"\n--- 批次列表 [HTTP {s}] 共 {r['total']} 批；本批次份数数 ="
          f" {[x['item_count'] for x in r['items'] if x['batch_id'] == batch_id]} ---")

    # 12.5) 对批次内某份的原移交纠错（作废）：只改变有效链与批次查询结果，批次原始归属可追溯
    s, r = call("POST", "/corrections", {
        "correction_id": f"API-FIX-B-{run_id}",
        "sample_code": codes[0],
        "target_event_id": f"API-BE-T1-{run_id}",
        "action": "void",
        "operator": "质控员",
        "reason": "批量移交中的该份记录作废",
    })
    show("对批次内原移交作废", s, r)
    s, r = call("GET", f"/transfer-batches/{batch_id}")
    it0 = [x for x in r["items"] if x["sample_code"] == codes[0]][0]
    print(f"\n--- 作废后批次查询 [HTTP {s}] 样本 {it0['sample_code']}:"
          f" 原事件有效={it0['event_effective']}（superseded_by={it0['superseded_by']}），"
          f"当前结论={it0['evaluation']['status']}；批次归属仍可查 ---")
    s, r = call("GET", f"/samples/{codes[0]}/chain")
    print("证据链 superseded_events 中的批次归属:",
          [(e["event_id"], e.get("batch_id")) for e in r["superseded_events"]])

    # 13) 核验结论变更游标增量：实验室对接系统按游标拉取上述每一次实际提交的结论快照
    print("\n=== 核验结论变更（游标增量）===")
    # 增量循环：以 next_cursor 作为下一页的 after，直到某页 has_more=false
    after = 0
    pages = 0
    total = 0
    while True:
        qs = f"/changes?after={after}&limit=5"
        req = urllib.request.Request(BASE + qs)
        with urllib.request.urlopen(req) as resp:
            page = json.loads(resp.read().decode("utf-8"))
        pages += 1
        for item in page["items"]:
            total += 1
            approval = ""
            if item.get("approval"):
                approval = f" 核定于={item['approval']['approved_storage_at']}"
            print(f"  游标 {item['cursor']:>3} | {item['sample_code']} | "
                  f"{item['operation_type']}/{item['business_ref']}"
                  f" → {item['status']} | 保管人={item['current_custodian']}{approval}")
        if not page["has_more"]:
            after = page["next_cursor"]
            break
        after = page["next_cursor"]
    print(f"共 {pages} 页、{total} 条变更；本批终态游标={after}")

    # 13.1) 从终态游标再拉：无后续 → 空列表 + 原游标
    s, r = call_via_query(f"/changes?after={after}&limit=5")
    print("无后续时:", json.dumps(r, ensure_ascii=False))

    # 13.2) 非法游标 / 条数 → 400 明确报错
    for path in ("/changes?after=abc", "/changes?limit=0", "/changes?limit=99999"):
        s, r = call_via_query(path)
        print(f"非法参数 {path} → HTTP {s} {r['error']['code']}: {r['error']['message']}")


if __name__ == "__main__":
    main()
