"""领域服务：登记、事件写入（幂等/冲突）、保管链重算、事件纠错与入库核定。

核验规则（与 README 一致）：
  * 移交必须由当前保管人发起；移交至签收期间不得再次移交；
  * 签收必须匹配移交单号、接收人、封签号，且发生在移交之后；重复签收无效；
  * 封签检查封签号不符或封签破损 → 无效；
  * 缺签收 / 缺入库 → 待补证；
  * 断链、重复签收、封签不符 → 无效，无效优先于待补证；
  * 乱序补录按发生时间排序后整体重算；
  * 纠错（作废/替换）仅核定前可提交，只作用于当前有效版本，原记录与纠错关系永久留痕；
    核验仅纳入有效版本，被替换记录可经链查询追溯；
  * 仅有效链可核定入库；核定后历史冻结。
"""
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from . import config
from .schemas import ApproveIn, CorrectionIn, EventIn, NewEventIn, SampleIn, TransferBatchIn

LAB_CUSTODIAN = "实验室"

EVENT_TYPES = ("transfer", "receive", "seal_check", "storage")


class DomainError(Exception):
    def __init__(self, status_code: int, code: str, message: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details or {}


# --------------------------------------------------------------------------- 通用工具

def to_utc_iso(dt: datetime) -> str:
    """统一规范为 UTC ISO8601（秒精度、Z 结尾），无时区输入按 UTC 处理。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _fingerprint(obj: dict[str, Any]) -> str:
    raw = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def get_sample_or_404(conn: sqlite3.Connection, sample_code: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM sample WHERE sample_code = ?", (sample_code,)).fetchone()
    if row is None:
        raise DomainError(404, "sample_not_found", f"样本不存在: {sample_code}")
    return row


def ensure_unlocked(sample: sqlite3.Row) -> None:
    if config.LOCK_AFTER_APPROVAL and sample["approved_storage_at"]:
        raise DomainError(
            409,
            "history_locked",
            f"样本 {sample['sample_code']} 已核定入库（{sample['approved_storage_at']}），历史记录冻结，禁止改写",
        )


def evaluation_summary(result: dict[str, Any], sample: dict[str, Any]) -> dict[str, Any]:
    """evaluate() 结果的对外摘要（列表/详情/批次逐份结论共用）。"""
    return {
        "sample_code": result["sample_code"],
        "status": result["status"],
        "reasons": result["reasons"],
        "current_custodian": result["current_custodian"],
        "pending_transfer": result["pending_transfer"],
        "has_storage": result["has_storage"],
        "event_count": result["event_count"],
        "collected_at": sample["collected_at"],
        "collector": sample["collector"],
        "seal_no": sample["seal_no"],
        "approved_storage_at": sample["approved_storage_at"],
        "approved_by": sample["approved_by"],
    }


# --------------------------------------------------------------------------- 样本登记

def register_sample(conn: sqlite3.Connection, data: SampleIn) -> tuple[sqlite3.Row, bool]:
    """返回 (样本行, 是否幂等命中已有记录)。"""
    canonical = {
        "sample_code": data.sample_code,
        "collected_at": to_utc_iso(data.collected_at),
        "collector": data.collector,
        "seal_no": data.seal_no,
    }
    h = _fingerprint(canonical)
    existing = conn.execute("SELECT * FROM sample WHERE sample_code = ?", (data.sample_code,)).fetchone()
    if existing is not None:
        if existing["content_hash"] != h:
            raise DomainError(
                409,
                "sample_conflict",
                f"样本编号 {data.sample_code} 已登记但内容不一致（同编号异内容冲突）",
                {"existing_content_hash": existing["content_hash"], "incoming_content_hash": h},
            )
        return existing, True

    from .db import utcnow_iso

    cur = conn.execute(
        "INSERT INTO sample (sample_code, collected_at, collector, seal_no, content_hash, created_at)"
        " VALUES (?,?,?,?,?,?)",
        (data.sample_code, canonical["collected_at"], data.collector, data.seal_no, h, utcnow_iso()),
    )
    row = conn.execute("SELECT * FROM sample WHERE id = ?", (cur.lastrowid,)).fetchone()
    return row, False


# --------------------------------------------------------------------------- 事件写入

def _event_canonical(data: EventIn | NewEventIn, sample_code: str | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    """构造参与指纹的规范字典与落库 payload（字段集随事件类型固定）。

    sample_code 用于纠错替换：NewEventIn 本身不含样本编号（不得跨样本），
    以纠错单的 sample_code 落库。
    """
    code = sample_code or getattr(data, "sample_code")
    occurred = to_utc_iso(data.occurred_at)
    payload: dict[str, Any]
    if data.event_type == "transfer":
        payload = {
            "transfer_no": data.transfer_no,
            "from_custodian": data.from_custodian,
            "to_custodian": data.to_custodian,
        }
    elif data.event_type == "receive":
        payload = {"transfer_no": data.transfer_no, "receiver": data.receiver, "seal_no": data.seal_no}
    elif data.event_type == "seal_check":
        payload = {"checked_seal_no": data.checked_seal_no, "seal_intact": bool(data.seal_intact)}
    else:  # storage
        payload = {"custodian": data.custodian or LAB_CUSTODIAN}
        if data.storage_location is not None:
            payload["storage_location"] = data.storage_location

    canonical: dict[str, Any] = {
        "event_id": data.event_id,
        "event_type": data.event_type,
        "sample_code": code,
        "occurred_at": occurred,
        **payload,
    }
    if data.note is not None:
        canonical["note"] = data.note
        payload = {**payload, "note": data.note}
    return canonical, payload


def create_event(conn: sqlite3.Connection, data: EventIn) -> tuple[sqlite3.Row, bool]:
    """写入事件。返回 (事件行, 是否幂等重复提交)。

    同 event_id + 同内容 → 幂等返回；同 event_id + 异内容 → 409 冲突。
    """
    sample = get_sample_or_404(conn, data.sample_code)

    canonical, payload = _event_canonical(data)
    h = _fingerprint(canonical)
    payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    # 幂等查重优先于冻结检查：既有规则（同 event_id 同内容 200 / 异内容 409）
    # 在核定入库后也保持兼容；新增事件才受历史冻结限制。
    existing = conn.execute("SELECT * FROM custody_event WHERE event_id = ?", (data.event_id,)).fetchone()
    if existing is not None:
        if existing["content_hash"] != h:
            raise DomainError(
                409,
                "event_conflict",
                f"事件号 {data.event_id} 已存在但提交内容不一致（同事件号异内容冲突）",
                {
                    "event_id": data.event_id,
                    "existing": {
                        "event_type": existing["event_type"],
                        "occurred_at": existing["occurred_at"],
                        "content_hash": existing["content_hash"],
                    },
                    "incoming": {"event_type": data.event_type, "occurred_at": canonical["occurred_at"], "content_hash": h},
                },
            )
        return existing, True

    ensure_unlocked(sample)

    from .db import utcnow_iso

    try:
        cur = conn.execute(
            "INSERT INTO custody_event (sample_id, event_id, event_type, occurred_at, payload_json, content_hash, created_at)"
            " VALUES (?,?,?,?,?,?,?)",
            (sample["id"], data.event_id, data.event_type, canonical["occurred_at"], payload_json, h, utcnow_iso()),
        )
    except sqlite3.IntegrityError:
        # 并发下唯一键竞争：重新读取后按幂等/冲突处理
        existing = conn.execute("SELECT * FROM custody_event WHERE event_id = ?", (data.event_id,)).fetchone()
        if existing is not None and existing["content_hash"] == h:
            return existing, True
        raise DomainError(409, "event_conflict", f"事件号 {data.event_id} 并发写入且内容冲突")

    row = conn.execute("SELECT * FROM custody_event WHERE id = ?", (cur.lastrowid,)).fetchone()
    return row, False


# --------------------------------------------------------------------------- 保管链重算

def _correction_rows(conn: sqlite3.Connection, sample_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM event_correction WHERE sample_id = ? ORDER BY id ASC",
        (sample_id,),
    ).fetchall()


def correction_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    """纠错留痕的对外结构（原事件与纠错关系永久可查）。"""
    return {
        "correction_id": row["correction_id"],
        "target_event_id": row["target_event_id"],
        "action": row["action"],
        "operator": row["operator"],
        "reason": row["reason"],
        "new_event_id": row["new_event_id"],
        "created_at": row["created_at"],
    }


def evaluate(conn: sqlite3.Connection, sample: sqlite3.Row) -> dict[str, Any]:
    """按发生时间重算该样本的完整证据链，输出结论。

    仅纳入当前有效版本（superseded_by IS NULL）；被作废/替换的事件不参与核验，
    其内容经 superseded_events 与 corrections 永久留痕、可追溯。
    """
    all_rows = conn.execute(
        "SELECT * FROM custody_event WHERE sample_id = ? ORDER BY occurred_at ASC, id ASC",
        (sample["id"],),
    ).fetchall()
    events = [e for e in all_rows if e["superseded_by"] is None]
    superseded_rows = [e for e in all_rows if e["superseded_by"] is not None]

    current = sample["collector"]                       # 当前保管人（初始为采集人）
    open_transfer: dict[str, Any] | None = None         # 在途移交（已移交未签收）
    closed_transfer_nos: set[str] = set()               # 已正常签收的移交单
    last_receive_at: str | None = None
    stored = False
    storage_without_chain = False                       # 入库节点缺失前序签收（链不完整）
    invalid_reasons: list[str] = []
    timeline: list[dict[str, Any]] = []

    # 事件原始批次归属映射（批量移交产生的事件可追溯到批次；纠错作废/替换不改变原归属）
    batch_pks = {e["batch_pk"] for e in all_rows if e["batch_pk"] is not None}
    batch_id_by_pk: dict[int, str] = {}
    if batch_pks:
        placeholders = ",".join("?" * len(batch_pks))
        for br in conn.execute(
            f"SELECT id, batch_id FROM transfer_batch WHERE id IN ({placeholders})",
            tuple(batch_pks),
        ):
            batch_id_by_pk[br["id"]] = br["batch_id"]

    # 预索引全部移交单（含乱序补录），用于把"签收早于移交"与"单号根本不存在"区分开
    transfer_by_no: dict[str, dict[str, Any]] = {}
    for ev0 in events:
        if ev0["event_type"] == "transfer":
            transfer_by_no[json.loads(ev0["payload_json"])["transfer_no"]] = {
                "row": ev0,
                "payload": json.loads(ev0["payload_json"]),
            }

    for ev in events:
        p = json.loads(ev["payload_json"])
        ref = f"[事件 {ev['event_id']}]"
        timeline.append(
            {
                "event_id": ev["event_id"],
                "event_type": ev["event_type"],
                "occurred_at": ev["occurred_at"],
                "recorded_at": ev["created_at"],
                "payload": p,
                "replaces_event_id": ev["replaces_event_id"],
                "batch_id": batch_id_by_pk.get(ev["batch_pk"]),
            }
        )

        # 任何事件早于采集时间：记录无效（不可能在登记采集前发生保管动作）
        if ev["occurred_at"] < sample["collected_at"]:
            invalid_reasons.append(f"{ref} 事件发生时间 {ev['occurred_at']} 早于样本采集时间 {sample['collected_at']}")
            continue

        et = ev["event_type"]

        if et == "transfer":
            if open_transfer is not None:
                invalid_reasons.append(
                    f"{ref} 移交至签收期间再次移交：移交单 {open_transfer['transfer_no']}"
                    f"（{open_transfer['from_custodian']} → {open_transfer['to_custodian']}）尚未签收"
                )
                continue
            frm, to, no = p["from_custodian"], p["to_custodian"], p["transfer_no"]
            if frm != current:
                invalid_reasons.append(
                    f"{ref} 移交人 {frm} 不是当前保管人 {current}（移交单 {no}，保管人断链）"
                )
                continue
            open_transfer = {"transfer_no": no, "from_custodian": frm, "to_custodian": to, "occurred_at": ev["occurred_at"]}
            # 签收完成前保管人不变，仍为移交人

        elif et == "receive":
            no = p["transfer_no"]
            if no in closed_transfer_nos:
                invalid_reasons.append(f"{ref} 重复签收：移交单 {no} 已被签收")
                continue
            known = transfer_by_no.get(no)
            if open_transfer is None or open_transfer["transfer_no"] != no:
                if known is not None and ev["occurred_at"] <= known["row"]["occurred_at"]:
                    invalid_reasons.append(
                        f"{ref} 签收时间 {ev['occurred_at']} 不晚于移交时间 {known['row']['occurred_at']}（单号 {no}）"
                    )
                    # 该移交到达时在途状态仍可能被后续正确签收关闭，故此处不改在途状态
                    continue
                invalid_reasons.append(f"{ref} 签收无对应在途移交：单号 {no} 未移交或不匹配（保管人断链）")
                continue

            mismatch = False
            if p["receiver"] != open_transfer["to_custodian"]:
                invalid_reasons.append(
                    f"{ref} 签收接收人不匹配：签收人 {p['receiver']} ≠ 移交单 {no} 指定接收人 {open_transfer['to_custodian']}"
                )
                mismatch = True
            if p["seal_no"] != sample["seal_no"]:
                invalid_reasons.append(
                    f"{ref} 封签号不符：签收封签 {p['seal_no']} ≠ 登记封签 {sample['seal_no']}"
                )
                mismatch = True
            if ev["occurred_at"] <= open_transfer["occurred_at"]:
                invalid_reasons.append(
                    f"{ref} 签收时间 {ev['occurred_at']} 不晚于移交时间 {open_transfer['occurred_at']}（单号 {no}）"
                )
                mismatch = True
            if mismatch:
                # 在途移交保留：允许后续按发生时间补录正确签收
                continue

            closed_transfer_nos.add(no)
            current = p["receiver"]
            last_receive_at = ev["occurred_at"]
            open_transfer = None

        elif et == "seal_check":
            if p["checked_seal_no"] != sample["seal_no"]:
                invalid_reasons.append(
                    f"{ref} 封签检查封签号不符：检查封签 {p['checked_seal_no']} ≠ 登记封签 {sample['seal_no']}"
                )
            if not p.get("seal_intact", True):
                invalid_reasons.append(f"{ref} 封签检查发现封签破损/不完好")

        else:  # storage
            if last_receive_at is not None and ev["occurred_at"] < last_receive_at:
                invalid_reasons.append(
                    f"{ref} 入库时间 {ev['occurred_at']} 早于最终签收时间 {last_receive_at}（保管链不成立）"
                )
                continue
            if open_transfer is not None or last_receive_at is None:
                # 尚无已完成签收（或还有在途移交）：入库节点缺失前序保管链 → 待补证，保管人不变更
                storage_without_chain = True
                continue
            stored = True
            current = p.get("custodian") or LAB_CUSTODIAN

    # ------------------------------------------------------------ 汇总（无效优先）
    pending_reasons: list[str] = []
    if open_transfer is not None:
        pending_reasons.append(
            f"移交单 {open_transfer['transfer_no']} 已移交但缺少签收"
            f"（{open_transfer['from_custodian']} → {open_transfer['to_custodian']}，移交时间 {open_transfer['occurred_at']}）"
        )
    elif storage_without_chain:
        pending_reasons.append("存在入库记录但缺少此前完整的移交-签收链，入库不能成立")
    if not stored:
        pending_reasons.append("缺少实验室入库记录")

    if invalid_reasons:
        status = "invalid"
        reasons = invalid_reasons + [f"另有待补证：{r}" for r in pending_reasons]
    elif pending_reasons:
        status = "pending"
        reasons = pending_reasons
    else:
        status = "valid"
        reasons = [f"保管链完整：采集 {sample['collector']} 至当前保管人 {current}，封签一致，已入库"]

    # 被作废/替换的原始事件（按发生时间排列），仅用于追溯，不参与上面的核验
    superseded_events = [
        {
            "event_id": ev["event_id"],
            "event_type": ev["event_type"],
            "occurred_at": ev["occurred_at"],
            "recorded_at": ev["created_at"],
            "payload": json.loads(ev["payload_json"]),
            "superseded_by": ev["superseded_by"],
            "replaces_event_id": ev["replaces_event_id"],
            "batch_id": batch_id_by_pk.get(ev["batch_pk"]),
        }
        for ev in superseded_rows
    ]
    corrections = [correction_to_dict(r) for r in _correction_rows(conn, sample["id"])]

    return {
        "sample_code": sample["sample_code"],
        "status": status,
        "reasons": reasons,
        "current_custodian": current,
        "pending_transfer": open_transfer,
        "has_storage": stored,
        "approved_storage_at": sample["approved_storage_at"],
        "event_count": len(events),
        "timeline": timeline,
        "corrections": corrections,
        "superseded_events": superseded_events,
    }


# --------------------------------------------------------------------------- 事件纠错（作废 / 替换）

def _correction_fingerprint(data: CorrectionIn, new_canonical: dict[str, Any] | None) -> str:
    obj = {
        "correction_id": data.correction_id,
        "sample_code": data.sample_code,
        "target_event_id": data.target_event_id,
        "action": data.action,
        "operator": data.operator,
        "reason": data.reason,
    }
    if new_canonical is not None:
        obj["new_event"] = new_canonical
    return _fingerprint(obj)


def _get_correction(conn: sqlite3.Connection, correction_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM event_correction WHERE correction_id = ?", (correction_id,)
    ).fetchone()


def submit_correction(conn: sqlite3.Connection, data: CorrectionIn) -> tuple[dict[str, Any], bool]:
    """提交作废/替换纠错。返回 (纠错结果, 是否幂等重试)。

    规则：
      * 仅核定入库前可纠错；纠错不得跨样本；
      * 只能针对当前有效版本（已被作废/替换的事件再次纠错 → 409，失败不改链）；
      * replace 必须给完整新事件内容且新事件号未被占用（含已留痕的旧号）；
      * 同 correction_id 同内容 → 幂等返回 200；异内容 → 409；
      * 并发针对同一有效版本只允许一次成功（行级条件更新守卫，失败回滚）。
    """
    from .db import utcnow_iso

    sample = get_sample_or_404(conn, data.sample_code)

    new_canonical: dict[str, Any] | None = None
    new_payload_json: str | None = None
    new_hash: str | None = None
    if data.new_event is not None:
        new_canonical, new_payload = _event_canonical(data.new_event, sample_code=data.sample_code)
        new_payload_json = json.dumps(new_payload, ensure_ascii=False, separators=(",", ":"))
        new_hash = _fingerprint(new_canonical)

    request_hash = _correction_fingerprint(data, new_canonical)

    # 1) 同纠错号：同内容幂等 / 异内容冲突（与既有事件上报语义一致）
    existing = _get_correction(conn, data.correction_id)
    if existing is not None:
        if existing["request_hash"] != request_hash:
            raise DomainError(
                409,
                "correction_conflict",
                f"纠错号 {data.correction_id} 已使用但提交内容不一致（同纠错号异内容冲突）",
                {"correction_id": data.correction_id,
                 "existing_request_hash": existing["request_hash"],
                 "incoming_request_hash": request_hash},
            )
        return _correction_result(conn, sample, existing), True

    # 2) 核定入库后拒绝纠错
    ensure_unlocked(sample)

    # 3) 目标事件必须存在、属于该样本、且仍是当前有效版本
    target = conn.execute(
        "SELECT * FROM custody_event WHERE event_id = ?", (data.target_event_id,)
    ).fetchone()
    if target is None:
        raise DomainError(404, "event_not_found", f"目标事件不存在: {data.target_event_id}")
    if target["sample_id"] != sample["id"]:
        raise DomainError(
            409,
            "correction_cross_sample",
            f"纠错不得跨样本：事件 {data.target_event_id} 不属于样本 {data.sample_code}",
            {"target_event_id": data.target_event_id,
               "target_sample_id": target["sample_id"],
               "request_sample_code": data.sample_code},
        )
    if target["superseded_by"] is not None:
        raise DomainError(
            409,
            "event_not_effective",
            f"事件 {data.target_event_id} 已被纠错 {target['superseded_by']} 作废/替换，"
            "仅可针对当前有效版本纠错",
            {"target_event_id": data.target_event_id, "superseded_by": target["superseded_by"]},
        )

    # 4) 替换：新事件号必须未被占用（有效版本与历史留痕共用全局唯一事件号）
    new_row_id: int | None = None
    if data.action == "replace":
        occupied = conn.execute(
            "SELECT 1 FROM custody_event WHERE event_id = ?", (data.new_event.event_id,)
        ).fetchone()
        if occupied is not None:
            raise DomainError(
                409,
                "new_event_id_occupied",
                f"新事件号 {data.new_event.event_id} 已被占用，替换须使用未占用的新事件号",
                {"new_event_id": data.new_event.event_id},
            )

    # 5) 原子生效：条件更新仅命中仍为有效版本的目标行。
    #    本事务在 IMMEDIATE 写锁内，其他纠错/核定无法穿插；若目标行已被并发事务
    #    抢先纠错（本事务持锁后重读必可见），rowcount=0，整体回滚，链不变。
    cur = conn.execute(
        "UPDATE custody_event SET superseded_by = ? WHERE id = ? AND superseded_by IS NULL",
        (data.correction_id, target["id"]),
    )
    if cur.rowcount == 0:
        raise DomainError(
            409,
            "event_not_effective",
            f"事件 {data.target_event_id} 已被并发纠错，同一有效版本仅允许一次纠错成功",
            {"target_event_id": data.target_event_id},
        )

    if data.action == "replace":
        ins = conn.execute(
            "INSERT INTO custody_event"
            " (sample_id, event_id, event_type, occurred_at, payload_json, content_hash,"
            "  created_at, superseded_by, replaces_event_id)"
            " VALUES (?,?,?,?,?,?,?,NULL,?)",
            (sample["id"], data.new_event.event_id, data.new_event.event_type,
             new_canonical["occurred_at"], new_payload_json, new_hash,
             utcnow_iso(), data.target_event_id),
        )
        new_row_id = ins.lastrowid

    corr_cur = conn.execute(
        "INSERT INTO event_correction"
        " (correction_id, sample_id, target_event_id, action, operator, reason,"
        "  new_event_id, new_event_row_id, request_hash, created_at)"
        " VALUES (?,?,?,?,?,?,?,?,?,?)",
        (data.correction_id, sample["id"], data.target_event_id, data.action,
         data.operator, data.reason,
         data.new_event.event_id if data.new_event else None, new_row_id,
         request_hash, utcnow_iso()),
    )
    corr_row = conn.execute(
        "SELECT * FROM event_correction WHERE id = ?", (corr_cur.lastrowid,)
    ).fetchone()
    return _correction_result(conn, sample, corr_row), False


def _correction_result(
    conn: sqlite3.Connection, sample: sqlite3.Row, corr_row: sqlite3.Row
) -> dict[str, Any]:
    """纠错提交后的响应体（含按最终有效链重算的结论、原因、当前保管人）。"""
    result = evaluate(conn, sample)
    return {"correction": correction_to_dict(corr_row), "evaluation": result}


def list_corrections(conn: sqlite3.Connection, sample_code: str) -> list[dict[str, Any]]:
    sample = get_sample_or_404(conn, sample_code)
    return [correction_to_dict(r) for r in _correction_rows(conn, sample["id"])]


# --------------------------------------------------------------------------- 入库核定

def approve_storage(conn: sqlite3.Connection, sample_code: str, data: ApproveIn) -> dict[str, Any]:
    sample = get_sample_or_404(conn, sample_code)
    result = evaluate(conn, sample)

    if sample["approved_storage_at"]:
        # 已核定：幂等返回，不允许借重复核定改写
        result["approval"] = {"approved": True, "approved_at": sample["approved_storage_at"], "approver": sample["approved_by"]}
        return result

    if result["status"] != "valid":
        raise DomainError(
            422,
            "approval_rejected",
            f"样本 {sample_code} 当前结论为 {result['status']}，仅有效（valid）保管链可核定入库",
            {"evaluation": result},
        )

    from .db import utcnow_iso

    approved_at = utcnow_iso()
    conn.execute(
        "UPDATE sample SET approved_storage_at = ?, approved_by = ?, approve_note = ? WHERE id = ?",
        (approved_at, data.approver, data.note, sample["id"]),
    )
    result["approved_storage_at"] = approved_at
    result["approval"] = {"approved": True, "approved_at": approved_at, "approver": data.approver, "note": data.note}
    return result


# --------------------------------------------------------------------------- 车次批量移交

def _batch_canonical(data: TransferBatchIn) -> dict[str, Any]:
    """整批内容的规范字典（参与批次指纹）。

    items 按 (sample_code, event_id, transfer_no) 排序后参与指纹——批次是同一车次
    多份移交的集合，重试时逐份排列顺序不同视为同内容幂等；落库与查询仍保留提交顺序。
    每份事件的规范内容与单事件接口一致（transfer 必填三字段 + 统一移交时间与
    交出/接收人），故批次内事件 content_hash 与 POST /events 完全相同，
    两种入口可互相识别幂等/冲突。
    """
    handed_over = to_utc_iso(data.handed_over_at)
    ordered = sorted(
        ((it.sample_code, it.event_id, it.transfer_no) for it in data.items),
        key=lambda x: (x[0], x[1], x[2]),
    )
    items = [
        {
            "sample_code": sample_code,
            "event_id": event_id,
            "event_type": "transfer",
            "occurred_at": handed_over,
            "transfer_no": transfer_no,
            "from_custodian": data.from_custodian,
            "to_custodian": data.to_custodian,
        }
        for sample_code, event_id, transfer_no in ordered
    ]
    return {
        "batch_id": data.batch_id,
        "handed_over_at": handed_over,
        "from_custodian": data.from_custodian,
        "to_custodian": data.to_custodian,
        "items": items,
    }


def _get_batch(conn: sqlite3.Connection, batch_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM transfer_batch WHERE batch_id = ?", (batch_id,)
    ).fetchone()


def submit_transfer_batch(conn: sqlite3.Connection, data: TransferBatchIn) -> tuple[sqlite3.Row, bool]:
    """同一交接车次多份样本的批量移交：各份作为既有 transfer 事件一次提交。

    原子性：批内结构重复已由 Pydantic 拦截（422）；以下任一业务条件不满足，
    整个批次失败并回滚——不留事件、不留批次：
      * 任一样本不存在（404 sample_not_found）；
      * 任一样本已核定入库（409 history_locked）；
      * 任一事件号已被占用（409 event_conflict）。
    幂等/冲突：同 batch_id 同内容重试 → 幂等（200，即使样本已核定）；
              同 batch_id 异内容 → 409 batch_conflict。
    返回 (批次行, 是否幂等重试)。
    """
    from .db import utcnow_iso

    canonical = _batch_canonical(data)
    h = _fingerprint(canonical)

    # 1) 同批次号：同内容幂等（只读返回，不受核定冻结影响）/ 异内容冲突
    existing = _get_batch(conn, data.batch_id)
    if existing is not None:
        if existing["content_hash"] != h:
            raise DomainError(
                409,
                "batch_conflict",
                f"批次号 {data.batch_id} 已使用但提交内容不一致（同批次号异内容冲突）",
                {"batch_id": data.batch_id,
                 "existing_content_hash": existing["content_hash"],
                 "incoming_content_hash": h},
            )
        return existing, True

    # 2) 全部样本必须存在且未核定（任一失败整批失败，事务由调用方回滚）
    missing: list[str] = []
    locked: list[dict[str, Any]] = []
    sample_rows: dict[str, sqlite3.Row] = {}
    for it in data.items:
        row = conn.execute(
            "SELECT * FROM sample WHERE sample_code = ?", (it.sample_code,)
        ).fetchone()
        if row is None:
            missing.append(it.sample_code)
            continue
        sample_rows[it.sample_code] = row
        if config.LOCK_AFTER_APPROVAL and row["approved_storage_at"]:
            locked.append({"sample_code": it.sample_code,
                           "approved_storage_at": row["approved_storage_at"]})
    if missing:
        raise DomainError(
            404,
            "sample_not_found",
            f"批次含不存在的样本: {', '.join(missing)}（整批失败，不落任何事件或批次）",
            {"missing_sample_codes": missing},
        )
    if locked:
        raise DomainError(
            409,
            "history_locked",
            f"批次含已核定入库的样本: {', '.join(x['sample_code'] for x in locked)}"
            "（整批失败，不落任何事件或批次）",
            {"locked_samples": locked},
        )

    # 3) 任一事件号已占用（含被纠错留痕的旧号）→ 整批失败
    occupied: list[dict[str, str]] = []
    for it in data.items:
        row = conn.execute(
            "SELECT event_id, event_type FROM custody_event WHERE event_id = ?", (it.event_id,)
        ).fetchone()
        if row is not None:
            occupied.append({"event_id": it.event_id, "event_type": row["event_type"]})
    if occupied:
        raise DomainError(
            409,
            "event_conflict",
            f"批次含已占用的事件号 {len(occupied)} 个（整批失败，不落任何事件或批次）",
            {"occupied_event_ids": [x["event_id"] for x in occupied], "events": occupied},
        )

    # 4) 写批次头，再按提交顺序写各份 transfer 事件（均带 batch_pk 原始归属）
    try:
        batch_cur = conn.execute(
            "INSERT INTO transfer_batch"
            " (batch_id, handed_over_at, from_custodian, to_custodian, content_hash, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (data.batch_id, canonical["handed_over_at"], data.from_custodian,
             data.to_custodian, h, utcnow_iso()),
        )
    except sqlite3.IntegrityError:
        # 并发下同批次号竞争：重读按幂等/冲突处理
        raced = _get_batch(conn, data.batch_id)
        if raced is not None and raced["content_hash"] == h:
            return raced, True
        raise DomainError(409, "batch_conflict", f"批次号 {data.batch_id} 并发提交且内容冲突")
    batch_pk = batch_cur.lastrowid

    now = utcnow_iso()
    for it in data.items:
        ev_in = EventIn(
            event_id=it.event_id,
            event_type="transfer",
            sample_code=it.sample_code,
            occurred_at=data.handed_over_at,
            transfer_no=it.transfer_no,
            from_custodian=data.from_custodian,
            to_custodian=data.to_custodian,
        )
        ev_canonical, payload = _event_canonical(ev_in)
        ev_hash = _fingerprint(ev_canonical)
        try:
            conn.execute(
                "INSERT INTO custody_event"
                " (sample_id, event_id, event_type, occurred_at, payload_json, content_hash,"
                "  created_at, superseded_by, replaces_event_id, batch_pk)"
                " VALUES (?,?,?,?,?,?,?,NULL,NULL,?)",
                (sample_rows[it.sample_code]["id"], it.event_id, "transfer",
                 ev_canonical["occurred_at"],
                 json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                 ev_hash, now, batch_pk),
            )
        except sqlite3.IntegrityError:
            # 并发事件号竞争（本事务 IMMEDIATE 串行下仅极端竞态可能）：整批回滚
            raise DomainError(
                409, "event_conflict",
                f"事件号 {it.event_id} 已被并发占用（整批失败）",
                {"event_id": it.event_id},
            )

    return conn.execute("SELECT * FROM transfer_batch WHERE id = ?", (batch_pk,)).fetchone(), False


def _batch_item_rows(conn: sqlite3.Connection, batch_pk: int) -> list[sqlite3.Row]:
    """批次内各份样本（按提交顺序 = 行 id 升序）。"""
    return conn.execute(
        "SELECT ce.*, s.sample_code AS sample_code"
        " FROM custody_event ce JOIN sample s ON s.id = ce.sample_id"
        " WHERE ce.batch_pk = ? ORDER BY ce.id ASC",
        (batch_pk,),
    ).fetchall()


def batch_header_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "batch_id": row["batch_id"],
        "handed_over_at": row["handed_over_at"],
        "from_custodian": row["from_custodian"],
        "to_custodian": row["to_custodian"],
        "created_at": row["created_at"],
    }


def build_batch_result(conn: sqlite3.Connection, batch: sqlite3.Row) -> dict[str, Any]:
    """批次查询/提交结果：批次头 + 逐份样本的当前核验结论与原事件是否仍有效。"""
    items: list[dict[str, Any]] = []
    for ev in _batch_item_rows(conn, batch["id"]):
        sample = conn.execute(
            "SELECT * FROM sample WHERE id = ?", (ev["sample_id"],)
        ).fetchone()
        evaluation = evaluate(conn, sample)
        items.append({
            "sample_code": ev["sample_code"],
            "event_id": ev["event_id"],
            "transfer_no": json.loads(ev["payload_json"])["transfer_no"],
            "event_effective": ev["superseded_by"] is None,
            "superseded_by": ev["superseded_by"],
            "evaluation": evaluation_summary(evaluation, dict(sample)),
        })
    return {"batch": batch_header_dict(batch), "total": len(items), "items": items}


def get_transfer_batch(conn: sqlite3.Connection, batch_id: str) -> dict[str, Any]:
    row = _get_batch(conn, batch_id)
    if row is None:
        raise DomainError(404, "batch_not_found", f"批次不存在: {batch_id}")
    return build_batch_result(conn, row)


def list_transfer_batches(conn: sqlite3.Connection) -> dict[str, Any]:
    rows = conn.execute(
        "SELECT b.*, (SELECT COUNT(*) FROM custody_event ce WHERE ce.batch_pk = b.id) AS item_count"
        " FROM transfer_batch b ORDER BY b.id ASC"
    ).fetchall()
    return {
        "total": len(rows),
        "items": [{**batch_header_dict(r), "item_count": r["item_count"]} for r in rows],
    }
