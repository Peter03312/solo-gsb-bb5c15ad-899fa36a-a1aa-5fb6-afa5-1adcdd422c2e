"""SQLite 连接、建表初始化与底层读写工具。"""
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS sample (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_code         TEXT NOT NULL UNIQUE,            -- 样本编号（业务唯一）
    collected_at        TEXT NOT NULL,                   -- 采集时间（UTC ISO8601）
    collector           TEXT NOT NULL,                   -- 采集人（初始保管人）
    seal_no             TEXT NOT NULL,                   -- 登记封签号
    content_hash        TEXT NOT NULL,                   -- 登记内容指纹（幂等/冲突判定）
    approved_storage_at TEXT,                            -- 入库核定时间；非空即历史锁定
    approved_by         TEXT,                            -- 核定操作人
    approve_note        TEXT,                            -- 核定备注
    created_at          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS custody_event (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id          INTEGER NOT NULL REFERENCES sample(id),
    event_id           TEXT NOT NULL UNIQUE,             -- 全局唯一事件号
    event_type         TEXT NOT NULL CHECK (event_type IN ('transfer','receive','seal_check','storage')),
    occurred_at        TEXT NOT NULL,                    -- 事件发生时间（UTC ISO8601）
    payload_json       TEXT NOT NULL,                    -- 类型相关字段
    content_hash       TEXT NOT NULL,                    -- 事件内容指纹（同事件号重试比对）
    created_at         TEXT NOT NULL,
    superseded_by      TEXT,                             -- 被哪条纠错作废/替换（NULL=当前有效版本）
    replaces_event_id  TEXT,                             -- 替换产生的新事件：指向被替换的原事件号
    batch_pk           INTEGER REFERENCES transfer_batch(id) -- 车次批量移交产生的事件：原始批次归属（纠错不改变）
);

CREATE TABLE IF NOT EXISTS event_correction (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    correction_id     TEXT NOT NULL UNIQUE,              -- 全局唯一纠错号
    sample_id         INTEGER NOT NULL REFERENCES sample(id),
    target_event_id   TEXT NOT NULL,                     -- 被纠错的事件号（提交时为当前有效版本）
    action            TEXT NOT NULL CHECK (action IN ('void','replace')),
    operator          TEXT NOT NULL,                     -- 纠错操作人
    reason            TEXT NOT NULL,                     -- 纠错原因
    new_event_id      TEXT,                              -- replace 时的新事件号（void 为 NULL）
    new_event_row_id  INTEGER REFERENCES custody_event(id), -- replace 时新事件行
    request_hash      TEXT NOT NULL,                     -- 纠错请求指纹（同纠错号幂等/冲突判定）
    created_at        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transfer_batch (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id        TEXT NOT NULL UNIQUE,                -- 全局唯一批次号（同一交接车次）
    handed_over_at  TEXT NOT NULL,                       -- 统一移交时间（UTC ISO8601）
    from_custodian  TEXT NOT NULL,                       -- 交出人
    to_custodian    TEXT NOT NULL,                       -- 接收人
    content_hash    TEXT NOT NULL,                       -- 整批内容指纹（同批次号幂等/冲突判定）
    created_at      TEXT NOT NULL
);

-- 核验结论变更记录（游标增量）：每次实际提交在业务写事务内为各受影响样本追加一行。
-- id 即全局递增游标：AUTOINCREMENT + 写事务 BEGIN IMMEDIATE 串行，游标顺序即提交可见顺序；
-- 失败回滚不占号、不留痕；幂等重试/重复核定不写入。
CREATE TABLE IF NOT EXISTS change_record (
    id                INTEGER PRIMARY KEY AUTOINCREMENT, -- 全局递增游标
    sample_id         INTEGER NOT NULL REFERENCES sample(id),
    sample_code       TEXT NOT NULL,                    -- 样本编号（冗余落库，便于增量查询）
    operation_type    TEXT NOT NULL CHECK (operation_type IN
                        ('register','event','batch_transfer','correction','approval','baseline')),
    business_ref      TEXT NOT NULL,                    -- 业务编号：登记/核定=样本编号，事件=事件号，
                                                        --   纠错=纠错号，批量=批次号，基线=样本编号
    ref_event_type    TEXT,                             -- 事件类操作的事件类型（transfer/receive/...）
    status            TEXT NOT NULL,                    -- 提交后结论：valid/pending/invalid
    reasons_json      TEXT NOT NULL,                    -- 提交后原因列表（JSON）
    current_custodian TEXT NOT NULL,                    -- 提交后当前保管人
    detail_json       TEXT NOT NULL DEFAULT '{}',       -- 操作附加明细（动作/目标事件/批次份内事件号等）
    approval_json     TEXT,                             -- 提交后核定信息快照（仅核定/已核定基线非空）
    committed_at      TEXT NOT NULL                     -- 提交时间（UTC ISO8601）
);

CREATE INDEX IF NOT EXISTS idx_event_sample_time ON custody_event(sample_id, occurred_at, id);
"""

# 索引（须在旧库补列迁移完成后再创建，故不放入 SCHEMA）
POST_MIGRATION_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_event_effective ON custody_event(sample_id, superseded_by, occurred_at)",
    "CREATE INDEX IF NOT EXISTS idx_correction_sample ON event_correction(sample_id, id)",
    "CREATE INDEX IF NOT EXISTS idx_event_batch ON custody_event(batch_pk)",
    "CREATE INDEX IF NOT EXISTS idx_change_sample ON change_record(sample_id, id)",
]

# 旧库增量补列
_MIGRATION_COLUMNS = {
    "custody_event": [
        ("superseded_by", "TEXT"),
        ("replaces_event_id", "TEXT"),
        ("batch_pk", "INTEGER REFERENCES transfer_batch(id)"),
    ],
}


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def get_connection() -> sqlite3.Connection:
    config.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(config.DB_PATH), timeout=config.DB_TIMEOUT)
    conn.row_factory = sqlite3.Row
    # WAL 提升读写并发，遇到锁立即报错由服务层转 409
    conn.execute("PRAGMA journal_mode=WAL");
    conn.execute("PRAGMA foreign_keys=ON");
    conn.execute("PRAGMA busy_timeout=%d" % int(config.DB_TIMEOUT * 1000))
    return conn


@contextmanager
def db_session(commit: bool = True, immediate: bool = False) -> Iterator[sqlite3.Connection]:
    """事务上下文。

    immediate=True 时以 BEGIN IMMEDIATE 开启，事务一开始即持有写锁，
    使所有写操作（事件上报 / 纠错 / 核定）在 SQLite 层严格串行：
    纠错与核定并发时后到者必然看到先到者已提交的最终状态。
    """
    conn = get_connection()
    if immediate:
        conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    """进程启动时建表（幂等），并对旧库执行增量迁移。

    旧库升级（含现存样本但无任何变更记录）时，在独立 IMMEDIATE 事务内为每条现存样本
    补一条 ``baseline`` 当前状态基线；不伪造历史操作（operation_type 明确为基线，
    business_ref 为样本编号、committed_at 为升级时刻）。新库/空库不产生基线，
    升级幂等：已有任何变更记录即不再补基线。
    """
    with db_session() as conn:
        conn.executescript(SCHEMA)
        for table, columns in _MIGRATION_COLUMNS.items():
            existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
            for name, decl in columns:
                if name not in existing:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")
        for stmt in POST_MIGRATION_INDEXES:
            conn.execute(stmt)
        sample_count = conn.execute("SELECT COUNT(*) FROM sample").fetchone()[0]
        change_count = conn.execute("SELECT COUNT(*) FROM change_record").fetchone()[0]

    if sample_count > 0 and change_count == 0:
        # 延迟导入避免 db <-> service 循环依赖
        from .service import write_baseline_changes

        with db_session(immediate=True) as conn:
            # 持写锁后复核：并发启动/多进程升级时仅一个事务会看到 0 条变更记录
            if (
                conn.execute("SELECT COUNT(*) FROM sample").fetchone()[0] > 0
                and conn.execute("SELECT COUNT(*) FROM change_record").fetchone()[0] == 0
            ):
                write_baseline_changes(conn)


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None
