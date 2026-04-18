#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd


_INIT_LOCK = threading.Lock()
_INITIALIZED = False

_RAW_EXPENSE_COLUMNS = [
    "expense_id",
    "date",
    "updated_at",
    "month",
    "description",
    "category",
    "amount",
    "currency",
]

_SHARE_COLUMNS = [
    "expense_id",
    "user_id",
    "person",
    "month",
    "owed_share",
    "paid_share",
]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS expenses (
    expense_id INTEGER PRIMARY KEY,
    date TEXT,
    updated_at TEXT,
    month TEXT,
    description TEXT,
    category TEXT,
    amount REAL NOT NULL DEFAULT 0,
    currency TEXT,
    payload_hash TEXT,
    source_event_time TEXT
);

CREATE TABLE IF NOT EXISTS shares (
    expense_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    person TEXT,
    month TEXT,
    owed_share REAL NOT NULL DEFAULT 0,
    paid_share REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (expense_id, user_id),
    FOREIGN KEY (expense_id) REFERENCES expenses(expense_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS group_state (
    group_id INTEGER PRIMARY KEY,
    group_name TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    start_month TEXT,
    end_month TEXT,
    debt_edges_json TEXT NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS app_state (
    scope TEXT NOT NULL,
    key TEXT NOT NULL,
    value_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (scope, key)
);

CREATE TABLE IF NOT EXISTS refresh_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    requested_at TEXT NOT NULL,
    source TEXT NOT NULL,
    handled_at TEXT
);

CREATE TABLE IF NOT EXISTS published_entities (
    entity_type TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (entity_type, entity_key)
);

CREATE INDEX IF NOT EXISTS idx_refresh_requests_pending
    ON refresh_requests(handled_at, requested_at);
"""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def database_path() -> str:
    raw = (os.getenv("SPLITWISE_READ_MODEL_DB") or "/tmp/splitwise-read-model.db").strip()
    return raw or "/tmp/splitwise-read-model.db"


def _connect_raw() -> sqlite3.Connection:
    path = database_path()
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def initialize_db() -> None:
    global _INITIALIZED
    with _INIT_LOCK:
        if _INITIALIZED:
            return
        conn = _connect_raw()
        try:
            conn.executescript(_SCHEMA)
            conn.commit()
        finally:
            conn.close()
        _INITIALIZED = True


def connect() -> sqlite3.Connection:
    initialize_db()
    return _connect_raw()


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def set_state(scope: str, key: str, value: Any) -> None:
    now = utc_now_iso()
    conn = connect()
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO app_state(scope, key, value_json, updated_at)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(scope, key) DO UPDATE
                SET value_json = excluded.value_json,
                    updated_at = excluded.updated_at
                """,
                (scope, key, _json_dump(value), now),
            )
    finally:
        conn.close()


def load_scope_state(scope: str) -> Dict[str, Any]:
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT key, value_json FROM app_state WHERE scope = ? ORDER BY key",
            (scope,),
        ).fetchall()
    finally:
        conn.close()
    result: Dict[str, Any] = {}
    for row in rows:
        try:
            result[str(row["key"])] = json.loads(row["value_json"])
        except Exception:
            result[str(row["key"])] = row["value_json"]
    return result


def record_refresh_request(source: str = "web") -> None:
    conn = connect()
    try:
        with conn:
            conn.execute(
                "INSERT INTO refresh_requests(requested_at, source, handled_at) VALUES(?, ?, NULL)",
                (utc_now_iso(), source),
            )
    finally:
        conn.close()


def pending_refresh_count() -> int:
    conn = connect()
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS count FROM refresh_requests WHERE handled_at IS NULL"
        ).fetchone()
    finally:
        conn.close()
    return int(row["count"]) if row else 0


def mark_refresh_requests_handled() -> int:
    now = utc_now_iso()
    conn = connect()
    try:
        with conn:
            cur = conn.execute(
                "UPDATE refresh_requests SET handled_at = ? WHERE handled_at IS NULL",
                (now,),
            )
            return int(cur.rowcount or 0)
    finally:
        conn.close()


def load_published_entity_hashes(entity_type: str) -> Dict[str, str]:
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT entity_key, payload_hash FROM published_entities WHERE entity_type = ?",
            (entity_type,),
        ).fetchall()
    finally:
        conn.close()
    return {str(row["entity_key"]): str(row["payload_hash"]) for row in rows}


def set_published_entity_hash(entity_type: str, entity_key: str, payload_hash: str) -> None:
    now = utc_now_iso()
    conn = connect()
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO published_entities(entity_type, entity_key, payload_hash, updated_at)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(entity_type, entity_key) DO UPDATE
                SET payload_hash = excluded.payload_hash,
                    updated_at = excluded.updated_at
                """,
                (entity_type, entity_key, payload_hash, now),
            )
    finally:
        conn.close()


def delete_published_entity_hash(entity_type: str, entity_key: str) -> None:
    conn = connect()
    try:
        with conn:
            conn.execute(
                "DELETE FROM published_entities WHERE entity_type = ? AND entity_key = ?",
                (entity_type, entity_key),
            )
    finally:
        conn.close()


def upsert_materialized_expense(event: Dict[str, Any], payload_hash: str) -> None:
    expense = dict(event.get("expense") or {})
    shares = list(event.get("shares") or [])
    expense_id = int(expense["expense_id"])
    source_event_time = str(event.get("emitted_at") or utc_now_iso())
    conn = connect()
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO expenses(
                    expense_id, date, updated_at, month, description, category, amount, currency,
                    payload_hash, source_event_time
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(expense_id) DO UPDATE
                SET date = excluded.date,
                    updated_at = excluded.updated_at,
                    month = excluded.month,
                    description = excluded.description,
                    category = excluded.category,
                    amount = excluded.amount,
                    currency = excluded.currency,
                    payload_hash = excluded.payload_hash,
                    source_event_time = excluded.source_event_time
                """,
                (
                    expense_id,
                    str(expense.get("date") or ""),
                    str(expense.get("updated_at") or ""),
                    str(expense.get("month") or ""),
                    str(expense.get("description") or ""),
                    str(expense.get("category") or ""),
                    float(expense.get("amount") or 0.0),
                    str(expense.get("currency") or ""),
                    payload_hash,
                    source_event_time,
                ),
            )
            conn.execute("DELETE FROM shares WHERE expense_id = ?", (expense_id,))
            if shares:
                conn.executemany(
                    """
                    INSERT INTO shares(expense_id, user_id, person, month, owed_share, paid_share)
                    VALUES(?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            expense_id,
                            int(share.get("user_id")),
                            str(share.get("person") or ""),
                            str(share.get("month") or ""),
                            float(share.get("owed_share") or 0.0),
                            float(share.get("paid_share") or 0.0),
                        )
                        for share in shares
                        if share.get("user_id") is not None
                    ],
                )
    finally:
        conn.close()


def delete_materialized_expense(expense_id: int) -> None:
    conn = connect()
    try:
        with conn:
            conn.execute("DELETE FROM expenses WHERE expense_id = ?", (int(expense_id),))
    finally:
        conn.close()


def replace_group_state(group_state: Dict[str, Any]) -> None:
    group_id = int(group_state.get("group_id") or 0)
    if not group_id:
        raise ValueError("group_state.group_id is required")
    range_info = dict(group_state.get("range") or {})
    conn = connect()
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO group_state(group_id, group_name, generated_at, start_month, end_month, debt_edges_json)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(group_id) DO UPDATE
                SET group_name = excluded.group_name,
                    generated_at = excluded.generated_at,
                    start_month = excluded.start_month,
                    end_month = excluded.end_month,
                    debt_edges_json = excluded.debt_edges_json
                """,
                (
                    group_id,
                    str(group_state.get("group_name") or ""),
                    str(group_state.get("generated_at") or utc_now_iso()),
                    str(range_info.get("start_month") or ""),
                    str(range_info.get("end_month") or ""),
                    _json_dump(group_state.get("debt_edges") or []),
                ),
            )
    finally:
        conn.close()


def _dataframe_from_rows(rows: List[sqlite3.Row], columns: List[str]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame([dict(row) for row in rows], columns=columns)


def load_materialized_state() -> Dict[str, Any]:
    conn = connect()
    try:
        group_row = conn.execute(
            """
            SELECT group_id, group_name, generated_at, start_month, end_month, debt_edges_json
            FROM group_state
            ORDER BY generated_at DESC
            LIMIT 1
            """
        ).fetchone()
        raw_rows = conn.execute(
            """
            SELECT expense_id, date, updated_at, month, description, category, amount, currency
            FROM expenses
            ORDER BY date DESC, expense_id DESC
            """
        ).fetchall()
        share_rows = conn.execute(
            """
            SELECT expense_id, user_id, person, month, owed_share, paid_share
            FROM shares
            ORDER BY expense_id DESC, user_id ASC
            """
        ).fetchall()
    finally:
        conn.close()

    raw_df = _dataframe_from_rows(raw_rows, _RAW_EXPENSE_COLUMNS)
    shares_df = _dataframe_from_rows(share_rows, _SHARE_COLUMNS)

    if "expense_id" in raw_df.columns:
        raw_df["expense_id"] = pd.to_numeric(raw_df["expense_id"], errors="coerce").astype("Int64")
    if "amount" in raw_df.columns:
        raw_df["amount"] = pd.to_numeric(raw_df["amount"], errors="coerce").fillna(0.0)
    for col in ("date", "updated_at", "month", "description", "category", "currency"):
        if col in raw_df.columns:
            raw_df[col] = raw_df[col].astype(str)

    if "expense_id" in shares_df.columns:
        shares_df["expense_id"] = pd.to_numeric(shares_df["expense_id"], errors="coerce").astype("Int64")
    if "user_id" in shares_df.columns:
        shares_df["user_id"] = pd.to_numeric(shares_df["user_id"], errors="coerce").astype("Int64")
    for col in ("owed_share", "paid_share"):
        if col in shares_df.columns:
            shares_df[col] = pd.to_numeric(shares_df[col], errors="coerce").fillna(0.0)
    for col in ("person", "month"):
        if col in shares_df.columns:
            shares_df[col] = shares_df[col].astype(str)

    debt_edges: List[Dict[str, Any]] = []
    group_id = 0
    group_name = ""
    generated_at = None
    range_info = {"start_month": "", "end_month": ""}
    if group_row is not None:
        group_id = int(group_row["group_id"] or 0)
        group_name = str(group_row["group_name"] or "")
        generated_at = str(group_row["generated_at"] or "")
        range_info = {
            "start_month": str(group_row["start_month"] or ""),
            "end_month": str(group_row["end_month"] or ""),
        }
        try:
            debt_edges = list(json.loads(group_row["debt_edges_json"] or "[]"))
        except Exception:
            debt_edges = []

    return {
        "group_id": group_id,
        "group_name": group_name,
        "generated_at": generated_at,
        "range": range_info,
        "debt_edges": debt_edges,
        "raw_df": raw_df,
        "shares_df": shares_df,
        "producer_state": load_scope_state("producer"),
        "consumer_state": load_scope_state("consumer"),
        "pending_refresh_count": pending_refresh_count(),
    }
