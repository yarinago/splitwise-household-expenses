#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import threading
import traceback
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit

import pandas as pd
from dotenv import load_dotenv
from flask import Flask, jsonify, redirect, render_template, request, url_for

# Load .env before importing the compute module because it validates env vars at import time.
load_dotenv()

import splitwise_to_excel as core


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def int_env(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid integer value for {name}: {raw}") from exc
    return max(value, minimum)


def coerce_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        raw = str(value).strip().replace(",", "")
        if not raw:
            return 0.0
        return float(raw)
    except Exception:
        return 0.0


def format_ils(value: Any) -> str:
    return f"\u20aa{coerce_float(value):,.2f}"


def resolve_user_id(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        raw = value.strip()
        return int(raw) if raw.isdigit() else None
    if isinstance(value, dict):
        for key in ("id", "user_id"):
            if key in value:
                return resolve_user_id(value.get(key))
        return None

    getter = getattr(value, "getId", None)
    if callable(getter):
        return resolve_user_id(getter())

    if hasattr(value, "id"):
        attr = getattr(value, "id")
        if not callable(attr):
            return resolve_user_id(attr)

    user_getter = getattr(value, "getUser", None)
    if callable(user_getter):
        return resolve_user_id(user_getter())

    return None


def extract_simplified_debts(group) -> List[Any]:
    debts = None
    getter = getattr(group, "getSimplifiedDebts", None)
    if callable(getter):
        try:
            debts = getter()
        except Exception:
            debts = None
    if debts is None:
        debts = getattr(group, "simplified_debts", None)
    return debts or []


def build_debt_edges(group, id_to_name: Dict[int, str]) -> List[Dict[str, Any]]:
    edges: List[Dict[str, Any]] = []
    for debt in extract_simplified_debts(group):
        if isinstance(debt, dict):
            from_raw = debt.get("from") or debt.get("from_user") or debt.get("from_user_id")
            to_raw = debt.get("to") or debt.get("to_user") or debt.get("to_user_id")
            amount_raw = debt.get("amount")
        else:
            from_raw = (
                core._safe_attr(debt, "getFrom")
                or core._safe_attr(debt, "from_user")
                or core._safe_attr(debt, "getFromUser")
            )
            to_raw = (
                core._safe_attr(debt, "getTo")
                or core._safe_attr(debt, "to_user")
                or core._safe_attr(debt, "getToUser")
            )
            if hasattr(debt, "amount"):
                amount_raw = getattr(debt, "amount")
            else:
                amount_raw = core._safe_attr(debt, "getAmount")

        # Splitwise SDK response direction appears opposite in this setup;
        # flip direction so UI text "X owes Y" matches observed balances.
        from_id = resolve_user_id(to_raw)
        to_id = resolve_user_id(from_raw)
        amount = round(coerce_float(amount_raw), 2)
        if amount <= 0:
            continue

        from_name = id_to_name.get(from_id, str(from_id) if from_id is not None else "Unknown")
        to_name = id_to_name.get(to_id, str(to_id) if to_id is not None else "Unknown")
        edges.append(
            {
                "from_id": from_id,
                "to_id": to_id,
                "from_name": from_name,
                "to_name": to_name,
                "amount": amount,
            }
        )

    edges.sort(key=lambda x: x["amount"], reverse=True)
    return edges


def dataframe_to_table(df: pd.DataFrame, include_index: bool = False) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"columns": [], "rows": []}

    out = df.copy()
    if include_index:
        index_name = out.index.name or "index"
        out = out.reset_index().rename(columns={"index": index_name})

    for col in out.columns:
        if pd.api.types.is_numeric_dtype(out[col]):
            out[col] = pd.to_numeric(out[col], errors="coerce").round(2)

    return {"columns": list(out.columns), "rows": json.loads(out.to_json(orient="records"))}


def build_person_month_owes_pivot(person_df: pd.DataFrame, id_to_name: Dict[int, str]) -> pd.DataFrame:
    if person_df is None or person_df.empty:
        return pd.DataFrame()

    df = person_df.copy()
    wanted_ids = list(id_to_name.keys())
    if "user_id" not in df.columns:
        return pd.DataFrame()
    df = df[df["user_id"].isin(wanted_ids)].copy()
    if df.empty:
        return pd.DataFrame()
    if "owed_share" not in df.columns:
        return pd.DataFrame()

    df["owed_share"] = pd.to_numeric(df.get("owed_share"), errors="coerce").fillna(0.0)
    if "paid_share" in df.columns:
        df["paid_share"] = pd.to_numeric(df.get("paid_share"), errors="coerce").fillna(0.0)
    else:
        df["paid_share"] = 0.0

    # Net amount this person still owes on each expense.
    df["net_owes"] = (df["owed_share"] - df["paid_share"]).clip(lower=0.0)
    df["person"] = df.apply(
        lambda r: id_to_name.get(int(r["user_id"]) if pd.notna(r["user_id"]) else -1, r.get("person", "Unknown")),
        axis=1,
    )

    pivot = pd.pivot_table(
        df,
        index="month",
        columns="person",
        values="net_owes",
        aggfunc="sum",
        fill_value=0.0,
        sort=True,
    ).sort_index(ascending=False)

    ordered_cols = [id_to_name[k] for k in wanted_ids if id_to_name[k] in pivot.columns]
    pivot = pivot.reindex(columns=ordered_cols, fill_value=0.0)
    pivot["Total"] = pivot.sum(axis=1)
    return pivot


def build_dashboard_snapshot(max_recent_rows: int) -> Dict[str, Any]:
    group_id = core.DEFAULT_GROUP_ID or int(os.getenv("SPLITWISE_GROUP_ID", "0") or "0")
    if not group_id:
        raise RuntimeError("SPLITWISE_GROUP_ID is required.")

    client_id = (os.getenv("SPLITWISE_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("SPLITWISE_CLIENT_SECRET") or "").strip()
    token_raw = os.getenv("SPLITWISE_ACCESS_TOKEN_JSON")
    if not client_id or not client_secret or not token_raw:
        raise RuntimeError(
            "Missing credentials: SPLITWISE_CLIENT_ID, SPLITWISE_CLIENT_SECRET, SPLITWISE_ACCESS_TOKEN_JSON."
        )

    start_ym = (os.getenv("SPLITWISE_FIRST_MONTH", core.FIRST_MONTH) or "2008-01").strip()
    end_ym = core.ym_today()
    token = core.resolve_token(token_raw)

    sw = core.make_client(client_id, client_secret, token)
    group = core.fetch_group(sw, group_id)
    group_name = group.getName()

    expenses = core.fetch_expenses_all_history(sw, group_id, start_ym, end_ym)
    raw_df = core.normalize_expenses(expenses)
    shares_df = core.normalize_person_shares(expenses)
    raw_df, shares_df = core.apply_custom_exclusions(raw_df, shares_df)
    raw_wide = core.widen_raw_with_member_owed(raw_df, shares_df, core.MEMBER_ID_TO_NAME)
    monthly_by_category, _ = core.build_pivots(raw_wide)
    per_person_month = build_person_month_owes_pivot(shares_df, core.MEMBER_ID_TO_NAME)

    debt_edges = build_debt_edges(group, core.MEMBER_ID_TO_NAME)
    member_names = list(core.MEMBER_ID_TO_NAME.values())
    member_owes: Dict[str, float] = {name: 0.0 for name in member_names}
    member_receives: Dict[str, float] = {name: 0.0 for name in member_names}
    for edge in debt_edges:
        from_name = edge["from_name"]
        to_name = edge["to_name"]
        amount = edge["amount"]
        if from_name in member_owes:
            member_owes[from_name] += amount
        if to_name in member_receives:
            member_receives[to_name] += amount

    member_balances = []
    for name in member_names:
        owes = round(member_owes.get(name, 0.0), 2)
        receives = round(member_receives.get(name, 0.0), 2)
        member_balances.append(
            {
                "name": name,
                "owes": owes,
                "to_receive": receives,
            }
        )

    raw_columns = [
        "date",
        "month",
        "description",
        "category",
        "amount",
        "currency",
        "expense_id",
    ]
    raw_columns.extend(f"{name}_owes" for name in member_names)
    present_columns = [c for c in raw_columns if c in raw_wide.columns]

    if raw_wide.empty:
        raw_expenses = raw_wide.copy()
    else:
        raw_expenses = raw_wide[present_columns].sort_values(by="date", ascending=False)
    recent_expenses = raw_expenses.head(max_recent_rows)

    currencies = []
    if "currency" in raw_wide.columns and not raw_wide.empty:
        currencies = sorted({str(v) for v in raw_wide["currency"].dropna().tolist() if str(v).strip()})

    total_expenses = 0.0
    if "amount" in raw_wide.columns and not raw_wide.empty:
        total_expenses = round(float(pd.to_numeric(raw_wide["amount"], errors="coerce").fillna(0.0).sum()), 2)

    months = []
    if "month" in raw_wide.columns and not raw_wide.empty:
        months = sorted({str(m) for m in raw_wide["month"].dropna().tolist() if str(m).strip()}, reverse=True)

    monthly_totals = []
    if "month" in raw_wide.columns and "amount" in raw_wide.columns and not raw_wide.empty:
        totals_df = (
            raw_wide.groupby("month", as_index=False)["amount"]
            .sum()
            .sort_values(by="month", ascending=False)
        )
        monthly_totals = [
            {"month": str(row["month"]), "amount": round(float(row["amount"]), 2)}
            for _, row in totals_df.iterrows()
        ]

    category_totals_all = []
    if "category" in raw_wide.columns and "amount" in raw_wide.columns and not raw_wide.empty:
        grouped = (
            raw_wide.groupby("category", as_index=False)["amount"]
            .sum()
            .sort_values(by="amount", ascending=False)
        )
        category_totals_all = [
            {"label": str(row["category"]), "amount": round(float(row["amount"]), 2)}
            for _, row in grouped.iterrows()
            if coerce_float(row["amount"]) > 0
        ]

    category_totals_by_month: Dict[str, List[Dict[str, Any]]] = {}
    if "month" in raw_wide.columns and "category" in raw_wide.columns and "amount" in raw_wide.columns and not raw_wide.empty:
        for month, month_df in raw_wide.groupby("month"):
            grouped = (
                month_df.groupby("category", as_index=False)["amount"]
                .sum()
                .sort_values(by="amount", ascending=False)
            )
            category_totals_by_month[str(month)] = [
                {"label": str(row["category"]), "amount": round(float(row["amount"]), 2)}
                for _, row in grouped.iterrows()
                if coerce_float(row["amount"]) > 0
            ]
    categories = sorted({row["label"] for row in category_totals_all})

    category_month_series: Dict[str, List[Dict[str, Any]]] = {}
    if "month" in raw_wide.columns and "category" in raw_wide.columns and "amount" in raw_wide.columns and not raw_wide.empty:
        category_month_df = (
            raw_wide.groupby(["month", "category"], as_index=False)["amount"]
            .sum()
            .sort_values(by=["category", "month"], ascending=[True, False])
        )
        for category, cat_df in category_month_df.groupby("category"):
            category_month_series[str(category)] = [
                {"month": str(row["month"]), "amount": round(float(row["amount"]), 2)}
                for _, row in cat_df.iterrows()
                if coerce_float(row["amount"]) > 0
            ]

    person_columns = [f"{name}_owes" for name in member_names if f"{name}_owes" in raw_wide.columns]
    person_totals_all = [
        {
            "label": col[:-5],
            "amount": round(float(pd.to_numeric(raw_wide[col], errors="coerce").fillna(0.0).sum()), 2),
        }
        for col in person_columns
    ]

    person_totals_by_month: Dict[str, List[Dict[str, Any]]] = {}
    if "month" in raw_wide.columns and person_columns and not raw_wide.empty:
        by_month = raw_wide.groupby("month")[person_columns].sum().sort_index()
        for month, row in by_month.iterrows():
            person_totals_by_month[str(month)] = [
                {"label": col[:-5], "amount": round(float(row[col]), 2)}
                for col in person_columns
            ]

    return {
        "group_id": group_id,
        "group_name": group_name,
        "generated_at": utc_now_iso(),
        "months": months,
        "range": {"start_month": start_ym, "end_month": end_ym},
        "summary": {
            "expense_count": int(len(raw_wide)),
            "total_expenses": total_expenses,
            "currencies": currencies,
        },
        "member_balances": member_balances,
        "debt_edges": debt_edges,
        "charts": {
            "monthly_totals": monthly_totals,
            "category_totals_all": category_totals_all,
            "category_totals_by_month": category_totals_by_month,
            "category_month_series": category_month_series,
            "categories": categories,
            "person_totals_all": person_totals_all,
            "person_totals_by_month": person_totals_by_month,
        },
        "tables": {
            "recent_expenses": dataframe_to_table(recent_expenses),
            "raw_expenses": dataframe_to_table(raw_expenses),
            "monthly_by_category": dataframe_to_table(monthly_by_category, include_index=True),
            "per_person_month": dataframe_to_table(per_person_month, include_index=True),
        },
    }


def normalize_month_filter(snapshot: Dict[str, Any], selected_month: str) -> str:
    selected = (selected_month or "all").strip()
    if selected == "all":
        return "all"
    months = snapshot.get("months", [])
    if selected in months:
        return selected
    return "all"


def normalize_category_filter(snapshot: Dict[str, Any], selected_category: str) -> str:
    categories = snapshot.get("charts", {}).get("categories", [])
    selected = (selected_category or "").strip()
    if selected in categories:
        return selected
    return categories[0] if categories else ""


def color_for_label(label: str) -> str:
    # Deterministic color with higher diversity than a small static palette.
    digest = hashlib.sha256((label or "unknown").encode("utf-8")).hexdigest()
    hue = int(digest[:8], 16) % 360
    saturation = 62 + (int(digest[8:10], 16) % 24)  # 62-85
    lightness = 40 + (int(digest[10:12], 16) % 20)  # 40-59
    return f"hsl({hue}deg {saturation}% {lightness}%)"


def bars_from_items(
    items: List[Dict[str, Any]],
    label_key: str,
    value_key: str,
    limit: int,
    sort_by_value: bool = True,
    color_by_label: bool = False,
) -> List[Dict[str, Any]]:
    rows = [row for row in items if coerce_float(row.get(value_key)) > 0]
    if sort_by_value:
        rows = sorted(rows, key=lambda x: coerce_float(x.get(value_key)), reverse=True)
    if limit > 0:
        rows = rows[:limit]
    if not rows:
        return []
    max_val = max(coerce_float(row.get(value_key)) for row in rows) or 1.0
    out = []
    for row in rows:
        amount = round(coerce_float(row.get(value_key)), 2)
        pct = round((amount / max_val) * 100.0, 1)
        label = str(row.get(label_key, ""))
        result_row = {"label": label, "amount": amount, "pct": pct}
        if color_by_label:
            result_row["color"] = color_for_label(label)
        out.append(result_row)
    return out


def build_person_month_groups(snapshot: Dict[str, Any], month_filter: str) -> List[Dict[str, Any]]:
    person_table = snapshot.get("tables", {}).get("per_person_month", {})
    rows = list(person_table.get("rows", []))
    columns = list(person_table.get("columns", []))
    person_names = [c for c in columns if c not in ("month", "Total", "index")]

    if not rows or not person_names:
        return []

    if month_filter != "all":
        rows = [row for row in rows if str(row.get("month", "")) == month_filter]

    month_rank = {m: idx for idx, m in enumerate(snapshot.get("months", []))}
    rows = sorted(
        rows,
        key=lambda row: month_rank.get(str(row.get("month", "")), 10_000),
    )

    max_val = max(
        (coerce_float(row.get(person_name)) for row in rows for person_name in person_names),
        default=0.0,
    )
    max_val = max(max_val, 1.0)

    groups: List[Dict[str, Any]] = []
    for row in rows:
        month = str(row.get("month", ""))
        people = []
        for person_name in person_names:
            amount = round(coerce_float(row.get(person_name)), 2)
            people.append(
                {
                    "name": person_name,
                    "amount": amount,
                    "pct": round((amount / max_val) * 100.0, 1),
                    "color": color_for_label(person_name),
                }
            )
        groups.append({"month": month, "people": people})

    return groups


def build_dashboard_view(
    snapshot: Dict[str, Any],
    selected_category_month: str,
    selected_person_month: str,
    selected_category: str,
) -> Dict[str, Any]:
    category_month_filter = normalize_month_filter(snapshot, selected_category_month)
    person_month_filter = normalize_month_filter(snapshot, selected_person_month)
    category_filter = normalize_category_filter(snapshot, selected_category)
    raw_rows = snapshot["tables"]["raw_expenses"]["rows"]
    if category_month_filter == "all":
        scoped_raw = raw_rows
    else:
        scoped_raw = [row for row in raw_rows if str(row.get("month", "")) == category_month_filter]

    if category_month_filter == "all":
        category_rows = snapshot["charts"]["category_totals_all"]
    else:
        category_rows = snapshot["charts"]["category_totals_by_month"].get(category_month_filter, [])

    debt_edges = snapshot.get("debt_edges", [])
    top_debt = debt_edges[0] if debt_edges else None
    category_month_rows = snapshot["charts"]["category_month_series"].get(category_filter, [])
    person_month_groups = build_person_month_groups(snapshot, person_month_filter)

    return {
        "selected_category_month": category_month_filter,
        "selected_person_month": person_month_filter,
        "selected_category": category_filter,
        "categories": snapshot.get("charts", {}).get("categories", []),
        "months": snapshot.get("months", []),
        "scoped_count": len(scoped_raw),
        "scoped_total": round(sum(coerce_float(row.get("amount")) for row in scoped_raw), 2),
        "top_debt": top_debt,
        "member_balances": snapshot.get("member_balances", []),
        "monthly_total_bars": bars_from_items(
            snapshot["charts"]["monthly_totals"],
            "month",
            "amount",
            limit=120,
            sort_by_value=False,
        ),
        "category_bars": bars_from_items(
            category_rows,
            "label",
            "amount",
            limit=20,
            sort_by_value=True,
            color_by_label=True,
        ),
        "person_month_groups": person_month_groups,
        "category_month_bars": bars_from_items(
            category_month_rows,
            "month",
            "amount",
            limit=120,
            sort_by_value=False,
        ),
    }


def contains_filter_value(row: Dict[str, Any], filter_text: str) -> bool:
    if not filter_text:
        return True
    needle = filter_text.lower()
    fields = [str(v) for v in row.values()]
    return needle in " ".join(fields).lower()


def build_tables_view(snapshot: Dict[str, Any], selected_month: str, filter_text: str, table_limit: int) -> Dict[str, Any]:
    month_filter = normalize_month_filter(snapshot, selected_month)
    query = (filter_text or "").strip()

    raw = snapshot["tables"]["raw_expenses"]
    monthly = snapshot["tables"]["monthly_by_category"]
    person = snapshot["tables"]["per_person_month"]

    raw_rows = raw["rows"]
    if month_filter != "all":
        raw_rows = [row for row in raw_rows if str(row.get("month", "")) == month_filter]
    if query:
        raw_rows = [row for row in raw_rows if contains_filter_value(row, query)]

    monthly_rows = monthly["rows"]
    if month_filter != "all":
        monthly_rows = [row for row in monthly_rows if str(row.get("month", "")) == month_filter]
    if query:
        monthly_rows = [row for row in monthly_rows if contains_filter_value(row, query)]

    person_rows = person["rows"]
    if month_filter != "all":
        person_rows = [row for row in person_rows if str(row.get("month", "")) == month_filter]
    if query:
        person_rows = [row for row in person_rows if contains_filter_value(row, query)]

    limited_raw_rows = raw_rows[:table_limit]
    raw_money_columns = [c for c in raw["columns"] if c == "amount" or c.endswith("_owes")]
    monthly_money_columns = [c for c in monthly["columns"] if c != "month"]
    person_money_columns = [c for c in person["columns"] if c != "month"]

    return {
        "selected_month": month_filter,
        "months": snapshot.get("months", []),
        "query": query,
        "raw_total": len(raw_rows),
        "raw_shown": len(limited_raw_rows),
        "table_limit": table_limit,
        "raw": {"columns": raw["columns"], "rows": limited_raw_rows, "money_columns": raw_money_columns},
        "monthly": {"columns": monthly["columns"], "rows": monthly_rows, "money_columns": monthly_money_columns},
        "person": {"columns": person["columns"], "rows": person_rows, "money_columns": person_money_columns},
    }


class DashboardState:
    def __init__(self, refresh_seconds: int, max_recent_rows: int):
        self.refresh_seconds = refresh_seconds
        self.max_recent_rows = max_recent_rows
        self._lock = threading.Lock()
        self._snapshot: Optional[Dict[str, Any]] = None
        self._last_error: Optional[str] = None
        self._last_refresh_started: Optional[str] = None
        self._last_refresh_finished: Optional[str] = None
        self._refresh_in_progress = False
        self._stop_event = threading.Event()
        self._loop_thread = threading.Thread(
            target=self._refresh_loop,
            name="splitwise-refresh-loop",
            daemon=True,
        )

    def start(self) -> None:
        self._loop_thread.start()

    def _refresh_loop(self) -> None:
        self.refresh()
        while not self._stop_event.wait(self.refresh_seconds):
            self.refresh()

    def refresh(self) -> None:
        with self._lock:
            if self._refresh_in_progress:
                return
            self._refresh_in_progress = True
            self._last_refresh_started = utc_now_iso()

        snapshot: Optional[Dict[str, Any]] = None
        error: Optional[str] = None
        try:
            snapshot = build_dashboard_snapshot(self.max_recent_rows)
        except Exception as exc:
            error = f"{exc.__class__.__name__}: {exc}"
            traceback.print_exc()

        with self._lock:
            if snapshot is not None:
                self._snapshot = snapshot
            self._last_error = error
            self._last_refresh_finished = utc_now_iso()
            self._refresh_in_progress = False

    def trigger_refresh_async(self) -> bool:
        with self._lock:
            if self._refresh_in_progress:
                return False
        threading.Thread(target=self.refresh, name="splitwise-manual-refresh", daemon=True).start()
        return True

    def model(self) -> Dict[str, Any]:
        with self._lock:
            snapshot = self._snapshot
            last_error = self._last_error
            last_refresh_started = self._last_refresh_started
            last_refresh_finished = self._last_refresh_finished
            refresh_in_progress = self._refresh_in_progress

        status = "ready"
        if snapshot is None and refresh_in_progress:
            status = "warming_up"
        elif snapshot is None and last_error:
            status = "error"
        elif refresh_in_progress:
            status = "refreshing"

        return {
            "status": status,
            "refresh_in_progress": refresh_in_progress,
            "refresh_seconds": self.refresh_seconds,
            "last_refresh_started": last_refresh_started,
            "last_refresh_finished": last_refresh_finished,
            "last_error": last_error,
            "snapshot": snapshot,
        }


REFRESH_SECONDS = int_env("SPLITWISE_REFRESH_SECONDS", default=900, minimum=30)
MAX_RECENT_ROWS = int_env("SPLITWISE_RECENT_EXPENSES_LIMIT", default=30, minimum=5)
TABLE_LIMIT = int_env("SPLITWISE_TABLE_LIMIT", default=500, minimum=50)

state = DashboardState(refresh_seconds=REFRESH_SECONDS, max_recent_rows=MAX_RECENT_ROWS)
state.start()

app = Flask(__name__)


@app.template_filter("ils")
def ils_filter(value: Any) -> str:
    return format_ils(value)


@app.get("/")
def index():
    model = state.model()
    snapshot = model["snapshot"]
    view = None
    if snapshot is not None:
        selected_category_month = request.args.get("category_month", "all")
        selected_person_month = request.args.get("person_month", "all")
        selected_category = request.args.get("category", "")
        view = build_dashboard_view(
            snapshot,
            selected_category_month,
            selected_person_month,
            selected_category,
        )
    return render_template("index.html", model=model, view=view)


@app.get("/tables")
def tables():
    model = state.model()
    snapshot = model["snapshot"]
    view = None
    if snapshot is not None:
        selected_month = request.args.get("month", "all")
        query = request.args.get("q", "")
        view = build_tables_view(snapshot, selected_month, query, TABLE_LIMIT)
    return render_template("tables.html", model=model, view=view)


def next_redirect_target(default_endpoint: str) -> str:
    next_path = (request.form.get("next") or "").strip()
    if next_path and next_path.startswith("/"):
        return next_path

    referer = request.headers.get("Referer", "")
    if referer:
        parsed = urlsplit(referer)
        if parsed.path and parsed.path.startswith("/"):
            if parsed.query:
                return f"{parsed.path}?{parsed.query}"
            return parsed.path

    return url_for(default_endpoint)


@app.post("/refresh")
def refresh():
    state.trigger_refresh_async()
    return redirect(next_redirect_target("index"))


@app.get("/api/dashboard")
def api_dashboard():
    model = state.model()
    status_code = 200 if model["snapshot"] is not None else 503
    return jsonify(model), status_code


@app.post("/api/refresh")
def api_refresh():
    scheduled = state.trigger_refresh_async()
    return jsonify({"scheduled": scheduled, "timestamp": utc_now_iso()}), (202 if scheduled else 409)


@app.get("/healthz")
def healthz():
    return jsonify({"status": "ok", "time": utc_now_iso()})


@app.get("/readyz")
def readyz():
    model = state.model()
    if model["snapshot"] is None:
        return jsonify({"status": "not_ready", "last_error": model["last_error"]}), 503
    return jsonify({"status": "ready", "last_refresh_finished": model["last_refresh_finished"]})


if __name__ == "__main__":
    host = os.getenv("SPLITWISE_APP_HOST", "0.0.0.0").strip() or "0.0.0.0"
    port = int_env("PORT", default=8080, minimum=1)
    app.run(host=host, port=port)
