#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import socket
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from prometheus_client import Counter, Gauge, start_http_server

import read_model
import splitwise_to_excel as core

try:
    from confluent_kafka import Consumer, KafkaError, Producer
except Exception as exc:  # pragma: no cover - import error surfaced at runtime
    Consumer = None  # type: ignore[assignment]
    KafkaError = None  # type: ignore[assignment]
    Producer = None  # type: ignore[assignment]
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None


PRODUCER_RUNS_TOTAL = Counter(
    "splitwise_kafka_producer_runs_total",
    "Number of producer refresh attempts.",
    ["status"],
)
PRODUCER_MESSAGES_TOTAL = Counter(
    "splitwise_kafka_producer_messages_total",
    "Number of Kafka messages produced by event type.",
    ["event_type"],
)
PRODUCER_LAST_SUCCESS_UNIX = Gauge(
    "splitwise_kafka_producer_last_success_unixtime",
    "Unix timestamp of the last successful producer refresh.",
)
CONSUMER_MESSAGES_TOTAL = Counter(
    "splitwise_kafka_consumer_messages_total",
    "Number of Kafka messages consumed by event type.",
    ["event_type"],
)
CONSUMER_LAST_SUCCESS_UNIX = Gauge(
    "splitwise_kafka_consumer_last_success_unixtime",
    "Unix timestamp of the last successfully processed consumer message.",
)
CONSUMER_ASSIGNED_PARTITIONS = Gauge(
    "splitwise_kafka_consumer_assigned_partitions",
    "Number of partitions assigned to the consumer process.",
)
CONSUMER_LAST_OFFSET = Gauge(
    "splitwise_kafka_consumer_last_offset",
    "Last committed consumer offset by topic and partition.",
    ["topic", "partition"],
)
LOADGEN_MESSAGES_TOTAL = Counter(
    "splitwise_kafka_loadgen_messages_total",
    "Number of synthetic load-generator messages produced.",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def unix_now() -> float:
    return datetime.now(timezone.utc).timestamp()


def _require_kafka_client() -> None:
    if Producer is None or Consumer is None:
        raise RuntimeError(
            f"Kafka client dependency is not available: {_IMPORT_ERROR}"
        )


def _read_required_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise RuntimeError(f"{name} is required for Kafka mode.")
    return value


def _read_int_env(name: str, default: int, minimum: int = 1) -> int:
    raw = (os.getenv(name) or str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid integer value for {name}: {raw}") from exc
    return max(value, minimum)


def _read_float_env(name: str, default: float, minimum: float = 0.0) -> float:
    raw = (os.getenv(name) or str(default)).strip()
    try:
        value = float(raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid float value for {name}: {raw}") from exc
    return max(value, minimum)


def kafka_topic() -> str:
    return (os.getenv("KAFKA_TOPIC") or "splitwise.expenses.v1").strip() or "splitwise.expenses.v1"


def kafka_group_id() -> str:
    return (
        os.getenv("KAFKA_GROUP_ID")
        or "splitwise-dashboard-materializer"
    ).strip() or "splitwise-dashboard-materializer"


def metrics_port() -> int:
    return _read_int_env("METRICS_PORT", default=9090, minimum=1)


def _hostname() -> str:
    return socket.gethostname().strip() or "unknown-host"


def _json_bytes(payload: Dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _payload_hash(payload: Dict[str, Any]) -> str:
    return hashlib.sha256(_json_bytes(payload)).hexdigest()


def _base_kafka_config(client_suffix: str) -> Dict[str, Any]:
    conf: Dict[str, Any] = {
        "bootstrap.servers": _read_required_env("KAFKA_BOOTSTRAP_SERVERS"),
        "client.id": f"splitwise-{client_suffix}-{_hostname()}",
    }
    optional_map = {
        "KAFKA_SECURITY_PROTOCOL": "security.protocol",
        "KAFKA_SASL_MECHANISM": "sasl.mechanism",
        "KAFKA_SASL_USERNAME": "sasl.username",
        "KAFKA_SASL_PASSWORD": "sasl.password",
        "KAFKA_SSL_CA_LOCATION": "ssl.ca.location",
    }
    for env_name, conf_key in optional_map.items():
        value = (os.getenv(env_name) or "").strip()
        if value:
            conf[conf_key] = value
    return conf


def _start_metrics_server() -> None:
    start_http_server(metrics_port())


def _safe_attr(obj: Any, getter_name: str, default: Any = None) -> Any:
    try:
        getter = getattr(obj, getter_name)
        return getter() if callable(getter) else getter
    except Exception:
        return default


def _resolve_user_id(value: Any) -> Optional[int]:
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
                return _resolve_user_id(value.get(key))
        return None
    getter = getattr(value, "getId", None)
    if callable(getter):
        return _resolve_user_id(getter())
    if hasattr(value, "id"):
        attr = getattr(value, "id")
        if not callable(attr):
            return _resolve_user_id(attr)
    user_getter = getattr(value, "getUser", None)
    if callable(user_getter):
        return _resolve_user_id(user_getter())
    return None


def _extract_simplified_debts(group: Any) -> List[Any]:
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


def _debt_direction_mode() -> str:
    mode = (os.getenv("SPLITWISE_DEBT_DIRECTION", "reverse") or "reverse").strip().lower()
    return mode if mode in {"normal", "reverse"} else "reverse"


def build_debt_edges(group: Any, id_to_name: Dict[int, str]) -> List[Dict[str, Any]]:
    edges: List[Dict[str, Any]] = []
    direction = _debt_direction_mode()
    for debt in _extract_simplified_debts(group):
        if isinstance(debt, dict):
            from_raw = debt.get("from") or debt.get("from_user") or debt.get("from_user_id")
            to_raw = debt.get("to") or debt.get("to_user") or debt.get("to_user_id")
            amount_raw = debt.get("amount")
        else:
            from_raw = (
                _safe_attr(debt, "getFrom")
                or _safe_attr(debt, "from_user")
                or _safe_attr(debt, "getFromUser")
            )
            to_raw = (
                _safe_attr(debt, "getTo")
                or _safe_attr(debt, "to_user")
                or _safe_attr(debt, "getToUser")
            )
            amount_raw = getattr(debt, "amount", None)
            if amount_raw is None:
                amount_raw = _safe_attr(debt, "getAmount")

        if direction == "reverse":
            from_id = _resolve_user_id(to_raw)
            to_id = _resolve_user_id(from_raw)
        else:
            from_id = _resolve_user_id(from_raw)
            to_id = _resolve_user_id(to_raw)

        amount = round(float(core._coerce_float(amount_raw, 0.0)), 2)
        if amount <= 0:
            continue
        edges.append(
            {
                "from_id": from_id,
                "to_id": to_id,
                "from_name": id_to_name.get(from_id, str(from_id) if from_id is not None else "Unknown"),
                "to_name": id_to_name.get(to_id, str(to_id) if to_id is not None else "Unknown"),
                "amount": amount,
            }
        )
    edges.sort(key=lambda row: row["amount"], reverse=True)
    return edges


def _shares_for_expense(shares_df: pd.DataFrame, expense_id: int) -> List[Dict[str, Any]]:
    if shares_df.empty:
        return []
    scoped = shares_df[shares_df["expense_id"] == expense_id].copy()
    if scoped.empty:
        return []
    scoped = scoped.sort_values(by=["user_id"], ascending=True)
    rows: List[Dict[str, Any]] = []
    for _, row in scoped.iterrows():
        user_id = row.get("user_id")
        if pd.isna(user_id):
            continue
        rows.append(
            {
                "user_id": int(user_id),
                "person": str(row.get("person") or ""),
                "month": str(row.get("month") or ""),
                "owed_share": round(float(core._coerce_float(row.get("owed_share"), 0.0)), 2),
                "paid_share": round(float(core._coerce_float(row.get("paid_share"), 0.0)), 2),
            }
        )
    return rows


def _expense_event_payload(row: pd.Series, shares_df: pd.DataFrame, group_id: int) -> Dict[str, Any]:
    expense_id = int(row["expense_id"])
    payload = {
        "schema_version": 1,
        "event_type": "expense_upsert",
        "emitted_at": utc_now_iso(),
        "group_id": group_id,
        "expense": {
            "expense_id": expense_id,
            "date": str(row.get("date") or ""),
            "updated_at": str(row.get("updated_at") or ""),
            "month": str(row.get("month") or ""),
            "description": str(row.get("description") or ""),
            "category": str(row.get("category") or ""),
            "amount": round(float(core._coerce_float(row.get("amount"), 0.0)), 2),
            "currency": str(row.get("currency") or ""),
        },
        "shares": _shares_for_expense(shares_df, expense_id),
    }
    return payload


def _group_state_payload(
    group_id: int,
    group_name: str,
    start_ym: str,
    end_ym: str,
    debt_edges: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "schema_version": 1,
        "event_type": "group_state",
        "emitted_at": utc_now_iso(),
        "group_state": {
            "group_id": group_id,
            "group_name": group_name,
            "generated_at": utc_now_iso(),
            "range": {
                "start_month": start_ym,
                "end_month": end_ym,
            },
            "debt_edges": debt_edges,
        },
    }


def _delete_event_payload(group_id: int, expense_id: int) -> Dict[str, Any]:
    return {
        "schema_version": 1,
        "event_type": "expense_delete",
        "emitted_at": utc_now_iso(),
        "group_id": group_id,
        "expense_id": int(expense_id),
    }


def _fetch_splitwise_dataset() -> Dict[str, Any]:
    core.refresh_runtime_config(load_env=False, require_members=False)
    if not core.MEMBER_ID_TO_NAME:
        raise RuntimeError("SPLITWISE_MEMBERS is required for Kafka producer mode.")

    group_id = core.DEFAULT_GROUP_ID or int(os.getenv("SPLITWISE_GROUP_ID", "0") or "0")
    if not group_id:
        raise RuntimeError("SPLITWISE_GROUP_ID is required for Kafka producer mode.")

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
    expenses = core.fetch_expenses_all_history(sw, group_id, start_ym, end_ym)
    raw_df = core.normalize_expenses(expenses)
    shares_df = core.normalize_person_shares(expenses)
    raw_df, shares_df = core.apply_custom_exclusions(raw_df, shares_df)
    return {
        "group_id": group_id,
        "group_name": str(group.getName()),
        "start_month": start_ym,
        "end_month": end_ym,
        "raw_df": raw_df,
        "shares_df": shares_df,
        "debt_edges": build_debt_edges(group, core.MEMBER_ID_TO_NAME),
    }


def _producer_instance() -> Any:
    _require_kafka_client()
    conf = _base_kafka_config("producer")
    conf["acks"] = "all"
    return Producer(conf)


def _send_messages(messages: Iterable[Tuple[str, Dict[str, Any]]]) -> Dict[str, int]:
    producer = _producer_instance()
    topic = kafka_topic()
    failures: List[str] = []
    counts: Dict[str, int] = {}

    def on_delivery(err: Any, msg: Any) -> None:
        if err is not None:
            failures.append(str(err))

    queued = 0
    for key, payload in messages:
        event_type = str(payload.get("event_type") or "unknown")
        counts[event_type] = counts.get(event_type, 0) + 1
        producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=_json_bytes(payload),
            on_delivery=on_delivery,
        )
        producer.poll(0)
        queued += 1

    if queued == 0:
        return counts

    remaining = producer.flush(timeout=_read_float_env("KAFKA_FLUSH_TIMEOUT_SECONDS", default=30.0, minimum=1.0))
    if remaining:
        failures.append(f"{remaining} message(s) remained in producer queue after flush.")
    if failures:
        raise RuntimeError("; ".join(failures))
    for event_type, count in counts.items():
        PRODUCER_MESSAGES_TOTAL.labels(event_type=event_type).inc(count)
    return counts


def _set_producer_state(key: str, value: Any) -> None:
    read_model.set_state("producer", key, value)


def _set_consumer_state(key: str, value: Any) -> None:
    read_model.set_state("consumer", key, value)


def run_producer_once() -> Dict[str, int]:
    dataset = _fetch_splitwise_dataset()
    raw_df: pd.DataFrame = dataset["raw_df"]
    shares_df: pd.DataFrame = dataset["shares_df"]
    group_id = int(dataset["group_id"])

    published_hashes = read_model.load_published_entity_hashes("expense")
    current_ids: set[str] = set()
    messages: List[Tuple[str, Dict[str, Any]]] = []
    updated_hashes: Dict[str, str] = {}

    if not raw_df.empty:
        scoped_raw = raw_df.dropna(subset=["expense_id"]).copy()
        scoped_raw["expense_id"] = pd.to_numeric(scoped_raw["expense_id"], errors="coerce").astype("Int64")
        scoped_raw = scoped_raw.dropna(subset=["expense_id"]).sort_values(by=["date", "expense_id"], ascending=[False, False])
        for _, row in scoped_raw.iterrows():
            expense_id = int(row["expense_id"])
            payload = _expense_event_payload(row, shares_df, group_id)
            payload_hash = _payload_hash(payload)
            key = str(expense_id)
            current_ids.add(key)
            updated_hashes[key] = payload_hash
            if published_hashes.get(key) != payload_hash:
                messages.append((f"expense:{expense_id}", payload))

    stale_ids = sorted(set(published_hashes.keys()) - current_ids, key=lambda value: int(value))
    for stale_id in stale_ids:
        messages.append((f"expense:{stale_id}", _delete_event_payload(group_id, int(stale_id))))

    messages.append(
        (
            f"group:{group_id}",
            _group_state_payload(
                group_id=group_id,
                group_name=str(dataset["group_name"]),
                start_ym=str(dataset["start_month"]),
                end_ym=str(dataset["end_month"]),
                debt_edges=list(dataset["debt_edges"]),
            ),
        )
    )

    counts = _send_messages(messages)
    for entity_key, payload_hash in updated_hashes.items():
        read_model.set_published_entity_hash("expense", entity_key, payload_hash)
    for stale_id in stale_ids:
        read_model.delete_published_entity_hash("expense", stale_id)
    if messages:
        read_model.mark_refresh_requests_handled()
    return counts


def run_producer_loop() -> None:
    _start_metrics_server()
    refresh_seconds = _read_int_env("SPLITWISE_REFRESH_SECONDS", default=900, minimum=30)
    while True:
        started_at = utc_now_iso()
        _set_producer_state("refresh_in_progress", True)
        _set_producer_state("last_refresh_started", started_at)
        try:
            counts = run_producer_once()
        except Exception as exc:
            PRODUCER_RUNS_TOTAL.labels(status="error").inc()
            _set_producer_state("last_error", f"{exc.__class__.__name__}: {exc}")
            _set_producer_state("last_refresh_finished", utc_now_iso())
            _set_producer_state("refresh_in_progress", False)
        else:
            PRODUCER_RUNS_TOTAL.labels(status="success").inc()
            PRODUCER_LAST_SUCCESS_UNIX.set(unix_now())
            _set_producer_state("last_error", "")
            _set_producer_state("last_refresh_finished", utc_now_iso())
            _set_producer_state("last_successful_refresh", utc_now_iso())
            _set_producer_state("last_published_counts", counts)
            _set_producer_state("refresh_in_progress", False)

        deadline = time.time() + refresh_seconds
        while time.time() < deadline:
            if read_model.pending_refresh_count() > 0:
                break
            time.sleep(1.0)


def _consumer_instance() -> Any:
    _require_kafka_client()
    conf = _base_kafka_config("consumer")
    conf["group.id"] = kafka_group_id()
    conf["enable.auto.commit"] = False
    conf["auto.offset.reset"] = (os.getenv("KAFKA_AUTO_OFFSET_RESET") or "earliest").strip() or "earliest"
    return Consumer(conf)


def _handle_consumer_event(payload: Dict[str, Any], payload_hash: str) -> str:
    event_type = str(payload.get("event_type") or "unknown")
    if event_type == "expense_upsert":
        read_model.upsert_materialized_expense(payload, payload_hash=payload_hash)
    elif event_type == "expense_delete":
        expense_id = int(payload.get("expense_id") or 0)
        if expense_id:
            read_model.delete_materialized_expense(expense_id)
    elif event_type == "group_state":
        read_model.replace_group_state(dict(payload.get("group_state") or {}))
    elif event_type == "loadgen_probe":
        pass
    else:
        raise RuntimeError(f"Unsupported event_type: {event_type}")
    return event_type


def run_consumer_loop() -> None:
    _start_metrics_server()
    consumer = _consumer_instance()
    topic = kafka_topic()

    def on_assign(client: Any, partitions: List[Any]) -> None:
        CONSUMER_ASSIGNED_PARTITIONS.set(len(partitions))
        _set_consumer_state("assigned_partitions", len(partitions))
        client.assign(partitions)

    def on_revoke(client: Any, partitions: List[Any]) -> None:
        CONSUMER_ASSIGNED_PARTITIONS.set(0)
        _set_consumer_state("assigned_partitions", 0)
        client.unassign()

    consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)
    _set_consumer_state("group_id", kafka_group_id())
    _set_consumer_state("topic", topic)

    poll_timeout = _read_float_env("KAFKA_CONSUMER_POLL_TIMEOUT_SECONDS", default=1.0, minimum=0.1)
    try:
        while True:
            msg = consumer.poll(poll_timeout)
            if msg is None:
                continue
            if msg.error():
                if KafkaError is not None and msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(str(msg.error()))

            payload_bytes = bytes(msg.value() or b"")
            payload = json.loads(payload_bytes.decode("utf-8"))
            payload_hash = hashlib.sha256(payload_bytes).hexdigest()
            event_type = _handle_consumer_event(payload, payload_hash=payload_hash)
            consumer.commit(message=msg, asynchronous=False)

            CONSUMER_MESSAGES_TOTAL.labels(event_type=event_type).inc()
            CONSUMER_LAST_SUCCESS_UNIX.set(unix_now())
            CONSUMER_LAST_OFFSET.labels(topic=msg.topic(), partition=str(msg.partition())).set(msg.offset())
            _set_consumer_state("last_error", "")
            _set_consumer_state("last_event_type", event_type)
            _set_consumer_state("last_processed_at", utc_now_iso())
            _set_consumer_state("last_processed_topic", msg.topic())
            _set_consumer_state("last_processed_partition", msg.partition())
            _set_consumer_state("last_processed_offset", msg.offset())
    except Exception as exc:
        _set_consumer_state("last_error", f"{exc.__class__.__name__}: {exc}")
        raise
    except KeyboardInterrupt:
        return
    finally:
        CONSUMER_ASSIGNED_PARTITIONS.set(0)
        consumer.close()


def run_load_generator() -> None:
    _start_metrics_server()
    producer = _producer_instance()
    topic = kafka_topic()
    rate = _read_float_env("KAFKA_LOADGEN_MESSAGES_PER_SECOND", default=100.0, minimum=1.0)
    payload_bytes = _read_int_env("KAFKA_LOADGEN_PAYLOAD_BYTES", default=512, minimum=1)
    interval = 1.0 / rate
    sequence = 0
    last_flush = time.time()
    try:
        while True:
            payload = {
                "schema_version": 1,
                "event_type": "loadgen_probe",
                "emitted_at": utc_now_iso(),
                "sequence": sequence,
                "payload": "x" * payload_bytes,
            }
            producer.produce(
                topic=topic,
                key=f"loadgen:{sequence}".encode("utf-8"),
                value=_json_bytes(payload),
            )
            producer.poll(0)
            LOADGEN_MESSAGES_TOTAL.inc()
            sequence += 1
            if time.time() - last_flush >= 1.0:
                producer.flush(timeout=5.0)
                last_flush = time.time()
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(timeout=5.0)
