#!/usr/bin/env python3
from __future__ import annotations

import os
import sys

import kafka_runtime


def _int_env(name: str, default: int) -> int:
    raw = (os.getenv(name) or str(default)).strip()
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid integer value for {name}: {raw}") from exc


def _exec_gunicorn() -> None:
    host = (os.getenv("SPLITWISE_APP_HOST") or "0.0.0.0").strip() or "0.0.0.0"
    port = _int_env("PORT", 8080)
    workers = _int_env("GUNICORN_WORKERS", 1)
    threads = _int_env("GUNICORN_THREADS", 4)
    timeout = _int_env("GUNICORN_TIMEOUT", 120)
    args = [
        "gunicorn",
        "--bind",
        f"{host}:{port}",
        "--workers",
        str(workers),
        "--threads",
        str(threads),
        "--timeout",
        str(timeout),
        "web_app:app",
    ]
    os.execvp(args[0], args)


def main() -> None:
    mode = (os.getenv("APP_MODE") or "web").strip().lower()
    if mode == "web":
        _exec_gunicorn()
    if mode == "producer":
        kafka_runtime.run_producer_loop()
        return
    if mode == "consumer":
        kafka_runtime.run_consumer_loop()
        return
    if mode == "loadgen":
        kafka_runtime.run_load_generator()
        return
    raise SystemExit(f"Unsupported APP_MODE: {mode}")


if __name__ == "__main__":
    main()
