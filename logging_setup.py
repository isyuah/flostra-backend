"""结构化日志：单行 JSON + 运行上下文字段 + secret 脱敏。

字段名与 Go 控制面（zerolog）对齐，便于在同一条查询里串起两侧：
``timestamp`` / ``level`` / ``logger`` / ``message`` / ``execution_id`` /
``attempt_id`` / ``worker_id`` / ``node_id``。

- 上下文用 contextvars 承载：``asyncio`` 创建子任务时会复制当前 context，因此
  在 ``_handle_message`` 里登记的 executionId 会自动出现在 ``run_workflow`` 与
  各节点协程的日志里；节点级的 ``node_id`` 只影响该节点所在的子任务。
- 脱敏在序列化之后对整行做字面值替换：无论 secret 出现在消息、附加字段还是
  异常堆栈里都不会落到日志（与 ``security/event_safety.py`` 的事件脱敏同源同目标）。
"""

from __future__ import annotations

import json
import logging
import os
import sys
import traceback
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

MASK = "***"

_CONTEXT: ContextVar[dict[str, str] | None] = ContextVar("log_context", default=None)
_SECRETS: ContextVar[tuple[str, ...]] = ContextVar("log_secrets", default=())


def _context_fields() -> dict[str, str]:
    return dict(_CONTEXT.get() or {})


# LogRecord 自带属性，不进入 JSON 输出（会与固定字段重复或纯实现细节）。
_RESERVED = frozenset(
    {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "message",
        "module",
        "msecs",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "stacklevel",
        "taskName",
        "thread",
        "threadName",
    }
)


@contextmanager
def log_context(**fields: Any) -> Iterator[None]:
    """把运行上下文字段并入当前 context，退出时恢复（支持嵌套）。"""
    merged = _context_fields()
    for key, value in fields.items():
        if value is None:
            continue
        merged[key] = str(value)
    token = _CONTEXT.set(merged)
    try:
        yield
    finally:
        _CONTEXT.reset(token)


@contextmanager
def secret_scope(values: Iterable[Any] | None) -> Iterator[None]:
    """登记本次运行可见的 secret 字面值，供日志脱敏使用。"""
    secrets = tuple(str(v) for v in (values or ()) if v)
    token = _SECRETS.set(_SECRETS.get() + secrets)
    try:
        yield
    finally:
        _SECRETS.reset(token)


class JsonFormatter(logging.Formatter):
    """单行 JSON 输出；时间戳为 UTC ISO8601。"""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload.update(_context_fields())
        for key, value in record.__dict__.items():
            if key in _RESERVED or key.startswith("_"):
                continue
            payload.setdefault(key, value)
        if record.exc_info:
            payload["exception"] = "".join(traceback.format_exception(*record.exc_info))

        line = json.dumps(payload, ensure_ascii=False, default=str)
        for secret in _SECRETS.get():
            if secret in line:
                line = line.replace(secret, MASK)
        return line


def setup_logging() -> None:
    """装配唯一的 stdout JSON handler；重复调用不会叠加 handler。"""
    level_name = os.getenv("WORKER_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level)
