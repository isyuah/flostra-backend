"""结构化日志测试：JSON 字段、运行上下文、secret 脱敏。"""

from __future__ import annotations

import json
import logging
import sys

import pytest

from logging_setup import JsonFormatter, log_context, secret_scope


def _format(record: logging.LogRecord) -> dict:
    return json.loads(JsonFormatter().format(record))


def _record(message: str, args: tuple = (), **extra) -> logging.LogRecord:
    record = logging.LogRecord(
        name="mq_worker",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=message,
        args=args,
        exc_info=None,
    )
    for key, value in extra.items():
        setattr(record, key, value)
    return record


def test_fields_and_extras_are_structured():
    payload = _format(_record("Run begin", nodes=3, targets=["n2"]))

    assert payload["level"] == "INFO"
    assert payload["logger"] == "mq_worker"
    assert payload["message"] == "Run begin"
    assert payload["nodes"] == 3
    assert payload["targets"] == ["n2"]
    assert payload["timestamp"].endswith("+00:00")
    # LogRecord 内部属性不应泄漏到输出里。
    assert "lineno" not in payload
    assert "levelno" not in payload


def test_run_context_is_injected_and_restored():
    with log_context(execution_id="exec-1", attempt_id="attempt-1", worker_id="worker-1"):
        inside = _format(_record("node start"))
    outside = _format(_record("worker start"))

    assert inside["execution_id"] == "exec-1"
    assert inside["attempt_id"] == "attempt-1"
    assert inside["worker_id"] == "worker-1"
    assert "execution_id" not in outside


def test_nested_context_merges_and_restores():
    with log_context(execution_id="exec-1"):
        with log_context(node_id="n1"):
            inner = _format(_record("node failed"))
        after = _format(_record("run end"))

    assert inner["execution_id"] == "exec-1"
    assert inner["node_id"] == "n1"
    assert after["execution_id"] == "exec-1"
    assert "node_id" not in after


def test_exception_text_is_redacted():
    record = _record("Workflow execution failed")
    try:
        raise ValueError("dsn=postgresql://user:s3cr3t@db/app")
    except ValueError:
        record.exc_info = sys.exc_info()

    with secret_scope(["s3cr3t"]):
        redacted = _format(record)["exception"]

    assert "s3cr3t" not in redacted
    assert "***" in redacted
    # 没有登记 secret 时保持原样，确认脱敏来自 secret_scope 而不是无条件替换。
    assert "s3cr3t" in _format(record)["exception"]


def test_secrets_are_redacted_from_message_and_extras():
    with secret_scope(["sk-live-key"]):
        payload = _format(
            _record("Calling upstream with %s", ("sk-live-key",), authorization="Bearer sk-live-key")
        )

    assert payload["message"] == "Calling upstream with ***"
    assert payload["authorization"] == "Bearer ***"


def test_secret_scope_is_restored_after_run():
    with secret_scope(["sk-live-key"]):
        pass
    payload = _format(_record("plain message"))

    assert payload["message"] == "plain message"


@pytest.mark.parametrize("level_name", ["INFO", "DEBUG", "NOT_A_LEVEL"])
def test_setup_logging_installs_single_json_handler(level_name, monkeypatch):
    import logging_setup

    monkeypatch.setenv("WORKER_LOG_LEVEL", level_name)
    root = logging.getLogger()
    original_handlers = root.handlers[:]
    original_level = root.level
    try:
        logging_setup.setup_logging()
        logging_setup.setup_logging()

        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0].formatter, JsonFormatter)
        assert root.level == getattr(logging, level_name, logging.INFO)
    finally:
        root.handlers[:] = original_handlers
        root.setLevel(original_level)
