"""Worker 事件安全：大小限制与敏感值脱敏。

- ``EVENT_PAYLOAD_MAX_BYTES``: 单个 worker 事件的序列化大小上限（默认 512 KiB）。
  超过上限的事件在发布前被拒绝，防止把超大节点输出推回控制面。
- 脱敏规则：节点输出（``node_completed`` 的 ``outputValues``、``inputValues``）
  中出现的 secret 字面值会被替换为 ``***``，避免密钥通过事件回传泄漏到
  Redis / SSE / 浏览器。
"""

from __future__ import annotations

import json
import os
from typing import Any

_EVENT_MAX_BYTES_DEFAULT = 512 * 1024

# 出现在输出中的 secret 值会整体替换为这个掩码。
_MASK = "***"

# 需要递归脱敏的字段路径（node_completed 中的 inputValues / outputValues）。
_SENSITIVE_KEYS = ("inputValues", "outputValues")


def event_max_bytes() -> int:
    try:
        value = int(os.getenv("EVENT_PAYLOAD_MAX_BYTES", str(_EVENT_MAX_BYTES_DEFAULT)))
        return value if value > 0 else _EVENT_MAX_BYTES_DEFAULT
    except (TypeError, ValueError):
        return _EVENT_MAX_BYTES_DEFAULT


def redact_secrets(value: Any, secret_values: dict[str, str] | None) -> Any:
    """递归脱敏：把 value 中出现的任意 secret 字面值替换为掩码。

    仅处理字符串。嵌套 dict/list 递归。secret_values 为空时原样返回。
    """
    if not secret_values:
        return value

    secrets = [str(v) for v in secret_values.values() if v]

    def _redact_string(text: str) -> str:
        for secret in secrets:
            if secret and secret in text:
                text = text.replace(secret, _MASK)
        return text

    if isinstance(value, str):
        return _redact_string(value)
    if isinstance(value, dict):
        return {k: redact_secrets(v, secret_values) for k, v in value.items()}
    if isinstance(value, list):
        return [redact_secrets(item, secret_values) for item in value]
    return value


def sanitize_event_data(data: dict[str, Any], secret_values: dict[str, str] | None) -> dict[str, Any]:
    """对事件 data 中标记为敏感的字段做递归脱敏，并返回新 dict。

    目前对 ``node_completed`` 的 ``inputValues`` / ``outputValues`` 脱敏；
    其余字段保持原样（事件元数据本身不含 secret 字面量）。
    """
    if not secret_values:
        return data

    result = dict(data)
    for key in _SENSITIVE_KEYS:
        if key in result:
            result[key] = redact_secrets(result[key], secret_values)
    return result


def serialize_event_payload(payload: dict[str, Any]) -> bytes:
    """序列化事件 payload，超过上限时抛 ValueError（由调用方决定失败策略）。"""
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    if len(body) > event_max_bytes():
        raise ValueError(
            f"worker event payload exceeds limit of {event_max_bytes()} bytes "
            f"(actual {len(body)} bytes)"
        )
    return body
