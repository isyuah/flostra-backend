"""Worker 事件安全测试：脱敏与大小限制。"""

import pytest

from security.event_safety import sanitize_event_data, serialize_event_payload


def test_redact_secret_values():
    secrets = {"k": "super-secret-value"}
    data = {
        "outputValues": {
            "text": "the super-secret-value is here",
            "nested": {"a": ["super-secret-value"]},
        },
    }
    sanitized = sanitize_event_data(data, secrets)
    assert "super-secret-value" not in sanitized["outputValues"]["text"]
    assert "***" in sanitized["outputValues"]["text"]
    assert sanitized["outputValues"]["nested"]["a"][0] == "***"


def test_no_secret_values_keeps_data():
    data = {"outputValues": {"text": "plain"}}
    assert sanitize_event_data(data, None) == data


def test_payload_size_limit(monkeypatch):
    monkeypatch.setenv("EVENT_PAYLOAD_MAX_BYTES", "64")
    payload = {"event": "node_completed", "data": {"outputValues": {"big": "x" * 10_000}}}
    with pytest.raises(ValueError):
        serialize_event_payload(payload)
