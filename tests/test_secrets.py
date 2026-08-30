"""Secret 解析 fail-closed 测试。"""

import pytest

from secrets_store import SecretResolutionError, resolve_secrets_in_params


def test_resolves_exact_and_nested():
    params = {
        "api_key": "[[OPENAI_API_KEY]]",
        "nested": {"dsn": "postgres://u:[[DB_PASS]]@host/db"},
        "list": ["[[TOKEN]]", "plain"],
    }
    secrets = {"OPENAI_API_KEY": "sk-123", "DB_PASS": "pw", "TOKEN": "tok"}
    resolved = resolve_secrets_in_params(params, secrets)
    assert resolved["api_key"] == "sk-123"
    assert resolved["nested"]["dsn"] == "postgres://u:pw@host/db"
    assert resolved["list"] == ["tok", "plain"]


def test_missing_secret_fails_closed():
    with pytest.raises(SecretResolutionError):
        resolve_secrets_in_params({"key": "[[MISSING]]"}, {"OTHER": "x"})


def test_no_secret_map_with_reference_fails_closed():
    with pytest.raises(SecretResolutionError):
        resolve_secrets_in_params({"key": "[[ANY]]"}, {})


def test_no_reference_without_secret_map_ok():
    assert resolve_secrets_in_params({"key": "plain"}, {}) == {"key": "plain"}
