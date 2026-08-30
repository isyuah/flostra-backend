from __future__ import annotations

import re
from typing import Any

# 匹配 [[ KEY ]] 或 [[KEY]]，忽略前后空格
# Group 1 是 KEY
SECRET_PATTERN = re.compile(r"\[\[\s*(\S+)\s*\]\]")


class SecretResolutionError(ValueError):
    """引用了不存在的 secret，且该引用无法解析。"""


def resolve_secrets_in_params(params: dict[str, Any], secret_map: dict[str, str]) -> dict[str, Any]:
    """
    递归遍历 params，将字符串值中的 [[ KEY ]] 替换为 secret_map[KEY]。

    fail-closed：发现 [[ KEY ]] 但 secret_map 中不存在对应 KEY 时抛出
    SecretResolutionError，而不是保留占位符继续执行。避免把字面量
    "[[KEY]]" 当成真实凭据发送到外部系统。
    """
    if not params:
        return {}
    if not secret_map:
        # 没有任何 secret 可用：只要存在占位符就应失败。
        _assert_no_references(params)
        return params

    resolved: dict[str, Any] = {}
    for k, v in params.items():
        resolved[k] = _resolve_value(v, secret_map)
    return resolved


def _assert_no_references(value: Any) -> None:
    """在没有 secret map 的情况下，检查是否存在任何占位符引用。"""
    if isinstance(value, str):
        if "[[" in value and SECRET_PATTERN.search(value):
            raise SecretResolutionError(
                f"secret reference found in workflow params but no secrets were provided: {value!r}"
            )
    elif isinstance(value, dict):
        for v in value.values():
            _assert_no_references(v)
    elif isinstance(value, list):
        for item in value:
            _assert_no_references(item)


def _resolve_value(value: Any, secret_map: dict[str, str]) -> Any:
    """内部递归辅助函数"""
    if isinstance(value, str):
        if "[[" not in value:
            return value

        full_match = SECRET_PATTERN.fullmatch(value.strip())
        if full_match:
            key = full_match.group(1)
            if key in secret_map:
                return secret_map[key]
            raise SecretResolutionError(
                f"secret {key!r} is referenced but not defined for this run"
            )

        def _replacer(match: re.Match) -> str:
            key = match.group(1)
            if key in secret_map:
                return secret_map[key]
            # fail-closed：无法解析的引用直接抛错，而不是保留占位符。
            raise SecretResolutionError(
                f"secret {key!r} is referenced but not defined for this run"
            )

        return SECRET_PATTERN.sub(_replacer, value)

    elif isinstance(value, dict):
        return {k: _resolve_value(v, secret_map) for k, v in value.items()}

    elif isinstance(value, list):
        return [_resolve_value(item, secret_map) for item in value]

    else:
        return value


# 废弃接口保留，防止硬编码导入报错
def resolve_secret_value(secret_id: str) -> str:
    raise NotImplementedError("Deprecated: Secrets are now resolved via context injection from MQ.")
