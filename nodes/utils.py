from __future__ import annotations

from typing import Any


def json_resolve_path(data: Any, path: str, default: Any = None) -> Any:
    """
    简单的 JSON 路径解析器，支持：
    - 点号访问：a.b.c -> data['a']['b']['c']
    - 中括号索引：items[0].price -> data['items'][0]['price']

    不支持复杂的 JSONPath 语法，仅用于本项目的轻量级提取需求。
    """
    if path is None or path == "":
        return data

    current = data
    segments = path.split(".")

    for seg in segments:
        if current is None:
            return default

        key = ""
        indexes: list[int] = []
        buf = ""
        in_bracket = False
        for ch in seg:
            if ch == "[":
                if not in_bracket:
                    in_bracket = True
                    key = buf
                    buf = ""
                else:
                    return default
            elif ch == "]":
                if not in_bracket:
                    return default
                in_bracket = False
                idx_str = buf.strip()
                buf = ""
                if not idx_str.isdigit():
                    return default
                indexes.append(int(idx_str))
            else:
                buf += ch

        if in_bracket:
            return default

        if key == "":
            key = buf

        if key:
            if isinstance(current, dict):
                if key not in current:
                    return default
                current = current.get(key)
            else:
                return default

        for idx in indexes:
            if isinstance(current, list):
                if idx < 0 or idx >= len(current):
                    return default
                current = current[idx]
            else:
                return default

    return current
