from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any

from llm_plugins import list_plugins_schema
from nodes import get_all_node_schemas


def _truthy_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"", "0", "false", "no", "off"}


def _atomic_write_json(path: Path, payload: Any) -> None:
    """
    以“写临时文件 + os.replace”的方式原子写入，避免并发/中途退出导致的半文件。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    data = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    tmp.write_text(data + "\n", encoding="utf-8")
    os.replace(tmp, path)


def export_definitions(out_dir: Path) -> dict[str, Path]:
    """
    导出前端所需的静态 schema：
    - nodes: get_all_node_schemas()
    - plugins: list_plugins_schema()
    """
    out_dir = out_dir.resolve()
    node_path = out_dir / "node_definitions.json"
    plugin_path = out_dir / "plugin_definitions.json"

    _atomic_write_json(node_path, get_all_node_schemas())
    _atomic_write_json(plugin_path, list_plugins_schema())

    return {"nodes": node_path, "plugins": plugin_path}


def export_definitions_if_enabled(*, base_dir: Path | None = None) -> dict[str, Path]:
    """
    供启动流程调用：由环境变量控制是否导出与导出目录。

    - EXPORT_DEFINITIONS_ON_STARTUP: 默认 true；设为 0/false/off 可禁用
    - EXPORT_DEFINITIONS_DIR: 默认 <base_dir>/exported（base_dir 默认当前文件所在目录）
    """
    if not _truthy_env("EXPORT_DEFINITIONS_ON_STARTUP", True):
        return {}

    if base_dir is None:
        base_dir = Path(__file__).resolve().parent

    out_dir = Path(os.getenv("EXPORT_DEFINITIONS_DIR") or (base_dir / "exported"))
    return export_definitions(out_dir)

