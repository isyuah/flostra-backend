from __future__ import annotations

import json

from .base import JsonDict, WorkflowNode, register_node
from .utils import json_resolve_path


@register_node
class TextToJsonNode(WorkflowNode):
    """文本转 JSON：将字符串解析为 JSON，可选再做路径提取"""

    type = "text-to-json"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "文本转JSON",
            "description": "解析字符串为 JSON，对无效 JSON 可选择容错",
            "category": "逻辑",
            "icon": "text-to-json",
            "inputs": [{"name": "text", "label": "文本", "type": "string", "required": True}],
            "outputs": [
                {"name": "json", "label": "JSON", "type": "any", "required": False},
                {"name": "ok", "label": "是否成功", "type": "boolean", "required": False},
                {"name": "error", "label": "错误信息", "type": "string", "required": False},
            ],
            "parameters": [
                {"name": "strict", "label": "严格模式", "type": "boolean", "widget": "switch", "default": True},
                {
                    "name": "json_path",
                    "label": "JSON 路径",
                    "type": "string",
                    "widget": "input",
                    "required": False,
                    "desc": "可选，从解析结果中按路径提取子字段，如 data.items[0]",
                },
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict, context: JsonDict = None) -> JsonDict:
        raw = inputs.get("text", "")
        text = str(raw)
        strict = bool(params.get("strict", True))
        path = params.get("json_path") or ""

        try:
            data = json.loads(text)
            if path:
                data = json_resolve_path(data, path, default=None)
            return {"json": data, "ok": True, "error": None}
        except Exception as exc:
            if strict:
                raise ValueError(f"JSON 解析失败: {exc}") from exc
            return {"json": None, "ok": False, "error": str(exc)}
