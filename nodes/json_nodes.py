from __future__ import annotations

import json
from typing import Any

from .base import JsonDict, WorkflowNode, register_node
from .utils import json_resolve_path


@register_node
class JsonExtractNode(WorkflowNode):
    """
    JSON 提取节点：
    - 固定一个输入端口 input（object/any）
    - 输出端口通过参数 ports（ports_list）配置
    """

    type = "json-extract"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "JSON 提取",
            "description": "根据配置的路径从输入 JSON 中提取多个字段作为输出端口",
            "category": "逻辑",
            "icon": "json",
            "inputs": [
                {
                    "name": "input",
                    "label": "JSON 输入",
                    "type": ["object", "string"],
                    "required": True,
                    "desc": "上游节点输出的 JSON 对象，或 JSON 字符串",
                }
            ],
            "outputs": [],
            "parameters": [
                {
                    "name": "ports",
                    "label": "输出端口配置",
                    "type": "array",
                    "widget": "ports-list",
                    "extra": {
                        "role": "json-extract-ports",
                        "template": {
                            "name": "",
                            "label": "",
                            "path": "",
                        },
                        "effects": ["cal_output_ports"],
                    },
                    "required": False,
                    "desc": "id = 输出端口 ID；label = 显示名称；path = JSON 路径，如 user.name 或 items[0].price",
                }
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict, context: JsonDict = None) -> JsonDict:
        raw = inputs.get("input")
        if raw is None:
            raise ValueError("JSON 提取节点缺少输入数据 (input 端口)")
            
        if isinstance(raw, str):
            try:
                data: Any = json.loads(raw)
            except json.JSONDecodeError:
                data = raw
        else:
            data = raw

        ports = params.get("ports") or []
        outputs: dict[str, Any] = {}

        for port_cfg in ports:
            if not isinstance(port_cfg, dict):
                continue
            out_id = port_cfg.get("name") or port_cfg.get("id")
            path = port_cfg.get("path")
            default = port_cfg.get("default")
            required = bool(port_cfg.get("required", False))

            if not out_id or not path:
                continue

            value = json_resolve_path(data, path, default=default)
            if value is None and required:
                raise ValueError(f"JSON 提取节点端口 {out_id!r} 在路径 {path!r} 上未找到值")

            outputs[out_id] = value

        return outputs


@register_node
class JsonBuildNode(WorkflowNode):
    """
    JSON 构造节点：
    - 所有输入端口通过参数 fields 配置（dynamicInputs）
    - 运行时将各输入端口的值聚合为一个 JSON 对象输出
    """

    type = "json-build"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "JSON 构造",
            "description": "将多个输入端口的值聚合为一个 JSON 对象输出",
            "category": "逻辑",
            "icon": "json",
            "inputs": [],
            "outputs": [
                {
                    "name": "output",
                    "label": "JSON 输出",
                    "type": "object",
                    "required": False,
                }
            ],
            "parameters": [
                {
                    "name": "fields",
                    "label": "字段配置",
                    "type": "array",
                    "widget": "ports-list",
                    "extra": {
                        "role": "json-build-inputs",
                        "template": {
                            "name": "",
                            "label": "",
                        },
                        "effects": ["cal_input_ports"],
                    },
                    "required": False,
                    "desc": "配置要聚合的输入端口，id 作为端口 ID，也是 JSON 对象中的 key。",
                }
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict, context: JsonDict = None) -> JsonDict:
        fields = params.get("fields") or []
        obj: dict[str, Any] = {}

        if not fields:
            for key, value in inputs.items():
                obj[key] = value
            return {"output": obj}

        for field_cfg in fields:
            if not isinstance(field_cfg, dict):
                continue
            fid = field_cfg.get("name") or field_cfg.get("id")
            required = bool(field_cfg.get("required", False))
            if not fid:
                continue

            if fid in inputs:
                obj[fid] = inputs.get(fid)
            elif required:
                raise ValueError(f"JSON 构造节点字段 {fid!r} 为必需，但未提供对应输入")

        return {"output": obj}
