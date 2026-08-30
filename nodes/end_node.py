from __future__ import annotations

from typing import Any

from .base import JsonDict, WorkflowNode, register_node


@register_node
class EndNode(WorkflowNode):
    """End 节点：标记工作流的结果输出位置（支持动态端口）"""

    type = "end"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "结束",
            "description": "工作流结束节点，作为结果输出节点；支持配置多个结果端口",
            "category": "控制",
            "icon": "end",
            "inputs": [],
            "outputs": [],
            "parameters": [
                {
                    "name": "ports",
                    "label": "结果端口配置",
                    "type": "array",
                    "widget": "ports-list",
                    "extra": {
                        "role": "end-ports",
                        "template": {
                            "name": "",
                            "label": "",
                        },
                        "effects": ["cal_input_ports"],
                    },
                    "required": False,
                    "default": [
                        {
                            "name": "value",
                            "label": "结果值",
                            "required": False,
                        }
                    ],
                    "desc": (
                        "配置 End 节点的结果端口；id 作为端口 ID，label 为展示名称。"
                        "所有端口类型均为 any，执行时会将对应输入端口的值原样透传到结果中。"
                    ),
                }
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict, context: JsonDict = None) -> JsonDict:
        ports = params.get("ports")
        outputs: dict[str, Any] = {}

        if not ports:
            if "value" in inputs:
                outputs["value"] = inputs.get("value")
            return outputs

        for port_cfg in ports:
            if not isinstance(port_cfg, dict):
                continue
            pid = port_cfg.get("name") or port_cfg.get("id")
            if not pid:
                continue
            if pid in inputs:
                outputs[pid] = inputs.get(pid)

        return outputs
