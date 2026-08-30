from __future__ import annotations

import asyncio
from typing import Any

from .base import JsonDict, WorkflowNode, register_node


@register_node
class CodeNode(WorkflowNode):
    """
    代码节点：
    - 通过 inputPorts / outputPorts 配置动态输入/输出端口
    - 用户在参数 code 中编写 Python 代码，并实现 run(inputs, params) 函数
    """

    type = "code"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "代码",
            "description": "执行自定义 Python 代码，对输入做任意处理并输出结果",
            "category": "逻辑",
            "icon": "code",
            "inputs": [],
            "outputs": [],
            "parameters": [
                {
                    "name": "inputPorts",
                    "label": "输入端口",
                    "type": "array",
                    "widget": "ports-list",
                    "extra": {
                        "role": "code-input-ports",
                        "template": {
                            "name": "",
                            "label": "",
                        },
                        "effects": ["cal_input_ports"],
                    },
                    "required": False,
                    "default": [
                        {
                            "name": "input",
                            "label": "输入",
                            "required": False,
                        }
                    ],
                },
                {
                    "name": "outputPorts",
                    "label": "输出端口",
                    "type": "array",
                    "widget": "ports-list",
                    "extra": {
                        "role": "code-output-ports",
                        "template": {
                            "name": "",
                            "label": "",
                        },
                        "effects": ["cal_output_ports"],
                    },
                    "required": False,
                    "default": [
                        {
                            "name": "output",
                            "label": "输出",
                            "required": False,
                        }
                    ],
                },
                {
                    "name": "lang",
                    "label": "语言",
                    "type": "string",
                    "widget": "select",
                    "extra": {"options": [{"label": "Python", "value": "python"}]},
                    "default": "python",
                    "required": True,
                },
                {
                    "name": "code",
                    "label": "代码",
                    "type": "string",
                    "widget": "code-editor",
                    "extra": {"language": "python"},
                    "required": True,
                    "desc": (
                        "实现 run(inputs: dict, params: dict) -> Any；"
                        "单输出端口可返回任意类型，多输出端口必须返回 dict 以端口 id 为 key。"
                    ),
                },
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict, context: JsonDict = None) -> JsonDict:
        lang = (params.get("lang") or "python").lower()
        if lang != "python":
            raise ValueError(f"暂不支持语言 {lang!r}，当前仅支持 'python'")

        code_str = params.get("code")
        if not isinstance(code_str, str) or not code_str.strip():
            raise ValueError("代码节点缺少必需的参数 code")

        output_ports = params.get("outputPorts") or []

        global_env: dict[str, Any] = {"__name__": "__code_node__"}
        local_env: dict[str, Any] = {}

        try:
            exec(code_str, global_env, local_env)
        except Exception as exc:
            raise ValueError(f"代码节点执行代码时出错：{exc}") from exc

        func = local_env.get("run") or global_env.get("run")
        if not callable(func):
            raise ValueError("代码节点必须在代码中定义函数 run(inputs, params)")

        import inspect

        sig = inspect.signature(func)
        param_count = len(sig.parameters)

        loop = asyncio.get_event_loop()

        def _call_user() -> Any:
            # 支持 run(inputs, params, context) 或 run(inputs, params)
            if param_count >= 3:
                return func(inputs, params, context)
            return func(inputs, params)

        try:
            result = await loop.run_in_executor(None, _call_user)
        except Exception as exc:
            raise ValueError(f"代码节点运行 run(inputs, params) 时出错：{exc}") from exc

        outputs: dict[str, Any] = {}
        num_ports = len(output_ports)

        if num_ports == 0:
            return outputs

        if num_ports == 1:
            port_id = output_ports[0].get("name") or output_ports[0].get("id") or "output"
            outputs[port_id] = result
            return outputs

        if not isinstance(result, dict):
            raise ValueError("代码节点配置了多个输出端口，但 run() 返回的结果不是 dict")

        for port_cfg in output_ports:
            if not isinstance(port_cfg, dict):
                continue
            pid = port_cfg.get("name") or port_cfg.get("id")
            if not pid:
                continue
            outputs[pid] = result.get(pid)

        return outputs
