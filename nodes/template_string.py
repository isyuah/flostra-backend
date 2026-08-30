from __future__ import annotations

import json
import re
from typing import Any

from .base import JsonDict, WorkflowNode, register_node
from .utils import json_resolve_path


@register_node
class TemplateStringNode(WorkflowNode):
    """
    模板字符串节点：使用 Mustache 风格占位符 {{path}} 渲染文本
    """

    type = "string-template"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "模板字符串",
            "description": "使用 {{path}} 占位符从输入 JSON 中取值并渲染为字符串",
            "category": "逻辑",
            "icon": "json",
            "inputs": [
                {
                    "name": "context",
                    "label": "上下文",
                    "type": ["object", "array", "string"],
                    "required": False,
                    "desc": "模板变量来源：对象或 JSON 字符串；若为非对象，将作为变量 value 提供",
                }
            ],
            "outputs": [{"name": "text", "label": "结果字符串", "type": "string", "required": True}],
            "parameters": [
                {
                    "name": "template",
                    "label": "模板",
                    "type": "string",
                    "widget": "textarea",
                    "extra": {"placeholder": "例如：你好，{{user.name}}，今天是 {{date}}。"},
                    "required": True,
                    "desc": "使用 {{path}} 作为占位符，path 语法同 JSON 提取节点",
                }
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict, context: JsonDict = None) -> JsonDict:
        template = params.get("template") or ""
        if not isinstance(template, str):
            template = str(template)

        raw_context = inputs.get("context")

        # 1. 解析显式输入的 context
        if isinstance(raw_context, (dict, list)):
            data_ctx: Any = raw_context
        elif isinstance(raw_context, str):
            try:
                parsed = json.loads(raw_context)
            except json.JSONDecodeError:
                parsed = raw_context
            if isinstance(parsed, (dict, list)):
                data_ctx = parsed
            else:
                data_ctx = {"value": parsed}
        elif raw_context is not None:
             # 有输入但不是 dict/list/str，包装成 value
            data_ctx = {"value": raw_context}
        else:
            # 没有显式输入，初始化为空字典
            data_ctx = {}

        # 2. 构造混合上下文：优先使用数据上下文，合并全局上下文
        # 如果 data_ctx 是字典，则可以将 global context 作为备选或合并进来
        # 策略：如果 data_ctx 是 dict，则让它 copy 一份，并把 global context 的 key 注入进去（不覆盖 data_ctx 原有 key）
        # 这样 {{user.id}} 可以访问 global，而 {{local_var}} 访问 input
        render_scope = data_ctx
        if isinstance(data_ctx, dict) and isinstance(context, dict):
            render_scope = context.copy()
            render_scope.update(data_ctx)  # input 优先级更高
            # 特殊：为了方便访问 global context 本身，也可以注入一个 __global__ 或 context 变量
            render_scope["context"] = context

        text = _render_template_string(template, render_scope)
        return {"text": text}


def _render_template_string(template: str, context: Any) -> str:
    if not template:
        return ""

    pattern = re.compile(r"{{\s*([a-zA-Z_][\w\.\[\]0-9]*|)\s*}}")

    def repl(match: re.Match[str]) -> str:
        path = match.group(1).strip()
        if path == "":
            value = context
        else:
            value = json_resolve_path(context, path, default="")
        if value is None:
            return ""
        return str(value)

    return pattern.sub(repl, template)
