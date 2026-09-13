from __future__ import annotations

import json
from typing import Any

from .base import JsonDict, WorkflowNode, register_node


@register_node
class ManualStartNode(WorkflowNode):
    """手动触发器：通过端口常量或 overrides 注入 payload，向下游透传"""

    type = "trigger.manual"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "手动触发器",
            "description": "用于调试或手动启动工作流，payload 由端口常量或 overrides 提供并透传",
            "category": "触发器",
            "icon": "trigger",
            "inputs": [
                {
                    "name": "payload",
                    "label": "输入数据",
                    "type": "object",
                    "required": False,
                    "desc": "运行时通过端口常量或 overrides 注入的数据",
                }
            ],
            "outputs": [
                {
                    "name": "payload",
                    "label": "输出数据",
                    "type": "object",
                    "required": False,
                }
            ],
            "parameters": [],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict, context: JsonDict = None) -> JsonDict:
        # 透传 payload，供下游节点使用
        return {"payload": inputs.get("payload")}


@register_node
class HttpStartNode(WorkflowNode):
    """HTTP 触发器：用于发布成 HTTP 端点的入口"""

    type = "trigger.http"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "HTTP 触发器",
            "description": "从外部 HTTP 请求触发工作流，支持按 method+path 匹配入口",
            "category": "触发器",
            "icon": "trigger",
            "inputs": [],
            "outputs": [
                {
                    "name": "method",
                    "label": "HTTP 方法",
                    "type": "string",
                    "required": False,
                },
                {
                    "name": "data",
                    "label": "合并数据 (body 覆盖 query)",
                    "type": "object",
                    "required": False,
                },
                {
                    "name": "request",
                    "label": "原始请求对象",
                    "type": "object",
                    "required": False,
                },
            ],
            "parameters": [
                {
                    "name": "method",
                    "label": "HTTP 方法",
                    "type": "string",
                    "widget": "select",
                    "extra": {
                        "options": [
                            {"label": "GET", "value": "GET"},
                            {"label": "POST", "value": "POST"},
                            {"label": "PUT", "value": "PUT"},
                            {"label": "DELETE", "value": "DELETE"},
                        ],
                    },
                    "default": "POST",
                    "required": True,
                },
                {
                    "name": "path",
                    "label": "路径",
                    "type": "string",
                    "widget": "input",
                    "extra": {
                        "placeholder": "/chat",
                    },
                    "default": "/chat",
                    "required": True,
                    "desc": "用于发布为 HTTP Endpoint 的路径（同一工作流内应唯一）",
                },
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict, context: JsonDict = None) -> JsonDict:
        context = context or {}
        ctx_request = context.get("request") if isinstance(context, dict) else None

        def _to_dict(val: Any) -> dict[str, Any]:
            if val is None:
                return {}
            if isinstance(val, dict):
                return val
            if isinstance(val, str):
                try:
                    parsed = json.loads(val)
                    return parsed if isinstance(parsed, dict) else {}
                except Exception:
                    return {}
            return {}

        query = _to_dict(inputs.get("query")) or _to_dict(ctx_request.get("query") if ctx_request else None)
        body_input = inputs.get("body")
        payload = inputs.get("payload")
        body_source = body_input if body_input is not None else payload
        body = _to_dict(body_source) or _to_dict(ctx_request.get("body") if ctx_request else None)

        # 合并数据：body 优先覆盖 query
        data: dict[str, Any] = {}
        data.update(query)
        data.update(body)

        method = inputs.get("method") or (ctx_request.get("method") if ctx_request else params.get("method")) or ""
        path = inputs.get("path") or (ctx_request.get("path") if ctx_request else params.get("path")) or ""

        # 构造返回的 request 对象，降级为最小字段
        request_obj = ctx_request if isinstance(ctx_request, dict) else {}
        if not request_obj:
            request_obj = {
                "method": method,
                "path": path,
                "headers": _to_dict(inputs.get("headers")),
                "query": query,
                "body": body,
                "rawBody": inputs.get("rawBody"),
            }

        return {
            "method": method,
            "data": data,
            "request": request_obj,
        }


@register_node
class CronStartNode(WorkflowNode):
    """Cron 触发器：消费控制面 cron 调度写入的运行上下文

    控制面在 schedule 触发时把 ``context.schedule = {name, cronExpression}``
    随任务一起持久化（见 gback internal/schedule/service.go）；手动或 webhook
    触发没有该键，引擎的入口筛选也会因此跳过本节点。
    """

    type = "trigger.cron"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "Cron 触发器",
            "description": "由控制面 cron 调度触发的入口，透传本次触发的 schedule 元数据",
            "category": "触发器",
            "icon": "trigger",
            "inputs": [],
            "outputs": [
                {
                    "name": "schedule",
                    "label": "Schedule 元数据",
                    "type": "object",
                    "required": False,
                    "desc": "运行上下文 context.schedule（name / cronExpression）",
                },
                {
                    "name": "scheduleName",
                    "label": "Schedule 名称",
                    "type": "string",
                    "required": False,
                },
                {
                    "name": "cronExpression",
                    "label": "Cron 表达式",
                    "type": "string",
                    "required": False,
                },
            ],
            "parameters": [],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict, context: JsonDict = None) -> JsonDict:
        ctx_schedule = context.get("schedule") if isinstance(context, dict) else None
        schedule = ctx_schedule if isinstance(ctx_schedule, dict) else {}
        return {
            "schedule": schedule,
            "scheduleName": str(schedule.get("name") or ""),
            "cronExpression": str(schedule.get("cronExpression") or ""),
        }
