from __future__ import annotations

import json
from typing import Any, Dict, Union
from urllib.parse import urlparse

import redis.asyncio as redis

from .base import JsonDict, WorkflowNode, register_node
from security.egress import check_outbound_host


@register_node
class RedisNode(WorkflowNode):
    """
    Redis 节点：
    支持常见的 GET/SET/DEL 等操作。
    """
    type = "redis"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "Redis",
            "description": "执行 Redis 命令 (GET, SET, DEL 等)",
            "category": "Database",
            "icon": "database",
            "inputs": [
                {
                    "name": "action",
                    "label": "操作",
                    "type": "string",
                    "required": True,
                    "widget": "select",
                    "options": [
                        {"label": "GET", "value": "GET"},
                        {"label": "SET", "value": "SET"},
                        {"label": "DEL", "value": "DEL"},
                        {"label": "EXISTS", "value": "EXISTS"},
                        {"label": "INCR", "value": "INCR"},
                        {"label": "EXPIRE", "value": "EXPIRE"},
                    ],
                    "default": "GET",
                },
                {"name": "key", "label": "Key", "type": "string", "required": True},
                {"name": "value", "label": "Value (for SET/EXPIRE)", "type": "any", "required": False},
                {
                    "name": "connection",
                    "label": "连接信息",
                    "type": "object",
                    "required": True,
                    "description": "JSON对象: {host, port, password, db} 或 redis:// URL 字符串",
                },
            ],
            "outputs": [
                {"name": "result", "label": "执行结果", "type": "any", "required": True},
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict, context: JsonDict = None) -> JsonDict:
        action = str(inputs.get("action") or "GET").upper()
        key = str(inputs.get("key") or "")

        # 处理连接信息
        conn_input = inputs.get("connection")
        client_kwargs: Dict[str, Any] = {}

        url = None

        if isinstance(conn_input, str) and conn_input.startswith("redis"):
            url = conn_input
        elif isinstance(conn_input, dict):
            client_kwargs["host"] = conn_input.get("host") or "localhost"
            client_kwargs["port"] = int(conn_input.get("port") or 6379)
            if conn_input.get("password"):
                client_kwargs["password"] = conn_input.get("password")
            if conn_input.get("db") is not None:
                client_kwargs["db"] = int(conn_input.get("db"))
        else:
            client_kwargs["host"] = "localhost"
            client_kwargs["port"] = 6379

        # 出站网络策略：Redis 目标必须通过校验。
        if url:
            _parsed = urlparse(url)
            check_outbound_host(_parsed.hostname or "", _parsed.port or 6379)
        else:
            check_outbound_host(str(client_kwargs.get("host") or "localhost"), int(client_kwargs.get("port") or 6379))

        # 建立连接
        if url:
            client = redis.from_url(url, decode_responses=True, socket_connect_timeout=5.0)
        else:
            client = redis.Redis(
                decode_responses=True,
                socket_connect_timeout=5.0,
                **client_kwargs
            )

        try:
            result: Any = None
            if action == "GET":
                result = await client.get(key)
            elif action == "SET":
                val = inputs.get("value")
                if isinstance(val, (dict, list)):
                    val = json.dumps(val, ensure_ascii=False)
                elif val is None:
                    val = ""
                result = await client.set(key, val)
            elif action == "DEL":
                result = await client.delete(key)
            elif action == "EXISTS":
                result = await client.exists(key)
            elif action == "INCR":
                result = await client.incr(key)
            elif action == "EXPIRE":
                seconds = inputs.get("value")
                try:
                    sec_int = int(seconds)  # type: ignore
                    result = await client.expire(key, sec_int)
                except (ValueError, TypeError):
                    result = False
            else:
                raise ValueError(f"不支持的 Redis 操作: {action}")

            return {"result": result}
        except Exception as e:
            raise ValueError(f"Redis 操作失败: {e}") from e
        finally:
            await client.aclose()
