from __future__ import annotations

import json
from urllib.parse import urlparse

import aio_pika

from security.egress import check_outbound_host

from .base import JsonDict, WorkflowNode, register_node


@register_node
class RabbitMQNode(WorkflowNode):
    """
    RabbitMQ 节点：
    支持发送消息到 Exchange 或 Queue。
    """
    type = "rabbitmq"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "RabbitMQ Publish",
            "description": "发送消息到 RabbitMQ",
            "category": "Messaging",
            "icon": "message-square",
            "inputs": [
                {
                    "name": "connection",
                    "label": "连接信息",
                    "type": "string",
                    "required": True,
                    "description": "amqp://user:pass@host:port/vhost",
                },
                {"name": "exchange", "label": "Exchange", "type": "string", "required": False},
                {"name": "routing_key", "label": "Routing Key", "type": "string", "required": False},
                {"name": "queue", "label": "Queue Name (for default exchange)", "type": "string", "required": False},
                {"name": "message", "label": "Message Body", "type": "any", "required": True},
                {
                    "name": "options",
                    "label": "选项",
                    "type": "object",
                    "required": False,
                    "description": "delivery_mode, expiration, headers 等",
                },
            ],
            "outputs": [
                {"name": "success", "label": "是否成功", "type": "boolean", "required": True},
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict, context: JsonDict = None) -> JsonDict:
        conn_str = inputs.get("connection")
        if not conn_str:
            raise ValueError("RabbitMQ 连接字符串不能为空")

        # 出站网络策略：RabbitMQ broker 目标必须通过校验。
        _parsed = urlparse(conn_str)
        check_outbound_host(_parsed.hostname or "", _parsed.port or 5672)

        # 优先使用 exchange + routing_key
        # 如果没有 exchange，尝试使用 queue 作为 routing_key (default exchange)
        exchange_name = inputs.get("exchange") or ""
        routing_key = inputs.get("routing_key") or ""
        queue_name = inputs.get("queue")

        if not exchange_name and not routing_key and queue_name:
            routing_key = queue_name

        message_body = inputs.get("message")
        if isinstance(message_body, (dict, list)):
            body_bytes = json.dumps(message_body, ensure_ascii=False).encode("utf-8")
            content_type = "application/json"
        elif isinstance(message_body, str):
            body_bytes = message_body.encode("utf-8")
            content_type = "text/plain"
        elif isinstance(message_body, bytes):
            body_bytes = message_body
            content_type = "application/octet-stream"
        else:
            body_bytes = str(message_body).encode("utf-8")
            content_type = "text/plain"

        # 处理额外选项
        opts = inputs.get("options") or {}
        headers = opts.get("headers")
        delivery_mode = opts.get("delivery_mode")  # 1: Transient, 2: Persistent

        try:
            connection = await aio_pika.connect_robust(conn_str)
            async with connection:
                channel = await connection.channel()

                if exchange_name:
                    exchange = await channel.get_exchange(exchange_name, ensure=False)
                else:
                    exchange = channel.default_exchange

                msg = aio_pika.Message(
                    body=body_bytes,
                    content_type=content_type,
                    headers=headers,
                    delivery_mode=delivery_mode,
                )

                await exchange.publish(msg, routing_key=routing_key)

            return {"success": True}

        except Exception as e:
            raise ValueError(f"RabbitMQ 发布失败: {e}") from e
