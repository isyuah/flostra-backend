from __future__ import annotations

import datetime
import decimal
import uuid
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from security.egress import check_outbound_host

from .base import JsonDict, WorkflowNode, register_node


def _json_serializer(obj: Any) -> Any:
    """Helper to serialize SQL types to JSON compatible formats."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, bytes):
        return "<binary>"
    raise TypeError(f"Type {type(obj)} not serializable")


@register_node
class SqlNode(WorkflowNode):
    """
    SQL 执行节点：
    - 支持 PostgreSQL (基于 asyncpg)
    - 使用 SQLAlchemy Core 执行原生 SQL
    - 强制参数化查询以防止注入
    """

    type = "sql-execute"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "SQL 执行",
            "description": "执行 SQL 查询或命令 (PostgreSQL)",
            "category": "Database",
            "icon": "database",
            "inputs": [
                {
                    "name": "connection",
                    "label": "数据库连接 (DSN)",
                    "type": "string",
                    "required": True,
                    "description": "postgresql+asyncpg://user:pass@host:port/dbname",
                },
                {
                    "name": "sql",
                    "label": "SQL 语句",
                    "type": "string",
                    "required": True,
                    "widget": "textarea",
                    "description": "使用 :param_name 作为参数占位符",
                },
                {
                    "name": "parameters",
                    "label": "参数 (JSON)",
                    "type": "object",
                    "required": False,
                    "description": "对应 SQL 中的 :param_name",
                },
            ],
            "outputs": [
                {"name": "rows", "label": "查询结果", "type": "array", "required": True},
                {"name": "row_count", "label": "影响行数", "type": "number", "required": True},
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict, context: JsonDict = None) -> JsonDict:
        dsn = inputs.get("connection")
        sql_raw = inputs.get("sql")
        parameters = inputs.get("parameters") or {}

        if not dsn:
            raise ValueError("缺少数据库连接字符串")
        if not sql_raw:
            raise ValueError("缺少 SQL 语句")

        # 简单的 DSN 校验/补全 (仅针对 PG)
        if not dsn.startswith("postgresql"):
            if dsn.startswith("postgres://"):
                dsn = dsn.replace("postgres://", "postgresql+asyncpg://", 1)
            elif dsn.startswith("postgresql://") and "+asyncpg" not in dsn:
                dsn = dsn.replace("postgresql://", "postgresql+asyncpg://", 1)

        # 出站网络策略：数据库目标必须通过校验。
        _parsed = urlparse(dsn)
        check_outbound_host(_parsed.hostname or "", _parsed.port)

        # 创建引擎 (每次创建销毁对于连接池不是最佳实践，但适合 Serverless/Stateless Worker)
        # 生产环境应当在 Worker 启动时维护全局 Engine Map。
        # 这里作为 MVP，使用 NullPool 或者默认 Pool 但注意 dispose。
        engine = create_async_engine(dsn, echo=False)

        try:
            async with engine.begin() as conn:
                stmt = text(sql_raw)

                result = await conn.execute(stmt, parameters)

                row_count = result.rowcount
                rows: list[dict[str, Any]] = []

                if result.returns_rows:
                    keys = result.keys()
                    for row in result.all():
                        row_dict = {}
                        for idx, key in enumerate(keys):
                            val = row[idx]
                            if isinstance(val, (datetime.datetime, datetime.date, decimal.Decimal, uuid.UUID)):
                                val = _json_serializer(val)
                            row_dict[key] = val
                        rows.append(row_dict)

                return {"rows": rows, "row_count": row_count}

        except Exception as e:
            raise ValueError(f"SQL 执行失败: {e}") from e
        finally:
            await engine.dispose()
