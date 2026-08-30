from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

JsonDict = dict[str, Any]


def _limit_str(s: Any, max_len: int = 200_000) -> str:
    """限制字符串长度，防止异常超大输入"""
    text = str(s) if s is not None else ""
    if len(text) > max_len:
        return text[:max_len]
    return text


class WorkflowNode(ABC):
    """工作流节点基类：负责 schema 定义 + 执行逻辑"""

    # 每个子类必须定义唯一的 type
    type: str

    # ---- 控制端口（默认每个节点 1 入 1 出；可由子类覆盖/扩展）----
    @classmethod
    def control_inputs(cls) -> list[JsonDict]:
        """控制输入端口定义；子类可覆盖"""
        return [
            {
                "name": "__ctl_in",
                "label": "控制输入",
                "type": "control",
                "required": False,
                "extra": {"kind": "control"},
            }
        ]

    @classmethod
    def control_outputs(cls) -> list[JsonDict]:
        """控制输出端口定义；子类可覆盖"""
        return [
            {
                "name": "__ctl_out",
                "label": "控制输出",
                "type": "control",
                "required": False,
                "extra": {"kind": "control"},
            }
        ]

    @classmethod
    def with_control_ports(cls, schema: JsonDict) -> JsonDict:
        """
        为节点 schema 注入控制端口定义：
        - 若 schema 已提供 controlInputs/Outputs，则尊重现有值
        - 否则使用默认的 1 入 1 出 控制端口
        """
        schema.setdefault("controlInputs", cls.control_inputs())
        schema.setdefault("controlOutputs", cls.control_outputs())
        return schema

    @classmethod
    @abstractmethod
    def get_schema(cls) -> JsonDict:
        """返回节点的 JSON 蓝图定义，前端根据这个渲染 UI"""

    @classmethod
    @abstractmethod
    async def run(cls, inputs: JsonDict, params: JsonDict, context: JsonDict = None) -> JsonDict:
        """执行节点逻辑，所有 I/O 都在后端完成"""


_NODE_REGISTRY: dict[str, type[WorkflowNode]] = {}


def register_node(node_cls: type[WorkflowNode]) -> type[WorkflowNode]:
    """将节点类注册到内存表中，提供给路由查询使用"""
    node_type = getattr(node_cls, "type", None)
    if not node_type:
        raise ValueError("Node class must define non-empty `type` attribute")
    if node_type in _NODE_REGISTRY:
        raise ValueError(f"Duplicate node type registered: {node_type}")
    _NODE_REGISTRY[node_type] = node_cls
    return node_cls


def get_all_node_schemas() -> list[JsonDict]:
    """给前端：返回所有节点的 schema 列表"""
    schemas: list[JsonDict] = []
    for cls in _NODE_REGISTRY.values():
        schema = cls.get_schema()
        schema = cls.with_control_ports(schema)
        schemas.append(schema)
    return schemas


def get_node_cls(node_type: str) -> type[WorkflowNode]:
    """根据 type 查找节点类"""
    try:
        return _NODE_REGISTRY[node_type]
    except KeyError as exc:
        raise KeyError(f"Unknown node type: {node_type!r}") from exc
