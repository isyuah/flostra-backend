from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Option:
    """通用选项项，用于 select 等控件"""

    label: str
    value: Any


@dataclass
class Field:
    """
    前端可渲染的字段描述统一规范：
    - 必填: name, label, type
    - 常用: desc, required, default, widget, extra
    - 复合类型: objectSchema / itemSchema 在 extra 中递归使用本 Field 结构
    """

    name: str
    label: str
    # 支持单类型或类型列表（端口多类型声明场景）
    type: str | list[str]  # string | number | boolean | object | array | any | [..]
    desc: str | None = None
    required: bool = False
    default: Any = None
    widget: str | None = None  # input | textarea | select | slider | switch | json-editor | secret | model-picker | ports-list | plugin-list | code-editor | custom
    extra: dict[str, Any] = field(default_factory=dict)


def field_option(label: str, value: Any) -> dict[str, Any]:
    """便捷构建 option"""
    return {"label": label, "value": value}


def field_object(
    name: str,
    label: str,
    object_schema: list[Field],
    desc: str | None = None,
    required: bool = False,
    widget: str | None = None,
    default: Any = None,
) -> Field:
    """构建 object 类型字段"""
    return Field(
        name=name,
        label=label,
        type="object",
        desc=desc,
        required=required,
        widget=widget,
        default=default,
        extra={"objectSchema": [f.__dict__ for f in object_schema]},
    )


def field_array(
    name: str,
    label: str,
    item_schema: Field,
    desc: str | None = None,
    required: bool = False,
    widget: str | None = None,
    default: Any = None,
) -> Field:
    """构建 array 类型字段"""
    return Field(
        name=name,
        label=label,
        type="array",
        desc=desc,
        required=required,
        widget=widget,
        default=default,
        extra={"itemSchema": item_schema.__dict__},
    )
