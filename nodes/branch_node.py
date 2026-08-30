from __future__ import annotations

import ast
import operator
from typing import Any

from .base import JsonDict, WorkflowNode, register_node


@register_node
class BranchNode(WorkflowNode):
    """
    分支节点：根据表达式选择一个或多个分支输出（控制流）
    """

    type = "branch"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "分支",
            "description": "按顺序执行布尔表达式，命中分支触发控制流，未命中可走默认分支",
            "category": "逻辑",
            "icon": "branch",
            "inputs": [
                {"name": "input", "label": "输入", "type": "any", "required": False},
                {"name": "context", "label": "上下文", "type": "object", "required": False},
            ],
            "outputs": [
                {"name": "__matched", "label": "命中分支列表", "type": "array", "required": False},
            ],
            "controlOutputs": [],
            "parameters": [
                {
                    "name": "cases",
                    "label": "分支列表",
                    "type": "array",
                    "widget": "ports-list",
                    "extra": {
                        "role": "branch-cases",
                        "template": {"name": "", "label": "", "expr": ""},
                        "effects": ["cal_control_output_ports"],
                    },
                    "required": True,
                    "desc": "expr 为受限 Python 表达式，仅可引用 input/context；分支 name 决定控制输出端口。",
                },
                {
                    "name": "default",
                    "label": "默认分支",
                    "type": "string",
                    "widget": "input",
                    "required": False,
                    "desc": "无分支命中时使用该分支名称触发；留空则依据 allow_empty 抛错或返回空。",
                },
                {
                    "name": "mode",
                    "label": "匹配模式",
                    "type": "string",
                    "widget": "select",
                    "default": "first_match",
                    "required": False,
                    "extra": {"options": [{"label": "首个命中", "value": "first_match"}, {"label": "全部命中", "value": "all_match"}]},
                },
                {
                    "name": "allow_empty",
                    "label": "允许无匹配",
                    "type": "boolean",
                    "widget": "switch",
                    "default": False,
                    "required": False,
                },
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict, context: JsonDict = None) -> JsonDict:
        inp = inputs.get("input")
        ctx = inputs.get("context")
        cases = params.get("cases") or []
        default_branch = params.get("default") or None
        mode = (params.get("mode") or "first_match").lower()
        allow_empty = bool(params.get("allow_empty", False))

        def safe_get(name: str):
            if name == "input":
                return inp
            if name == "context":
                return ctx
            raise KeyError(name)

        matched: list[str] = []

        for case in cases:
            if not isinstance(case, dict):
                continue
            branch_id = case.get("name") or case.get("id")
            expr = case.get("expr")
            if not branch_id or not expr:
                continue
            try:
                if cls._eval_expr(expr, safe_get):
                    matched.append(branch_id)
                    if mode == "first_match":
                        break
            except Exception as exc:
                raise ValueError(f"分支 {branch_id} 表达式错误: {exc}") from exc

        if not matched and default_branch:
            matched = [default_branch]

        if not matched and not allow_empty:
            raise ValueError("分支未命中且未配置默认分支")

        outputs: dict[str, Any] = {}
        if matched:
            outputs["__matched"] = matched

        control_signals = {mid: True for mid in matched}
        outputs["controlSignals"] = control_signals
        return outputs

    @staticmethod
    def _eval_expr(expr: str, var_getter) -> bool:
        allowed_bin = {
            ast.Add: operator.add,
            ast.Sub: operator.sub,
            ast.Mult: operator.mul,
            ast.Div: operator.truediv,
            ast.Mod: operator.mod,
            ast.Pow: operator.pow,
        }
        allowed_unary = {ast.Not: operator.not_, ast.USub: operator.neg, ast.UAdd: operator.pos}
        allowed_cmp = {
            ast.Eq: operator.eq,
            ast.NotEq: operator.ne,
            ast.Lt: operator.lt,
            ast.LtE: operator.le,
            ast.Gt: operator.gt,
            ast.GtE: operator.ge,
        }

        def eval_node(n):
            if isinstance(n, ast.Constant):
                return n.value
            if isinstance(n, ast.Name):
                return var_getter(n.id)
            if isinstance(n, ast.Attribute):
                val = eval_node(n.value)
                return getattr(val, n.attr, None)
            if isinstance(n, ast.Subscript):
                val = eval_node(n.value)
                key = eval_node(n.slice)
                try:
                    return val[key]
                except Exception:
                    return None
            if isinstance(n, ast.Index):  # py<3.9
                return eval_node(n.value)
            if isinstance(n, ast.BinOp) and type(n.op) in allowed_bin:
                return allowed_bin[type(n.op)](eval_node(n.left), eval_node(n.right))
            if isinstance(n, ast.UnaryOp) and type(n.op) in allowed_unary:
                return allowed_unary[type(n.op)](eval_node(n.operand))
            if isinstance(n, ast.BoolOp):
                if isinstance(n.op, ast.And):
                    return all(eval_node(v) for v in n.values)
                if isinstance(n.op, ast.Or):
                    return any(eval_node(v) for v in n.values)
            if isinstance(n, ast.Compare):
                left = eval_node(n.left)
                result = True
                for op, comparator in zip(n.ops, n.comparators):
                    right = eval_node(comparator)
                    if type(op) not in allowed_cmp:
                        raise ValueError("unsupported comparator")
                    if not allowed_cmp[type(op)](left, right):
                        result = False
                        break
                    left = right
                return result
            if isinstance(n, (ast.List, ast.Tuple)):
                return [eval_node(e) for e in n.elts]
            if isinstance(n, ast.Dict):
                return {eval_node(k): eval_node(v) for k, v in zip(n.keys, n.values)}
            raise ValueError(f"表达式包含不支持的语法: {type(n).__name__}")

        return bool(eval_node(ast.parse(expr, mode="eval").body))
