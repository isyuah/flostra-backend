from __future__ import annotations

import asyncio
import collections
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from nodes import get_node_cls
from secrets_store import resolve_secrets_in_params

JsonDict = dict[str, Any]


@dataclass
class WorkflowNodeDTO:
    """前端传入的节点定义（运行时视角）"""

    id: str
    type: str
    params: JsonDict
    port_constants: JsonDict | None = None


@dataclass
class WorkflowEdgeDTO:
    """前端传入的边定义（运行时视角）"""

    id: str
    source_node_id: str
    source_port_id: str
    target_node_id: str
    target_port_id: str
    kind: str = "data"  # "data" or "control"


@dataclass
class OverrideDTO:
    """单次运行的端口覆盖值"""

    node_id: str
    port_id: str
    value: Any


@dataclass
class RunWorkflowSpec:
    """一次运行需要的完整图结构"""

    nodes: list[WorkflowNodeDTO]
    edges: list[WorkflowEdgeDTO]
    entry_nodes: list[str] | None = None
    targets: list[str] | None = None
    overrides: list[OverrideDTO] = None

    def __post_init__(self) -> None:
        if self.overrides is None:
            self.overrides = []


@dataclass
class WorkflowEvent:
    """发给 SSE 层的事件"""

    event: str
    data: JsonDict


EventEmitter = Callable[[WorkflowEvent], Awaitable[None]]


def _build_graph(
    spec: RunWorkflowSpec,
) -> dict[str, Any]:
    """根据节点和边构建拓扑所需的辅助结构"""
    node_map: dict[str, WorkflowNodeDTO] = {n.id: n for n in spec.nodes}

    # 出边 / 入边表（区分 data 与 control）
    out_edges_data: dict[str, list[WorkflowEdgeDTO]] = {n.id: [] for n in spec.nodes}
    in_edges_data: dict[str, list[WorkflowEdgeDTO]] = {n.id: [] for n in spec.nodes}
    out_edges_ctl: dict[str, list[WorkflowEdgeDTO]] = {n.id: [] for n in spec.nodes}
    in_edges_ctl: dict[str, list[WorkflowEdgeDTO]] = {n.id: [] for n in spec.nodes}

    indegree_all: dict[str, int] = {n.id: 0 for n in spec.nodes}
    indegree_ctl: dict[str, int] = {n.id: 0 for n in spec.nodes}

    for e in spec.edges:
        if e.source_node_id not in node_map or e.target_node_id not in node_map:
            raise ValueError(f"Edge {e.id!r} references unknown node(s)")
        kind = (e.kind or "data").lower()
        if kind not in {"data", "control"}:
            raise ValueError(f"Edge {e.id!r} has invalid kind {kind!r}")

        indegree_all[e.target_node_id] += 1

        if kind == "data":
            out_edges_data[e.source_node_id].append(e)
            in_edges_data[e.target_node_id].append(e)
        else:
            out_edges_ctl[e.source_node_id].append(e)
            in_edges_ctl[e.target_node_id].append(e)
            indegree_ctl[e.target_node_id] += 1

    # Kahn 拓扑排序，要求是 DAG
    queue: collections.deque[str] = collections.deque(
        [nid for nid, deg in indegree_all.items() if deg == 0]
    )
    topo_order: list[str] = []

    while queue:
        nid = queue.popleft()
        topo_order.append(nid)
        for e in out_edges_data.get(nid, []) + out_edges_ctl.get(nid, []):
            indegree_all[e.target_node_id] -= 1
            if indegree_all[e.target_node_id] == 0:
                queue.append(e.target_node_id)

    if len(topo_order) != len(spec.nodes):
        raise ValueError("Workflow graph contains cycles, DAG is required")

    # 控制子图也需无环
    queue_ctl: collections.deque[str] = collections.deque(
        [nid for nid, deg in indegree_ctl.items() if deg == 0]
    )
    visited_ctl: list[str] = []
    while queue_ctl:
        nid = queue_ctl.popleft()
        visited_ctl.append(nid)
        for e in out_edges_ctl.get(nid, []):
            indegree_ctl[e.target_node_id] -= 1
            if indegree_ctl[e.target_node_id] == 0:
                queue_ctl.append(e.target_node_id)
    nodes_with_ctl = {nid for nid, edges in in_edges_ctl.items() if edges} | {
        nid for nid, edges in out_edges_ctl.items() if edges
    }
    if nodes_with_ctl and len(visited_ctl) < len(nodes_with_ctl):
        raise ValueError("Control subgraph contains cycles, DAG is required")

    # 出度为 0 的节点（sink）
    sink_nodes = [
        nid
        for nid in node_map
        if not out_edges_data.get(nid) and not out_edges_ctl.get(nid)
    ]

    # 入度为 0 的节点（source）
    source_nodes = [nid for nid, deg in indegree_all.items() if deg == 0]

    # End 节点：约定 type == "end"
    end_nodes = [nid for nid, n in node_map.items() if n.type == "end"]

    return {
        "node_map": node_map,
        "out_edges_data": out_edges_data,
        "in_edges_data": in_edges_data,
        "out_edges_ctl": out_edges_ctl,
        "in_edges_ctl": in_edges_ctl,
        "topo_order": topo_order,
        "sink_nodes": sink_nodes,
        "source_nodes": source_nodes,
        "end_nodes": end_nodes,
    }


def _apply_overrides(
    node_id: str,
    base_inputs: JsonDict,
    overrides_by_node: dict[str, list[OverrideDTO]],
) -> JsonDict:
    """将 overrides 应用到某个节点的输入上，后写覆盖先写"""
    result = dict(base_inputs)
    for ov in overrides_by_node.get(node_id, []):
        result[ov.port_id] = ov.value
    return result


def _merge_inputs_for_node(
    node_id: str,
    node: WorkflowNodeDTO,
    in_edges_data: dict[str, list[WorkflowEdgeDTO]],
    context_outputs: dict[str, JsonDict],
    port_constants: JsonDict | None = None,
) -> JsonDict:
    """
    汇总某个节点的输入：
    - 先应用 port_constants（仅作为“未连线时的默认值”）
    - 再叠加来自所有入边的上游输出（上游优先级更高）
    """
    inputs: JsonDict = {}

    # 1) 先用常量端口作为默认值
    constants = port_constants if port_constants is not None else node.port_constants
    if constants:
        inputs.update(constants)

    # 2) 再用连线覆盖：有上游时，以上游为准
    for edge in in_edges_data.get(node_id, []):
        source_outputs = context_outputs.get(edge.source_node_id, {})
        if edge.source_port_id in source_outputs:
            inputs[edge.target_port_id] = source_outputs[edge.source_port_id]

    return inputs


async def run_workflow(
    run_id: str,
    spec: RunWorkflowSpec,
    emit: EventEmitter,
    context: dict[str, Any] | None = None,
) -> None:
    """
    执行整张工作流：
    - 先拓扑排序
    - 就绪节点并发执行（互不依赖的节点并行），事件仍按拓扑序发布
    - 支持节点级 retryPolicy（对可重试异常做指数退避重试）
    - 通过 emit 回调把事件抛给外层（通常是 SSE）
    """
    graph = _build_graph(spec)
    node_map: dict[str, WorkflowNodeDTO] = graph["node_map"]
    out_edges_data: dict[str, list[WorkflowEdgeDTO]] = graph["out_edges_data"]
    in_edges_data: dict[str, list[WorkflowEdgeDTO]] = graph["in_edges_data"]
    out_edges_ctl: dict[str, list[WorkflowEdgeDTO]] = graph["out_edges_ctl"]
    in_edges_ctl: dict[str, list[WorkflowEdgeDTO]] = graph["in_edges_ctl"]
    topo_order: list[str] = graph["topo_order"]
    sink_nodes: list[str] = graph["sink_nodes"]
    end_nodes: list[str] = graph["end_nodes"]

    # 预处理 overrides：按 node 分组
    overrides_by_node: dict[str, list[OverrideDTO]] = {}
    for ov in spec.overrides:
        overrides_by_node.setdefault(ov.node_id, []).append(ov)

    context_outputs: dict[str, JsonDict] = {}
    control_fired: set[str] = set()  # 已触发的控制边 ID
    failed_node: str | None = None
    failure_message: str | None = None

    def should_skip_node(node_id: str) -> bool:
        ctl_in_edges = in_edges_ctl.get(node_id, [])
        data_in_edges = in_edges_data.get(node_id, [])

        # 控制门槛：若有控制入边，则需要全部触发才执行
        if ctl_in_edges:
            for e in ctl_in_edges:
                if e.id not in control_fired:
                    return True

        # 无数据入边 & 无常量/override 时可视为“纯控制驱动”，已经通过上面的控制门槛即可执行
        if not data_in_edges and not node_map[node_id].port_constants and not overrides_by_node.get(node_id):
            return False

        # 无入边的节点（数据）永远执行
        if not data_in_edges:
            return False
        # 只要有常量或 overrides，就执行
        if node_map[node_id].port_constants:
            return False
        if overrides_by_node.get(node_id):
            return False
        # 如果至少有一个数据入边已经产生了对应端口的输出，则执行
        for e in data_in_edges:
            if e.source_node_id in context_outputs:
                src_out = context_outputs[e.source_node_id]
                if e.source_port_id in src_out:
                    return False
        # 没有任何可用输入，则跳过
        return True

    # 计算可达子图（REACHABLE_SET）
    all_nodes = set(node_map.keys())
    reachable: set[str]

    if not spec.entry_nodes:
        trigger_entries = [nid for nid in graph["source_nodes"] if node_map[nid].type.startswith("trigger.")]
        if trigger_entries:
            default_entries = trigger_entries
            if not (context or {}).get("request"):
                default_entries = [nid for nid in default_entries if node_map[nid].type != "trigger.http"]
        else:
            default_entries = [nid for nid in graph["source_nodes"]]

        if default_entries:
            spec.entry_nodes = default_entries

    if spec.entry_nodes is not None and len(spec.entry_nodes) == 0:
        reachable = set()
    elif spec.entry_nodes:
        for nid in spec.entry_nodes:
            if nid not in node_map:
                raise ValueError(f"entry node {nid!r} not found in workflow nodes")

        reachable = set()
        queue: collections.deque[str] = collections.deque(spec.entry_nodes)
        while queue:
            nid = queue.popleft()
            if nid in reachable:
                continue
            reachable.add(nid)
            for edge in out_edges_data.get(nid, []) + out_edges_ctl.get(nid, []):
                queue.append(edge.target_node_id)
    else:
        reachable = all_nodes

    # 在全局拓扑序基础上过滤得到本次实际执行顺序
    effective_order: list[str] = [nid for nid in topo_order if nid in reachable]

    # 触发全局开始事件
    await emit(WorkflowEvent(event="workflow_started", data={"runId": run_id}))

    # 并发调度：按拓扑序扫描，每轮收集“依赖已满足且尚未执行”的节点，
    # 用 asyncio.gather 并行执行；事件发布仍按拓扑序串行，保证前端时序稳定。
    remaining = set(effective_order)
    while remaining and failed_node is None:
        ready = [
            nid for nid in effective_order
            if nid in remaining and not should_skip_node(nid)
        ]
        if not ready:
            # 没有任何节点可执行：剩余节点全部跳过
            for nid in effective_order:
                if nid in remaining:
                    await emit(
                        WorkflowEvent(
                            event="node_skipped",
                            data={"runId": run_id, "nodeId": nid, "reason": "no_input"},
                        )
                    )
            remaining.clear()
            break

        async def run_one(node_id: str) -> None:
            nonlocal failed_node, failure_message
            node = node_map[node_id]
            try:
                await _execute_node(
                    node_id=node_id,
                    node=node,
                    run_id=run_id,
                    emit=emit,
                    context=context,
                    context_outputs=context_outputs,
                    in_edges_data=in_edges_data,
                    overrides_by_node=overrides_by_node,
                    out_edges_ctl=out_edges_ctl,
                    control_fired=control_fired,
                )
            except NodeExecutionError as exc:
                failed_node = node_id
                failure_message = exc.message
                raise

        # 并行执行本批就绪节点；任一失败即短路（gather 会等待其它任务完成，
        # 但失败节点之后不会再调度新批次）。run_one 已在失败时设置
        # failed_node/failure_message，这里只需吞掉已处理的 NodeExecutionError。
        await asyncio.gather(
            *(run_one(nid) for nid in ready),
            return_exceptions=True,
        )
        for nid in ready:
            remaining.discard(nid)

    if failed_node is not None:
        await emit(
            WorkflowEvent(
                event="node_completed",
                data={
                    "runId": run_id,
                    "nodeId": failed_node,
                    "status": "error",
                    "errorMessage": failure_message or "node failed",
                },
            )
        )
        await emit(
            WorkflowEvent(
                event="workflow_completed",
                data={
                    "runId": run_id,
                    "status": "error",
                    "errorMessage": failure_message or "node failed",
                },
            )
        )
        return

    # 计算最终返回结果
    targets: list[str]
    if spec.targets:
        targets = [nid for nid in spec.targets if nid in reachable]
    else:
        reachable_ends = [nid for nid in end_nodes if nid in reachable]
        if reachable_ends:
            targets = reachable_ends
        else:
            reachable_sinks = [nid for nid in sink_nodes if nid in reachable]
            targets = reachable_sinks

    results_dict: dict[str, Any] = {
        nid: context_outputs.get(nid) for nid in targets if nid in context_outputs
    }

    if len(results_dict) == 1:
        results: Any = next(iter(results_dict.values()))
    else:
        results = results_dict

    await emit(
        WorkflowEvent(
            event="workflow_completed",
            data={
                "runId": run_id,
                "status": "success",
                "results": results,
            },
        )
    )


class NodeExecutionError(Exception):
    """节点执行失败（携带用户可见的错误信息）。"""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class RetryableNodeError(NodeExecutionError):
    """可重试的节点异常（配合 retryPolicy 使用）。"""


async def _execute_node(
    *,
    node_id: str,
    node: WorkflowNodeDTO,
    run_id: str,
    emit: EventEmitter,
    context: dict[str, Any] | None,
    context_outputs: dict[str, JsonDict],
    in_edges_data: dict[str, list[WorkflowEdgeDTO]],
    overrides_by_node: dict[str, list[OverrideDTO]],
    out_edges_ctl: dict[str, list[WorkflowEdgeDTO]],
    control_fired: set[str],
) -> None:
    """执行单个节点：事件发布 + secret 解析 + retryPolicy 重试。"""

    # 广播节点开始
    await emit(
        WorkflowEvent(
            event="node_started",
            data={"runId": run_id, "nodeId": node_id},
        )
    )

    #    port_constants 属于作者静态配置，与 params 同等可信，可参与 secret 替换；
    #    inputs（上游节点输出）是不可信数据，永不替换。
    secret_map = (context or {}).get("secrets")
    safe_constants = (
        resolve_secrets_in_params(node.port_constants, secret_map)
        if secret_map and node.port_constants
        else node.port_constants
    )
    base_inputs = _merge_inputs_for_node(
        node_id=node_id,
        node=node,
        in_edges_data=in_edges_data,
        context_outputs=context_outputs,
        port_constants=safe_constants,
    )
    inputs = _apply_overrides(
        node_id=node_id,
        base_inputs=base_inputs,
        overrides_by_node=overrides_by_node,
    )

    # 仅对 params 进行 [[ KEY ]] 替换，inputs 保持原样
    safe_params = resolve_secrets_in_params(node.params, secret_map) if secret_map else node.params
    safe_inputs = inputs  # Inputs do not support secret replacement

    retry_policy = _parse_retry_policy(node.params.get("retryPolicy"))

    try:
        node_cls = get_node_cls(node.type)
    except KeyError as exc:
        raise NodeExecutionError(f"unknown node type: {node.type!r}") from exc

    last_exc: Exception | None = None
    for attempt in range(1, retry_policy["maxAttempts"] + 1):
        try:
            outputs = await node_cls.run(safe_inputs, safe_params, context=context)
            last_exc = None
            break
        except RetryableNodeError as exc:
            last_exc = exc
            if attempt < retry_policy["maxAttempts"]:
                await asyncio.sleep(retry_policy["backoffMs"] / 1000 * (2 ** (attempt - 1)))
                continue
            raise
        except Exception as exc:
            # 非可重试异常：直接失败
            raise NodeExecutionError(str(exc)) from exc

    if last_exc is not None:
        raise NodeExecutionError(str(last_exc)) from last_exc

    control_signals: dict[str, bool] | None = None
    data_outputs: Any = outputs
    if isinstance(outputs, dict):
        cs_candidate = outputs.get("controlSignals")
        if isinstance(cs_candidate, dict):
            control_signals = {k: bool(v) for k, v in cs_candidate.items()}
        data_outputs = dict(outputs)
        if "controlSignals" in data_outputs:
            data_outputs.pop("controlSignals")

    context_outputs[node_id] = data_outputs

    # 触发控制边：默认触发该节点所有控制输出；若提供 controlSignals，则只触发为 True 的端口
    allowed_ports: set[str] | None = None
    if control_signals is not None:
        allowed_ports = {pid for pid, flag in control_signals.items() if flag}
    for e in out_edges_ctl.get(node_id, []):
        if allowed_ports is None or e.source_port_id in allowed_ports:
            control_fired.add(e.id)

    await emit(
        WorkflowEvent(
            event="node_completed",
            data={
                "runId": run_id,
                "nodeId": node_id,
                "status": "success",
                "inputValues": inputs,
                "outputValues": outputs,
            },
        )
    )


def _parse_retry_policy(raw: Any) -> dict[str, int]:
    """解析节点 retryPolicy 参数：{maxAttempts, backoffMs}。"""
    if not isinstance(raw, dict):
        return {"maxAttempts": 1, "backoffMs": 0}
    try:
        max_attempts = max(1, min(int(raw.get("maxAttempts", 1)), 10))
    except (TypeError, ValueError):
        max_attempts = 1
    try:
        backoff_ms = max(0, min(int(raw.get("backoffMs", 200)), 30_000))
    except (TypeError, ValueError):
        backoff_ms = 0
    return {"maxAttempts": max_attempts, "backoffMs": backoff_ms}
