"""Workflow 引擎并行调度与节点级重试测试。"""

import asyncio
from typing import Any

from workflow_engine import (
    RetryableNodeError,
    RunWorkflowSpec,
    WorkflowEdgeDTO,
    WorkflowEvent,
    WorkflowNodeDTO,
    run_workflow,
)


class _CollectingEmitter:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self._lock = asyncio.Lock()

    async def emit(self, event: WorkflowEvent) -> None:
        async with self._lock:
            self.events.append({"event": event.event, "data": event.data})

    def node_events(self, node_id: str):
        return [e for e in self.events if e["data"].get("nodeId") == node_id]


def _manual_node(node_id: str, params: dict[str, Any] | None = None) -> WorkflowNodeDTO:
    return WorkflowNodeDTO(id=node_id, type="trigger.manual", params=params or {})


async def _run(spec: RunWorkflowSpec, context: dict[str, Any] | None = None):
    emitter = _CollectingEmitter()
    await run_workflow(run_id="test-run", spec=spec, emit=emitter.emit, context=context)
    return emitter


def test_diamond_dependency_runs_in_order():
    """A → B,C → D：B/C 并行执行，D 等待两者完成。"""
    a = _manual_node("a")
    b = _manual_node("b")
    c = _manual_node("c")
    d = _manual_node("d")
    spec = RunWorkflowSpec(
        nodes=[a, b, c, d],
        edges=[
            WorkflowEdgeDTO(id="e1", source_node_id="a", source_port_id="payload", target_node_id="b", target_port_id="payload"),
            WorkflowEdgeDTO(id="e2", source_node_id="a", source_port_id="payload", target_node_id="c", target_port_id="payload"),
            WorkflowEdgeDTO(id="e3", source_node_id="b", source_port_id="payload", target_node_id="d", target_port_id="payload"),
            WorkflowEdgeDTO(id="e4", source_node_id="c", source_port_id="payload", target_node_id="d", target_port_id="payload"),
        ],
        entry_nodes=["a"],
    )
    emitter = asyncio.run(_run(spec))
    completed = [e for e in emitter.events if e["event"] == "node_completed"]
    ids = [e["data"]["nodeId"] for e in completed]
    assert ids[0] == "a"
    assert set(ids[1:3]) == {"b", "c"}
    assert ids[3] == "d"
    terminal = [e for e in emitter.events if e["event"] == "workflow_completed"][0]
    assert terminal["data"]["status"] == "success"


def test_retry_policy_retries_then_succeeds():
    """retryPolicy 下可重试异常重试后成功。"""

    class FlakyNode:
        type = "trigger.manual"

        @classmethod
        async def run(cls, inputs, params, context=None):
            attempts = params["_attempts"]
            attempts[0] += 1
            if attempts[0] < 3:
                raise RetryableNodeError("transient")
            return {"payload": "ok"}

    # 用 monkeypatch 替换 workflow_engine 中绑定的 get_node_cls
    import workflow_engine as engine_mod

    original = engine_mod.get_node_cls
    engine_mod.get_node_cls = lambda t: FlakyNode  # type: ignore[assignment]

    try:
        attempts = [0]
        node = _manual_node("n", {"_attempts": attempts, "retryPolicy": {"maxAttempts": 3, "backoffMs": 1}})
        spec = RunWorkflowSpec(nodes=[node], edges=[], entry_nodes=["n"])
        emitter = asyncio.run(_run(spec))
        terminal = [e for e in emitter.events if e["event"] == "workflow_completed"][0]
        assert terminal["data"]["status"] == "success"
        assert attempts[0] == 3
    finally:
        engine_mod.get_node_cls = original


def test_retry_policy_exhausted_fails():
    """retryPolicy 耗尽后仍失败。"""

    class AlwaysFailNode:
        type = "trigger.manual"

        @classmethod
        async def run(cls, inputs, params, context=None):
            raise RetryableNodeError("always")

    import workflow_engine as engine_mod

    original = engine_mod.get_node_cls
    engine_mod.get_node_cls = lambda t: AlwaysFailNode  # type: ignore[assignment]

    try:
        node = _manual_node("n", {"retryPolicy": {"maxAttempts": 2, "backoffMs": 1}})
        spec = RunWorkflowSpec(nodes=[node], edges=[], entry_nodes=["n"])
        emitter = asyncio.run(_run(spec))
        terminal = [e for e in emitter.events if e["event"] == "workflow_completed"][0]
        assert terminal["data"]["status"] == "error"
        assert "always" in terminal["data"]["errorMessage"]
    finally:
        engine_mod.get_node_cls = original
