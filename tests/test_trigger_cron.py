"""trigger.cron 节点与控制面 schedule 上下文的对接测试。"""

import asyncio
from typing import Any

from nodes import get_all_node_schemas, get_node_cls
from workflow_engine import (
    RunWorkflowSpec,
    WorkflowEvent,
    WorkflowNodeDTO,
    run_workflow,
)

SCHEDULE_CONTEXT = {
    "trigger": "schedule",
    "schedule": {"name": "nightly", "cronExpression": "0 3 * * *"},
}


class _CollectingEmitter:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self._lock = asyncio.Lock()

    async def emit(self, event: WorkflowEvent) -> None:
        async with self._lock:
            self.events.append({"event": event.event, "data": event.data})


async def _run(spec: RunWorkflowSpec, context: dict[str, Any] | None = None):
    emitter = _CollectingEmitter()
    await run_workflow(run_id="test-run", spec=spec, emit=emitter.emit, context=context)
    return emitter


def _completed_ids(emitter: _CollectingEmitter) -> set[str]:
    return {event["data"].get("nodeId") for event in emitter.events if event["event"] == "node_completed"}


def _node_output(emitter: _CollectingEmitter, node_id: str) -> dict[str, Any]:
    for event in emitter.events:
        if event["event"] == "node_completed" and event["data"].get("nodeId") == node_id:
            return event["data"].get("outputValues") or {}
    raise AssertionError(f"node {node_id!r} did not complete: {emitter.events}")


def test_schema_is_registered_and_documented():
    """前端 palette 依赖注册表导出，节点必须在 definitions 里出现。"""
    assert get_node_cls("trigger.cron").type == "trigger.cron"
    exported = {schema["type"] for schema in get_all_node_schemas()}
    assert "trigger.cron" in exported


def test_run_returns_schedule_metadata_from_context():
    node = WorkflowNodeDTO(id="cron", type="trigger.cron", params={})
    spec = RunWorkflowSpec(nodes=[node], edges=[], entry_nodes=None)

    emitter = asyncio.run(_run(spec, context=dict(SCHEDULE_CONTEXT)))

    assert _node_output(emitter, "cron") == {
        "schedule": {"name": "nightly", "cronExpression": "0 3 * * *"},
        "scheduleName": "nightly",
        "cronExpression": "0 3 * * *",
    }


def test_run_without_schedule_context_degrades_to_empty_fields():
    """没有 schedule 上下文（例如显式入口的手动运行）不应抛异常。"""
    node = WorkflowNodeDTO(id="cron", type="trigger.cron", params={})
    spec = RunWorkflowSpec(nodes=[node], edges=[], entry_nodes=["cron"])

    emitter = asyncio.run(_run(spec, context={"trigger": "manual"}))

    assert _node_output(emitter, "cron") == {"schedule": {}, "scheduleName": "", "cronExpression": ""}


def test_default_entry_selection_respects_the_trigger_context():
    """无显式入口时按运行上下文挑入口：手动运行不该从 cron 入口进入。"""

    def _spec() -> RunWorkflowSpec:
        return RunWorkflowSpec(
            nodes=[
                WorkflowNodeDTO(id="manual", type="trigger.manual", params={}),
                WorkflowNodeDTO(id="cron", type="trigger.cron", params={}),
            ],
            edges=[],
            entry_nodes=None,
        )

    manual_run = _spec()
    manual_emitter = asyncio.run(_run(manual_run, context={"trigger": "manual"}))
    assert set(manual_run.entry_nodes or []) == {"manual"}
    assert _completed_ids(manual_emitter) == {"manual"}

    cron_run = _spec()
    cron_emitter = asyncio.run(_run(cron_run, context=dict(SCHEDULE_CONTEXT)))
    # trigger.manual 不受上下文门槛约束：cron 运行时两个入口都可达。
    assert set(cron_run.entry_nodes or []) == {"manual", "cron"}
    assert _completed_ids(cron_emitter) == {"manual", "cron"}
