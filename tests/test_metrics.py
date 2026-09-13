"""Worker 指标测试：事件发布、执行终态、节点耗时/重试、安全策略计数。"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

import pytest

from metrics import REGISTRY
from mq_worker import MQEventEmitter, WorkflowMQWorker
from secrets_store import SecretResolutionError, resolve_secrets_in_params
from security.egress import EgressError, check_outbound_host, check_outbound_url
from workflow_engine import (
    RetryableNodeError,
    RunWorkflowSpec,
    WorkflowEvent,
    WorkflowNodeDTO,
    run_workflow,
)


def _value(name: str, labels: dict[str, str] | None = None) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


class FakeExchange:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.published: list[dict[str, Any]] = []

    async def publish(self, message: Any, routing_key: str) -> None:
        if self.fail:
            raise ConnectionError("broker down")
        self.published.append({"body": message.body, "routing_key": routing_key})


class FakeChannel:
    def __init__(self, fail: bool = False) -> None:
        self.default_exchange = FakeExchange(fail=fail)


class FakeMessage:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.acked = False
        self.requeued: bool | None = None

    async def ack(self) -> None:
        self.acked = True

    async def nack(self, requeue: bool = False) -> None:
        self.requeued = requeue


def _task_payload() -> bytes:
    return json.dumps(
        {
            "executionId": str(uuid.uuid4()),
            "attemptId": str(uuid.uuid4()),
            "workflow": {"id": str(uuid.uuid4()), "workspace": {"id": str(uuid.uuid4())}, "name": "w"},
            "graph": {"nodes": [{"id": "n1", "data": {"type": "trigger.manual", "params": {}}}], "edges": []},
            "secrets": {},
            "context": {},
        }
    ).encode("utf-8")


def test_event_publish_outcomes_are_counted():
    published_before = _value("flostra_worker_events_published_total", {"event": "node_started", "outcome": "published"})
    failed_before = _value("flostra_worker_events_published_total", {"event": "workflow_heartbeat", "outcome": "error"})

    good = MQEventEmitter(FakeChannel(), "workflow.run_result", "exec-1", "attempt-1", "worker-1")
    asyncio.run(good.emit(WorkflowEvent(event="node_started", data={"nodeId": "n1"})))

    broken = MQEventEmitter(FakeChannel(fail=True), "workflow.run_result", "exec-1", "attempt-1", "worker-1")
    with pytest.raises(ConnectionError):
        asyncio.run(broken.emit(WorkflowEvent(event="workflow_heartbeat", data={})))

    assert _value("flostra_worker_events_published_total", {"event": "node_started", "outcome": "published"}) == published_before + 1
    assert _value("flostra_worker_events_published_total", {"event": "workflow_heartbeat", "outcome": "error"}) == failed_before + 1


def test_successful_run_records_outcome_duration_and_inflight():
    success_before = _value("flostra_worker_executions_total", {"outcome": "success"})
    duration_before = _value("flostra_worker_execution_duration_seconds_count")

    worker = WorkflowMQWorker()
    worker._channel = FakeChannel()  # type: ignore[assignment]
    message = FakeMessage(_task_payload())

    asyncio.run(worker._handle_message(message))  # type: ignore[arg-type]

    assert _value("flostra_worker_executions_total", {"outcome": "success"}) == success_before + 1
    assert _value("flostra_worker_execution_duration_seconds_count") == duration_before + 1
    assert _value("flostra_worker_inflight") == 0
    assert message.acked is True
    # 终态事件成功发布后才 ACK；这里没有 NACK 重投路径。
    assert message.requeued is None


async def _emit_noop(_event: WorkflowEvent) -> None:
    return None


def _flaky_node_cls(attempts: list[int], fail_times: int):
    class FlakyNode:
        @classmethod
        async def run(cls, inputs, params, context=None):
            attempts[0] += 1
            if attempts[0] <= fail_times:
                raise RetryableNodeError("transient")
            return {"payload": "ok"}

    return FlakyNode


def test_node_duration_and_retries_are_recorded(monkeypatch):
    import workflow_engine as engine_mod

    attempts = [0]
    monkeypatch.setattr(engine_mod, "get_node_cls", lambda _type: _flaky_node_cls(attempts, fail_times=2))

    retries_before = _value("flostra_node_retries_total", {"node_type": "trigger.manual"})
    success_before = _value("flostra_node_duration_seconds_count", {"node_type": "trigger.manual", "status": "success"})

    node = WorkflowNodeDTO(id="n", type="trigger.manual", params={"retryPolicy": {"maxAttempts": 3, "backoffMs": 1}})
    spec = RunWorkflowSpec(nodes=[node], edges=[], entry_nodes=["n"])
    asyncio.run(run_workflow(run_id="run-1", spec=spec, emit=_emit_noop, context=None))

    assert attempts[0] == 3
    assert _value("flostra_node_retries_total", {"node_type": "trigger.manual"}) == retries_before + 2
    assert _value("flostra_node_duration_seconds_count", {"node_type": "trigger.manual", "status": "success"}) == success_before + 1


def test_failed_node_is_recorded_as_error(monkeypatch):
    import workflow_engine as engine_mod

    class AlwaysFailNode:
        @classmethod
        async def run(cls, inputs, params, context=None):
            raise ValueError("boom")

    monkeypatch.setattr(engine_mod, "get_node_cls", lambda _type: AlwaysFailNode)

    error_before = _value("flostra_node_duration_seconds_count", {"node_type": "trigger.manual", "status": "error"})

    node = WorkflowNodeDTO(id="n", type="trigger.manual", params={})
    spec = RunWorkflowSpec(nodes=[node], edges=[], entry_nodes=["n"])
    asyncio.run(run_workflow(run_id="run-2", spec=spec, emit=_emit_noop, context=None))

    assert _value("flostra_node_duration_seconds_count", {"node_type": "trigger.manual", "status": "error"}) == error_before + 1


def test_egress_denials_are_counted_by_bounded_reason():
    private_before = _value("flostra_egress_denied_total", {"reason": "private_network"})
    metadata_before = _value("flostra_egress_denied_total", {"reason": "metadata"})
    scheme_before = _value("flostra_egress_denied_total", {"reason": "invalid_scheme"})

    with pytest.raises(EgressError):
        check_outbound_host("127.0.0.1", 80)
    with pytest.raises(EgressError):
        check_outbound_host("169.254.169.254", 80)
    with pytest.raises(EgressError):
        check_outbound_url("ftp://example.com/file")

    assert _value("flostra_egress_denied_total", {"reason": "private_network"}) == private_before + 1
    assert _value("flostra_egress_denied_total", {"reason": "metadata"}) == metadata_before + 1
    assert _value("flostra_egress_denied_total", {"reason": "invalid_scheme"}) == scheme_before + 1


def test_secret_fail_closed_is_counted():
    before = _value("flostra_secret_resolution_failed_total")

    with pytest.raises(SecretResolutionError):
        resolve_secrets_in_params({"key": "[[MISSING]]"}, {"OTHER": "x"})
    with pytest.raises(SecretResolutionError):
        resolve_secrets_in_params({"key": "[[ANY]]"}, {})

    assert _value("flostra_secret_resolution_failed_total") == before + 2


def test_metadata_stays_blocked_when_private_networks_are_allowed(monkeypatch):
    """显式放行私网时，metadata 端点仍必须被拒绝并单独计数。"""

    monkeypatch.setenv("WORKER_EGRESS_ALLOW_PRIVATE", "1")
    metadata_before = _value("flostra_egress_denied_total", {"reason": "metadata"})

    with pytest.raises(EgressError):
        check_outbound_host("fd00:ec2::254", 80)
    with pytest.raises(EgressError):
        check_outbound_host("169.254.169.254", 80)

    assert _value("flostra_egress_denied_total", {"reason": "metadata"}) == metadata_before + 2
