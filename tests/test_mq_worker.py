"""Worker MQ 消息解析测试。"""

import json
import uuid

import pytest

from mq_worker import WorkflowMQWorker, _build_spec_from_graph


class FakeMessage:
    def __init__(self, body: bytes) -> None:
        self.body = body


def _task_payload(**overrides) -> bytes:
    payload = {
        "executionId": str(uuid.uuid4()),
        "attemptId": str(uuid.uuid4()),
        "workflow": {"id": str(uuid.uuid4()), "workspace": {"id": str(uuid.uuid4())}, "name": "w"},
        "graph": {"nodes": [{"id": "n1", "data": {"type": "trigger.manual", "params": {}}}], "edges": []},
        "secrets": {"API_KEY": "sk-123"},
        "context": {"request": {"method": "GET"}},
    }
    payload.update(overrides)
    return json.dumps(payload).encode("utf-8")


def test_parse_task_extracts_all_fields():
    worker = WorkflowMQWorker()
    parsed = worker._parse_task(FakeMessage(_task_payload()))
    assert parsed.execution_id
    assert parsed.attempt_id
    assert parsed.workflow_meta["workspaceId"]
    assert parsed.secrets == {"API_KEY": "sk-123"}
    assert parsed.extra_context["request"]["method"] == "GET"
    assert len(parsed.spec.nodes) == 1
    assert parsed.spec.nodes[0].type == "trigger.manual"


def test_parse_task_without_attempt_id_is_optional():
    worker = WorkflowMQWorker()
    parsed = worker._parse_task(FakeMessage(_task_payload(attemptId=None)))
    assert parsed.attempt_id is None


def test_parse_task_invalid_attempt_id_fails():
    worker = WorkflowMQWorker()
    with pytest.raises(ValueError):
        worker._parse_task(FakeMessage(_task_payload(attemptId="not-a-uuid")))


def test_parse_task_missing_execution_id_fails():
    worker = WorkflowMQWorker()
    with pytest.raises(ValueError):
        worker._parse_task(FakeMessage(_task_payload(executionId=None)))


def test_safe_ids_never_raise():
    worker = WorkflowMQWorker()
    assert worker._safe_execution_id(FakeMessage(b"not json")) == "unknown"
    assert worker._safe_attempt_id(FakeMessage(b"not json")) is None

    good = FakeMessage(_task_payload())
    assert worker._safe_execution_id(good) != "unknown"
    assert worker._safe_attempt_id(good) is not None


def test_build_spec_from_graph_rejects_missing_node_type():
    graph = {"nodes": [{"id": "n1", "data": {}}], "edges": []}
    with pytest.raises(ValueError):
        _build_spec_from_graph(graph)
