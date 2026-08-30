"""Graph 解析 fail-closed 测试。"""

import pytest

from mq_worker import _build_spec_from_graph


def test_valid_graph_parses():
    graph = {
        "nodes": [{"id": "n1", "data": {"type": "trigger.manual", "params": {}}}],
        "edges": [],
    }
    spec = _build_spec_from_graph(graph)
    assert len(spec.nodes) == 1
    assert spec.nodes[0].type == "trigger.manual"


def test_node_without_type_fails_closed():
    graph = {"nodes": [{"id": "n1", "data": {}}], "edges": []}
    with pytest.raises(ValueError):
        _build_spec_from_graph(graph)


def test_edge_without_source_fails_closed():
    graph = {
        "nodes": [{"id": "n1", "data": {"type": "trigger.manual"}}],
        "edges": [{"id": "e1", "target": "n1"}],
    }
    with pytest.raises(ValueError):
        _build_spec_from_graph(graph)
