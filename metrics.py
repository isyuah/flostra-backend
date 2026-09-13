"""Worker 侧 Prometheus 指标。

分层定位：
- Redis 心跳（`worker:online` / `worker:worker:<id>`）继续作为「谁在线、各跑几个」的
  即时读模型，由 Go 控制面聚合成集群视角的 gauge；
- 本模块承载时间序列语义：计数、耗时直方图、按节点类型/事件类型的分布。这些是
  Redis hash 里的单值快照（EWMA 平均值、进程重启即清零）给不了的。

约定：
- 单进程 asyncio worker，不需要 prometheus_client 的多进程聚合模式；
- metrics 端口打开失败（例如本地端口冲突）只记 warning，绝不影响任务执行；
- 标签只能是有限枚举（node_type/event/outcome/reason），execution_id、worker_id
  这类高基数维度由 trace 与日志承载。
"""

from __future__ import annotations

import logging
import os

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

logger = logging.getLogger(__name__)

DEFAULT_METRICS_PORT = 9101
DEFAULT_METRICS_ADDR = "0.0.0.0"

# 私有 registry：测试与嵌入进程不会和 prometheus_client 的全局默认 registry 冲突。
REGISTRY = CollectorRegistry(auto_describe=True)

# 分桶覆盖毫秒级空跑到长连接节点；按「秒」计。
EXECUTION_DURATION_BUCKETS = (0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 900.0)
NODE_DURATION_BUCKETS = (0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0)

EXECUTIONS_TOTAL = Counter(
    "flostra_worker_executions_total",
    "Workflow executions finished by this worker process, by outcome.",
    ["outcome"],
    registry=REGISTRY,
)

EXECUTION_DURATION_SECONDS = Histogram(
    "flostra_worker_execution_duration_seconds",
    "Wall-clock duration of one workflow execution handled by this worker process.",
    buckets=EXECUTION_DURATION_BUCKETS,
    registry=REGISTRY,
)

INFLIGHT = Gauge(
    "flostra_worker_inflight",
    "Workflow executions currently running in this worker process.",
    registry=REGISTRY,
)

CAPACITY = Gauge(
    "flostra_worker_capacity",
    "Configured WORKFLOW_MAX_CONCURRENCY of this worker process.",
    registry=REGISTRY,
)

NODE_DURATION_SECONDS = Histogram(
    "flostra_node_duration_seconds",
    "Node execution duration including retries, by node type and outcome.",
    ["node_type", "status"],
    buckets=NODE_DURATION_BUCKETS,
    registry=REGISTRY,
)

NODE_RETRIES_TOTAL = Counter(
    "flostra_node_retries_total",
    "Retryable node failures that scheduled another attempt.",
    ["node_type"],
    registry=REGISTRY,
)

EVENTS_PUBLISHED_TOTAL = Counter(
    "flostra_worker_events_published_total",
    "Worker events published to the result queue, by event type and outcome.",
    ["event", "outcome"],
    registry=REGISTRY,
)

EGRESS_DENIED_TOTAL = Counter(
    "flostra_egress_denied_total",
    "Outbound targets rejected by the worker egress policy, by reason.",
    ["reason"],
    registry=REGISTRY,
)

SECRET_RESOLUTION_FAILED_TOTAL = Counter(
    "flostra_secret_resolution_failed_total",
    "Secret placeholders that failed to resolve; the run fails closed.",
    registry=REGISTRY,
)


def metrics_port() -> int:
    """Port to serve /metrics on; 0 disables the endpoint."""
    try:
        return int(os.getenv("WORKER_METRICS_PORT", str(DEFAULT_METRICS_PORT)))
    except ValueError:
        return DEFAULT_METRICS_PORT


def metrics_addr() -> str:
    return os.getenv("WORKER_METRICS_ADDR") or DEFAULT_METRICS_ADDR


def start_metrics_server_from_env() -> bool:
    """Start the scrape endpoint, returning False when it is disabled or unavailable."""
    port = metrics_port()
    if port <= 0:
        logger.info("Worker metrics disabled (WORKER_METRICS_PORT=%d)", port)
        return False

    addr = metrics_addr()
    try:
        start_http_server(port, addr=addr, registry=REGISTRY)
    except Exception as exc:  # 端口冲突等部署问题不应让 worker 起不来
        logger.warning("Worker metrics endpoint unavailable on %s:%d: %s", addr, port, exc)
        return False

    logger.info("Worker metrics endpoint listening on %s:%d", addr, port)
    return True
