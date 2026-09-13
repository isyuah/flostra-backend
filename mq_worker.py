from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import socket
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from time import monotonic
from typing import Any

import aio_pika
from aio_pika import DeliveryMode, IncomingMessage, Message
from redis import asyncio as aioredis

from logging_setup import log_context, secret_scope
from metrics import (
    CAPACITY,
    EVENTS_PUBLISHED_TOTAL,
    EXECUTION_DURATION_SECONDS,
    EXECUTIONS_TOTAL,
    INFLIGHT,
)
from security.event_safety import sanitize_event_data, serialize_event_payload
from workflow_engine import (
    OverrideDTO,
    RunWorkflowSpec,
    WorkflowEdgeDTO,
    WorkflowEvent,
    WorkflowNodeDTO,
    run_workflow,
)

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _positive_float_env(name: str, default: float) -> float:
    """读取正浮点环境变量，错误配置时保留一个可工作的最小周期。"""
    try:
        return max(float(os.getenv(name, str(default))), 0.1)
    except (TypeError, ValueError):
        return default


def _first_non_null(*values: Any) -> Any:
    """返回第一个非 None 的值。"""
    for v in values:
        if v is not None:
            return v
    return None


def _from_nested(d: dict[str, Any], key: str, subkey: str | None = None) -> Any:
    """安全地从嵌套 dict 中取值。"""
    if subkey is None:
        return d.get(key)
    obj = d.get(key)
    if isinstance(obj, dict):
        return obj.get(subkey)
    return None


def _build_spec_from_graph(graph: dict[str, Any]) -> RunWorkflowSpec:
    """
    解析基于 React Flow `toObject()` 的最新图格式：
    - 节点字段主要在 data.* 内：type/params/inputValues/portConstants
    - 边字段使用 source/target/sourceHandle/targetHandle，kind 可在 data.kind
    - entryNodes/targets/overrides 若存在，保持原名
    """
    nodes = graph.get("nodes") or []
    edges = graph.get("edges") or []
    entry_nodes = graph.get("entryNodes") or graph.get("entry_nodes")
    targets = graph.get("targets")
    overrides = graph.get("overrides") or []

    def _node_type(n: dict[str, Any]) -> str | None:
        return _from_nested(n, "data", "type") or n.get("type")

    node_dtos = []
    for n in nodes:
        node_id = n.get("id")
        node_type = _node_type(n)
        if not node_id or not node_type:
            # fail-closed：非法节点不再静默跳过，避免图在 worker 端被悄悄
            # 改变语义（例如节点缺失导致下游收到不完整输入）。
            raise ValueError(f"workflow graph contains a node without id/type: {n!r}")
        node_dtos.append(
            WorkflowNodeDTO(
                id=node_id,
                type=node_type,
                params=_from_nested(n, "data", "params") or n.get("params") or {},
                port_constants=_first_non_null(
                    _from_nested(n, "data", "inputValues"),
                    _from_nested(n, "data", "portConstants"),
                    n.get("portConstants"),
                    n.get("port_constants"),
                ),
            )
        )

    edge_dtos = []
    for e in edges:
        edge_id = e.get("id")
        src = _first_non_null(e.get("source"), e.get("sourceNodeId"), e.get("source_node_id"))
        tgt = _first_non_null(e.get("target"), e.get("targetNodeId"), e.get("target_node_id"))
        if not edge_id or not src or not tgt:
            # fail-closed：非法边不再静默跳过，避免图在 worker 端被悄悄改语义。
            raise ValueError(f"workflow graph contains an edge without id/source/target: {e!r}")
        edge_dtos.append(
            WorkflowEdgeDTO(
                id=edge_id,
                source_node_id=src,
                source_port_id=_first_non_null(e.get("sourceHandle"), e.get("sourcePortId"), e.get("source_port_id")),
                target_node_id=tgt,
                target_port_id=_first_non_null(e.get("targetHandle"), e.get("targetPortId"), e.get("target_port_id")),
                kind=_first_non_null(_from_nested(e, "data", "kind"), e.get("kind"), "data"),
            )
        )

    override_dtos = [
        OverrideDTO(
            node_id=_first_non_null(o.get("nodeId"), o.get("node_id")),
            port_id=_first_non_null(o.get("portId"), o.get("port_id")),
            value=o.get("value"),
        )
        for o in overrides
        if _first_non_null(o.get("nodeId"), o.get("node_id"))
    ]

    return RunWorkflowSpec(
        nodes=node_dtos,
        edges=edge_dtos,
        entry_nodes=entry_nodes,
        targets=targets,
        overrides=override_dtos,
    )


def _short_hostname(max_len: int = 12) -> str:
    return socket.gethostname().split(".")[0][:max_len]


def generate_worker_id() -> str:
    """
    生成稳定且可读的 workerId：
    f-wk-{host}-{pid}-{rand6}
    """
    short_uuid = uuid.uuid4().hex[:6]
    return f"f-wk-{_short_hostname()}-{os.getpid()}-{short_uuid}".lower()


def generate_worker_name(queue: str, worker_id: str) -> str:
    """
    生成展示用名称：flo-wk-{host}-{queue}-{idTail}
    queue 中的 '.' 替换为 '-'，末尾使用 workerId 最后一段避免过长。
    """
    queue_sanitized = queue.replace(".", "-")
    short_id = worker_id.rsplit("-", 1)[-1]
    return f"flo-wk-{_short_hostname()}-{queue_sanitized}-{short_id}".lower()


@dataclass
class ParsedRunTask:
    execution_id: str
    attempt_id: str | None
    spec: RunWorkflowSpec
    workflow_meta: dict[str, Any]
    secrets: dict[str, str]  # New field
    extra_context: dict[str, Any]  # 存放上游传来的通用 context


class MQEventEmitter:
    """将 workflow 事件写回 RabbitMQ 队列，附带 sequenceId。"""

    def __init__(
        self,
        channel: aio_pika.abc.AbstractChannel,
        result_queue: str,
        execution_id: str,
        attempt_id: str | None = None,
        worker_id: str | None = None,
        secret_values: dict[str, str] | None = None,
    ) -> None:
        self._channel = channel
        self._result_queue = result_queue
        self._execution_id = execution_id
        self._attempt_id = attempt_id
        self._worker_id = worker_id
        self._secret_values = secret_values
        self._sequence = 0
        self._publish_lock = asyncio.Lock()

    async def emit(self, event: WorkflowEvent) -> None:
        # The execution heartbeat runs in a second coroutine. Keep sequence IDs
        # and Rabbit publish order serialized, otherwise an async heartbeat can
        # overtake workflow_completed and confuse replay consumers.
        async with self._publish_lock:
            data = sanitize_event_data(event.data, self._secret_values)
            payload = {
                "executionId": self._execution_id,
                "sequenceId": self._sequence,
                "event": event.event,
                "data": data,
                "timestamp": datetime.now(UTC).isoformat(),
            }
            if self._attempt_id:
                payload["attemptId"] = self._attempt_id
                payload["workerId"] = self._worker_id
            self._sequence += 1

            body = serialize_event_payload(payload)
            message = Message(
                body=body,
                delivery_mode=DeliveryMode.PERSISTENT,
                content_type="application/json",
                content_encoding="utf-8",
            )
            try:
                await self._channel.default_exchange.publish(message, routing_key=self._result_queue)
            except Exception:
                # 终态事件发布失败会让调用方 NACK 重投，这里按事件类型记账。
                EVENTS_PUBLISHED_TOTAL.labels(event.event, "error").inc()
                raise
            EVENTS_PUBLISHED_TOTAL.labels(event.event, "published").inc()


class ExecutionCancelled(Exception):
    """由控制面写入的 Redis 取消标记触发的协作式终止。"""


class WorkflowMQWorker:
    """
    从 RabbitMQ 队列消费 WorkflowRunTask，执行 run_workflow，并将事件按序推送到结果队列。
    - 输入队列：默认 workflow.run
    - 输出队列：默认 workflow.run_result
    - 手动 ACK；业务/解析错误也会 ACK
    """

    def __init__(self) -> None:
        self._url = os.getenv("RABBITMQ_URL", "amqp://user:password@localhost/")
        self._queue_in = os.getenv("RABBITMQ_QUEUE_WORKFLOW", "workflow.run")
        self._queue_out = os.getenv("RABBITMQ_QUEUE_RESULT", "workflow.run_result")
        self._prefetch = _env_int("WORKFLOW_PREFETCH", 4)
        self._max_concurrency = _env_int("WORKFLOW_MAX_CONCURRENCY", 4)
        self._connection: aio_pika.RobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None
        self._consume_tag: str | None = None
        self._semaphore = asyncio.Semaphore(self._max_concurrency)
        # 容量是配置事实，与 broker 连接状态无关：连不上时也应能看出进程配置。
        CAPACITY.set(self._max_concurrency)
        self._closing = asyncio.Event()
        self._worker_id = generate_worker_id()
        self._worker_name = generate_worker_name(self._queue_in, self._worker_id)
        # Redis / 心跳配置
        self._redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self._redis_prefix = os.getenv("WORKER_REDIS_PREFIX", "worker")
        self._heartbeat_interval = float(os.getenv("WORKER_HEARTBEAT_SECONDS", "10"))
        self._heartbeat_ttl = int(os.getenv("WORKER_HEARTBEAT_TTL_SECONDS", "30"))
        self._execution_heartbeat_interval = _positive_float_env("WORKER_EXECUTION_HEARTBEAT_SECONDS", 3.0)
        self._redis: aioredis.Redis | None = None
        self._heartbeat_task: asyncio.Task | None = None
        # 运行统计
        self._started_at = datetime.now(UTC).isoformat()
        self._last_execution_at: str | None = None
        self._success = 0
        self._failed = 0
        self._errors = 0
        self._inflight = 0
        self._avg_duration_ms: float | None = None
        self._ewma_alpha = float(os.getenv("WORKER_EWMA_ALPHA", "0.2"))

    async def start(self) -> None:
        if self._connection:
            return
        logger.info(
            "Connecting to RabbitMQ: queue=%s",
            self._queue_in,
            extra={"worker_id": self._worker_id, "worker_name": self._worker_name},
        )

        # Redis 连接（可选，失败不阻塞 MQ 运行）
        await self._init_redis()

        self._connection = await aio_pika.connect_robust(self._url)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=self._prefetch)

        # 声明队列（与已有配置保持一致，使用持久化）
        queue_in = await self._channel.declare_queue(self._queue_in, durable=True)
        await self._channel.declare_queue(self._queue_out, durable=True)

        # 开始消费
        self._consume_tag = await queue_in.consume(self._on_message, no_ack=False)
        logger.info(
            "WorkflowMQWorker started, consuming '%s', publishing to '%s', prefetch=%d, concurrency=%d",
            self._queue_in,
            self._queue_out,
            self._prefetch,
            self._max_concurrency,
            extra={"worker_id": self._worker_id, "worker_name": self._worker_name},
        )

        # 启动心跳
        if self._redis:
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self) -> None:
        self._closing.set()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            with contextlib.suppress(Exception):
                await self._heartbeat_task
        if self._channel and self._consume_tag:
            try:
                await self._channel.cancel(self._consume_tag)
            except Exception:
                pass
        if self._connection:
            try:
                await self._connection.close()
            except Exception:
                pass
        if self._redis:
            try:
                await self._clear_worker_state()
                await self._redis.close()
            except Exception:
                pass
        self._connection = None
        self._channel = None
        logger.info("WorkflowMQWorker stopped")

    async def _on_message(self, message: IncomingMessage) -> None:
        # 将耗时处理放入并发控制
        async with self._semaphore:
            await self._handle_message(message)

    async def _handle_message(self, message: IncomingMessage) -> None:
        try:
            parsed = self._parse_task(message)
        except Exception as exc:
            self._errors += 1
            execution_id = self._safe_execution_id(message)
            attempt_id = self._safe_attempt_id(message)
            logger.exception(
                "Failed to parse workflow task: %s",
                exc,
                extra={"execution_id": execution_id, "attempt_id": attempt_id},
            )
            await self._publish_error(execution_id, str(exc), attempt_id)
            await message.ack()
            return

        with log_context(
            execution_id=parsed.execution_id,
            attempt_id=parsed.attempt_id,
            worker_id=self._worker_id,
        ), secret_scope(parsed.secrets.values()):
            await self._run_task(message, parsed)

    async def _run_task(self, message: IncomingMessage, parsed: ParsedRunTask) -> None:
        """
        执行一条已解析的任务。

        运行上下文字段（execution_id/attempt_id/worker_id）与本次运行的 secret 字面值
        已由调用方登记，这里只负责编排事件发布、指标与 ACK/NACK 决策。
        """
        base_emitter = MQEventEmitter(
            self._channel,
            self._queue_out,
            parsed.execution_id,
            parsed.attempt_id,
            self._worker_id,
            parsed.secrets,
        )

        if await self._is_cancellation_requested(parsed.execution_id):
            # The Go control plane sets this key after the execution enters
            # CANCEL_REQUESTED. A message may already be in this consumer's
            # prefetch buffer, so RabbitMQ alone cannot make this race safe.
            EXECUTIONS_TOTAL.labels("cancelled_before_start").inc()
            await self._publish_cancelled(base_emitter, parsed.execution_id)
            await message.ack()
            return

        execution_heartbeat_stop = asyncio.Event()
        execution_heartbeat_task: asyncio.Task | None = None
        terminal_delivered = False

        async def stop_execution_heartbeat() -> None:
            execution_heartbeat_stop.set()
            if execution_heartbeat_task:
                execution_heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await execution_heartbeat_task

        async def counting_emit(event: WorkflowEvent) -> None:
            """拦截 workflow_completed 统计成功/失败后再转发。"""
            nonlocal execution_heartbeat_task, terminal_delivered
            # `run_workflow` emits between node transitions. This makes running
            # cancellation cooperative: a long single-node operation finishes
            # its current await before the worker can observe the marker.
            if await self._is_cancellation_requested(parsed.execution_id):
                raise ExecutionCancelled()
            if event.event == "workflow_completed":
                status = (event.data or {}).get("status")
                if status == "success":
                    self._success += 1
                    EXECUTIONS_TOTAL.labels("success").inc()
                elif status == "error":
                    self._failed += 1
                    EXECUTIONS_TOTAL.labels("error").inc()
                # Stop before publishing the terminal message. The emitter lock
                # still preserves any already-started heartbeat ahead of it.
                execution_heartbeat_stop.set()
            await base_emitter.emit(event)
            if event.event == "workflow_completed":
                # 终态事件成功发布后才允许 ack 输入任务。
                terminal_delivered = True
            if event.event == "workflow_started" and parsed.attempt_id and execution_heartbeat_task is None:
                execution_heartbeat_task = asyncio.create_task(
                    self._execution_heartbeat_loop(base_emitter, execution_heartbeat_stop, parsed.execution_id)
                )

        logger.info(
            "Run begin",
            extra={
                "nodes": len(parsed.spec.nodes),
                "edges": len(parsed.spec.edges),
                "entry_nodes": parsed.spec.entry_nodes,
                "targets": parsed.spec.targets,
            },
        )

        self._inflight += 1
        INFLIGHT.set(self._inflight)
        start_ts = monotonic()

        try:
            # 构造引擎运行时上下文
            engine_context = {
                "workflow": parsed.workflow_meta,
                "secrets": parsed.secrets,
            }
            # 合并上游传入的 context 字段
            engine_context.update(parsed.extra_context)

            # 【兼容性映射】：如果上游提供了 http，映射到引擎内部预期的 request 键
            # 这样 HttpStartNode 和引擎的 entryNode 过滤逻辑都能正常工作
            if "http" in parsed.extra_context and "request" not in engine_context:
                engine_context["request"] = parsed.extra_context["http"]

            await run_workflow(
                run_id=parsed.execution_id,
                spec=parsed.spec,
                emit=counting_emit,
                context=engine_context,
            )
            logger.info("Run finished")
        except ExecutionCancelled:
            EXECUTIONS_TOTAL.labels("cancelled").inc()
            logger.info("Workflow execution canceled")
            await stop_execution_heartbeat()
            await self._publish_cancelled(base_emitter, parsed.execution_id)
            terminal_delivered = True
        except Exception as exc:
            self._errors += 1
            EXECUTIONS_TOTAL.labels("error").inc()
            logger.exception("Workflow execution failed")
            await stop_execution_heartbeat()
            await base_emitter.emit(
                WorkflowEvent(
                    event="workflow_completed",
                    data={
                        "runId": parsed.execution_id,
                        "status": "error",
                        "errorMessage": str(exc),
                    },
                )
            )
            terminal_delivered = True
        finally:
            await stop_execution_heartbeat()
            duration_ms = (monotonic() - start_ts) * 1000
            self._update_ewma(duration_ms)
            EXECUTION_DURATION_SECONDS.observe(duration_ms / 1000.0)
            self._last_execution_at = datetime.now(UTC).isoformat()
            self._inflight = max(0, self._inflight - 1)
            INFLIGHT.set(self._inflight)
            logger.info("Run end", extra={"duration_ms": round(duration_ms, 2)})
            await self._publish_heartbeat(force=True)
            # 终态事件发布失败（broker 不可用、事件超限等）时 NACK 重投，
            # 让 broker 重新投递任务；成功才 ack。这样执行结果不会因为
            # 发布失败而永久丢失。
            if terminal_delivered:
                await message.ack()
            else:
                logger.warning(
                    "Terminal event delivery failed for executionId=%s; nacking for redelivery",
                    parsed.execution_id,
                )
                await message.nack(requeue=True)

    def _parse_task(self, message: IncomingMessage) -> ParsedRunTask:
        if not message.body:
            raise ValueError("empty message body")
        raw = message.body.decode("utf-8")
        payload = json.loads(raw)

        execution_id = payload.get("executionId")
        if not execution_id:
            raise ValueError("executionId is required")

        attempt_id = payload.get("attemptId")
        if attempt_id is not None:
            try:
                attempt_id = str(uuid.UUID(str(attempt_id)))
            except (TypeError, ValueError) as exc:
                raise ValueError("attemptId must be a UUID") from exc

        workflow = payload.get("workflow") or {}
        workflow_meta: dict[str, Any] = {
            "id": workflow.get("id"),
            "workspaceId": (workflow.get("workspace") or {}).get("id") if isinstance(workflow.get("workspace"), dict) else None,
            "name": workflow.get("name"),
            "description": workflow.get("description"),
        }

        # graph 位于消息顶层
        graph = payload.get("graph") or {}
        spec = _build_spec_from_graph(graph)
        
        # Extract secrets map (injected by Java)
        secrets = payload.get("secrets") or {}
        if not isinstance(secrets, dict):
            secrets = {}

        # 提取上游传入的 context
        extra_context = payload.get("context")
        if not isinstance(extra_context, dict):
            extra_context = {}

        return ParsedRunTask(
            execution_id=str(execution_id),
            attempt_id=attempt_id,
            spec=spec,
            workflow_meta=workflow_meta,
            secrets=secrets,
            extra_context=extra_context,
        )

    def _safe_execution_id(self, message: IncomingMessage) -> str:
        try:
            raw = json.loads(message.body.decode("utf-8"))
            return str(raw.get("executionId")) if raw.get("executionId") else "unknown"
        except Exception:
            return "unknown"

    def _safe_attempt_id(self, message: IncomingMessage) -> str | None:
        try:
            raw = json.loads(message.body.decode("utf-8"))
            value = raw.get("attemptId")
            return str(uuid.UUID(str(value))) if value else None
        except Exception:
            return None

    async def _publish_error(self, execution_id: str, error_message: str, attempt_id: str | None = None) -> None:
        if not self._channel:
            return
        emitter = MQEventEmitter(self._channel, self._queue_out, execution_id, attempt_id, self._worker_id)
        await emitter.emit(
            WorkflowEvent(
                event="workflow_completed",
                data={
                    "runId": execution_id,
                    "status": "error",
                    "errorMessage": error_message,
                },
            )
        )

    async def _publish_cancelled(self, emitter: MQEventEmitter, execution_id: str) -> None:
        await emitter.emit(
            WorkflowEvent(
                event="workflow_completed",
                data={
                    "runId": execution_id,
                    "status": "cancelled",
                },
            )
        )

    async def _execution_heartbeat_loop(
        self,
        emitter: MQEventEmitter,
        stop_event: asyncio.Event,
        execution_id: str,
    ) -> None:
        """续租一个已开始的 attempt；旧任务没有 attemptId 时不会启动该协程。"""
        try:
            while not stop_event.is_set():
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self._execution_heartbeat_interval)
                    continue
                except TimeoutError:
                    pass
                if stop_event.is_set():
                    continue
                await emitter.emit(
                    WorkflowEvent(
                        event="workflow_heartbeat",
                        data={"runId": execution_id},
                    )
                )
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning("Execution heartbeat failed for %s: %s", execution_id, exc)

    def _cancellation_key(self, execution_id: str) -> str:
        # Keep this key byte-for-byte aligned with gback/internal/execution.
        return f"execution:{execution_id}:cancel-requested"

    async def _is_cancellation_requested(self, execution_id: str) -> bool:
        if not self._redis:
            return False
        try:
            return bool(await self._redis.get(self._cancellation_key(execution_id)))
        except Exception as exc:
            # Redis outages must not turn healthy workflow work into failures.
            # The control plane reports cancellation unavailable before it marks
            # an execution CANCEL_REQUESTED, so this is only a degraded worker.
            logger.warning("Unable to check cancellation marker for %s: %s", execution_id, exc)
            return False

    def _update_ewma(self, duration_ms: float) -> None:
        if duration_ms < 0:
            return
        if self._avg_duration_ms is None:
            self._avg_duration_ms = duration_ms
        else:
            alpha = self._ewma_alpha
            self._avg_duration_ms = alpha * duration_ms + (1 - alpha) * self._avg_duration_ms

    def _redis_key(self, suffix: str) -> str:
        return f"{self._redis_prefix}:{suffix}"

    async def _init_redis(self) -> None:
        try:
            self._redis = aioredis.from_url(self._redis_url)
            # 测试连接
            await self._redis.ping()
            await self._publish_heartbeat(force=True, init=True)
        except Exception as exc:
            self._redis = None
            logger.warning("Redis not available, heartbeat disabled: %s", exc)

    async def _heartbeat_loop(self) -> None:
        assert self._redis is not None
        try:
            while not self._closing.is_set():
                await self._publish_heartbeat()
                try:
                    await asyncio.wait_for(self._closing.wait(), timeout=self._heartbeat_interval)
                except TimeoutError:
                    # 正常心跳间隔超时，继续循环
                    continue
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning("Heartbeat loop error: %s", exc)

    async def _publish_heartbeat(self, force: bool = False, init: bool = False) -> None:
        if not self._redis:
            return
        now_iso = datetime.now(UTC).isoformat()
        key = self._redis_key(f"worker:{self._worker_id}")
        online_key = self._redis_key("online")
        payload = {
            "id": self._worker_id,
            "name": self._worker_name,
            "host": _short_hostname(),
            "queue": self._queue_in,
            "startedAt": self._started_at,
            "lastHeartbeat": now_iso,
            "capacity": self._max_concurrency,
            "inflight": self._inflight,
            "success": self._success,
            "failed": self._failed,
            "errors": self._errors,
            "avgDurationMs": f"{self._avg_duration_ms:.2f}" if self._avg_duration_ms is not None else None,
            "lastExecutionAt": self._last_execution_at,
        }
        try:
            pipe = self._redis.pipeline()
            pipe.hset(key, mapping={k: v for k, v in payload.items() if v is not None})
            pipe.expire(key, self._heartbeat_ttl)
            pipe.sadd(online_key, self._worker_id)
            pipe.expire(online_key, self._heartbeat_ttl)
            await pipe.execute()
        except Exception as exc:
            if force or init:
                logger.warning("Publish heartbeat failed: %s", exc)

    async def _clear_worker_state(self) -> None:
        if not self._redis:
            return
        key = self._redis_key(f"worker:{self._worker_id}")
        online_key = self._redis_key("online")
        try:
            pipe = self._redis.pipeline()
            pipe.delete(key)
            pipe.srem(online_key, self._worker_id)
            await pipe.execute()
        except Exception:
            pass


__all__ = ["WorkflowMQWorker"]
