import asyncio
import contextlib
import logging
import os
import signal
from pathlib import Path

from logging_setup import setup_logging
from metrics import start_metrics_server_from_env
from mq_worker import WorkflowMQWorker


def worker_reconnect_delay() -> float:
    """Return a bounded retry delay without letting a bad env value kill the worker."""
    try:
        return max(float(os.getenv("WORKER_RECONNECT_DELAY", "2")), 0.1)
    except ValueError:
        return 2.0


async def start_worker_or_stop(worker: WorkflowMQWorker, stop_event: asyncio.Event) -> bool:
    """Start one worker instance unless process shutdown wins the race."""
    start_task = asyncio.create_task(worker.start())
    stop_task = asyncio.create_task(stop_event.wait())
    done, _ = await asyncio.wait({start_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
    if start_task in done:
        stop_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await stop_task
        await start_task
        return True

    # A TCP/DNS failure can leave a connection coroutine waiting longer than a
    # container shutdown budget. Cancel it before worker.stop() closes partial
    # resources, so SIGTERM is prompt even during the first broker connection.
    start_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await start_task
    return False


def load_env_from_dotenv() -> None:
    """
    从当前目录的 .env 文件加载环境变量：
    - 忽略不存在的文件
    - 忽略注释行和空行
    - 不覆盖已有的环境变量
    """
    base_dir = Path(__file__).resolve().parent
    env_path = base_dir / ".env"
    if not env_path.is_file():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        if key in os.environ:
            continue
        value = value.strip().strip('"').strip("'")
        os.environ[key] = value


async def _run_worker() -> None:
    load_env_from_dotenv()
    setup_logging()
    metrics_enabled = start_metrics_server_from_env()
    logger = logging.getLogger(__name__)

    stop_event = asyncio.Event()
    reconnect_delay = worker_reconnect_delay()

    def _signal_handler(sig: signal.Signals) -> None:
        logger.info("Received signal %s, stopping worker...", sig.name)
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, _signal_handler, sig)
        except NotImplementedError:
            # Windows/limited environments可能不支持 add_signal_handler
            pass

    logger.info(
        "Worker service starting",
        extra={"metrics_enabled": metrics_enabled, "reconnect_delay_seconds": reconnect_delay},
    )
    while not stop_event.is_set():
        # WorkflowMQWorker.stop() marks its instance as closing. Recreate it for
        # every failed initial connection so a RabbitMQ restart cannot leave the
        # process alive with a permanently closed channel or heartbeat task.
        worker = WorkflowMQWorker()
        try:
            if not await start_worker_or_stop(worker, stop_event):
                break
            logger.info("Worker started; waiting for messages")
            await stop_event.wait()
        except Exception as exc:
            if stop_event.is_set():
                break
            logger.warning(
                "Worker connection or consumer setup failed; retrying in %.1fs: %s",
                reconnect_delay,
                exc,
            )
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=reconnect_delay)
            except TimeoutError:
                pass
        finally:
            await worker.stop()

    logger.info("Worker service stopped")


def main() -> None:
    try:
        asyncio.run(_run_worker())
    except KeyboardInterrupt:
        # 已在信号里处理，这里确保退出码干净
        pass


if __name__ == "__main__":
    main()
