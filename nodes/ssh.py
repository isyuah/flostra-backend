from __future__ import annotations

import asyncio
import contextlib
import shlex
from typing import Any, Dict, Optional

import asyncssh

from .base import JsonDict, WorkflowNode, register_node
from security.egress import check_outbound_host


def _to_str_limit(text: str, limit: int = 1_000_000) -> str:
    """截断过长输出，避免内存暴涨。"""
    if text is None:
        return ""
    if len(text) > limit:
        return text[:limit] + f"...[truncated {len(text) - limit} chars]"
    return text


def _merge_command(command: Any) -> str:
    """
    接受字符串或字符串数组，不做语义处理，只做最小拼接：
    - 字符串：原样返回（包含换行也原样发给远端 shell）
    - 数组：用换行拼接，等价于多行命令，行为由远端 shell 决定
    """
    if isinstance(command, str):
        cmd = command.strip("\n")
        if not cmd:
            raise ValueError("SSH 节点缺少命令")
        return cmd
    if isinstance(command, list):
        parts: list[str] = []
        for item in command:
            if isinstance(item, str):
                parts.append(item)
            else:
                parts.append(str(item))
        cmd = "\n".join(parts).strip("\n")
        if not cmd:
            raise ValueError("SSH 节点缺少命令")
        return cmd
    raise ValueError("command 需为字符串或字符串数组")


def _parse_env(val: Any) -> Optional[Dict[str, str]]:
    if val is None:
        return None
    if not isinstance(val, dict):
        raise ValueError("env 必须是对象(map)")
    env_str: Dict[str, str] = {}
    for k, v in val.items():
        env_str[str(k)] = "" if v is None else str(v)
    return env_str


@register_node
class SSHNode(WorkflowNode):
    """
    SSH 远程执行节点：
    - 支持密码或私钥（明文）认证
    - 不对命令做额外魔法，前端传什么就远端执行什么
    - 返回 stdout/stderr/exitCode；可选 failOnNonZero 失败即终止
    """

    type = "ssh"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "SSH",
            "description": "通过 SSH 在远端执行命令，返回 stdout/stderr/exitCode",
            "category": "IO",
            "icon": "terminal",
            "inputs": [
                {"name": "host", "label": "主机", "type": "string", "required": True},
                {"name": "port", "label": "端口", "type": "number", "required": False},
                {"name": "username", "label": "用户名", "type": "string", "required": True},
                {"name": "password", "label": "密码", "type": "string", "required": False},
                {"name": "privateKey", "label": "私钥(PEM)", "type": "string", "required": False},
                {"name": "passphrase", "label": "私钥口令", "type": "string", "required": False},
                {
                    "name": "command",
                    "label": "命令",
                    "type": ["string", "array"],
                    "required": True,
                    "desc": "可输入多行文本或字符串数组，按原样发送远端 shell",
                },
                {"name": "env", "label": "环境变量", "type": "object", "required": False},
                {"name": "workdir", "label": "工作目录", "type": "string", "required": False},
            ],
            "outputs": [
                {"name": "stdout", "label": "标准输出", "type": "string", "required": False},
                {"name": "stderr", "label": "标准错误", "type": "string", "required": False},
                {"name": "exitCode", "label": "退出码", "type": "number", "required": True},
            ],
            "parameters": [
                {"name": "port", "label": "默认端口", "type": "number", "widget": "input", "default": 22},
                {"name": "timeout", "label": "默认超时(秒)", "type": "number", "widget": "input", "default": 30},
                {
                    "name": "failOnNonZero",
                    "label": "非零退出视为失败",
                    "type": "boolean",
                    "widget": "switch",
                    "default": True,
                },
                {
                    "name": "ignoreHostKey",
                    "label": "忽略 HostKey",
                    "type": "boolean",
                    "widget": "switch",
                    "default": False,
                    "desc": "仅开发环境使用，生产请校验主机指纹",
                },
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict, context: JsonDict = None) -> JsonDict:
        host = inputs.get("host") or params.get("host")
        username = inputs.get("username") or params.get("username")
        if not host or not username:
            raise ValueError("SSH 节点需要 host 与 username")

        def _int_or_default(val: Any, default: int) -> int:
            try:
                return int(val)
            except Exception:
                return default

        port = _int_or_default(inputs.get("port") or params.get("port"), 22)
        timeout = float(params.get("timeout") or 30)
        fail_on_nonzero = params.get("failOnNonZero", True)
        ignore_host_key = params.get("ignoreHostKey", False)
        fail_on_nonzero = bool(fail_on_nonzero)
        ignore_host_key = bool(ignore_host_key)

        # 出站网络策略：SSH 目标主机必须通过校验，防止连接内网主机。
        check_outbound_host(host, port)

        password = inputs.get("password") or params.get("password")
        private_key = inputs.get("privateKey") or params.get("privateKey")
        passphrase = inputs.get("passphrase") or params.get("passphrase")
        env = _parse_env(inputs.get("env") or params.get("env"))
        workdir = inputs.get("workdir") or params.get("workdir")

        if not password and not private_key:
            raise ValueError("SSH 节点需要 password 或 privateKey 至少一项")

        cmd = _merge_command(inputs.get("command"))
        if workdir:
            cmd = f"cd {shlex.quote(str(workdir))} && {cmd}"

        client_keys = None
        if private_key:
            try:
                key_obj = asyncssh.import_private_key(private_key, passphrase=passphrase)
                client_keys = [key_obj]
            except Exception as exc:  # noqa: BLE001
                raise ValueError(f"解析私钥失败：{exc}") from exc

        try:
            conn_kwargs: Dict[str, Any] = {
                "host": host,
                "port": port,
                "username": username,
                "password": password,
                "client_keys": client_keys,
            }
            if ignore_host_key:
                conn_kwargs["known_hosts"] = None

            conn = await asyncssh.connect(**conn_kwargs)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"SSH 连接失败：{exc}") from exc

        try:
            proc = await conn.create_process(command=cmd, env=env)
            try:
                stdout_data, stderr_data = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            except asyncio.TimeoutError:
                proc.close()
                try:
                    await proc.wait_closed()
                finally:
                    raise ValueError(f"SSH 命令超时（>{timeout} 秒）")
            exit_code = proc.exit_status
            stdout_text = _to_str_limit(stdout_data or "")
            stderr_text = _to_str_limit(stderr_data or "")

            if fail_on_nonzero and exit_code != 0:
                raise ValueError(f"SSH 命令退出码 {exit_code}，stderr: {stderr_text}")

            return {
                "stdout": stdout_text,
                "stderr": stderr_text,
                "exitCode": exit_code,
            }
        finally:
            conn.close()
            with contextlib.suppress(Exception):
                await conn.wait_closed()
