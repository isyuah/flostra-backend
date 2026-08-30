from __future__ import annotations

import asyncio
import json
import smtplib
import ssl
from email.message import EmailMessage
from email.utils import make_msgid

from io_utils import FileIoService
from runtime_files import is_file_ref

from .base import JsonDict, WorkflowNode, register_node


@register_node
class EmailSendNode(WorkflowNode):
    """
    邮件发送节点（仅 SMTP/SMTPS）：
    - 参数配置服务器、账户、TLS 方式
    - 输入端口提供收件人、标题、正文
    - 支持附件
    """

    type = "email-send"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "邮件发送",
            "description": "通过 SMTP/SMTPS 发送邮件，支持 STARTTLS 和附件",
            "category": "IO",
            "icon": "mail",
            "inputs": [
                {"name": "to", "label": "收件人", "type": ["string", "array"], "required": True},
                {"name": "subject", "label": "主题", "type": "string", "required": False},
                {"name": "body", "label": "正文", "type": ["string", "object"], "required": False},
                {"name": "attachments", "label": "附件", "type": ["object", "array"], "required": False},
            ],
            "outputs": [
                {"name": "ok", "label": "是否成功", "type": "boolean", "required": True},
                {"name": "message_id", "label": "Message-ID", "type": "string", "required": False},
                {"name": "log", "label": "日志", "type": "string", "required": False},
            ],
            "parameters": [
                {"name": "server", "label": "SMTP 服务器", "type": "string", "widget": "input", "required": True},
                {"name": "port", "label": "端口", "type": "number", "widget": "input", "required": False, "default": 587},
                {"name": "username", "label": "账号", "type": "string", "widget": "input", "required": False},
                {"name": "password", "label": "密码/授权码", "type": "string", "widget": "secret", "required": False},
                {"name": "from", "label": "发件人", "type": "string", "widget": "input", "required": False},
                {"name": "use_ssl", "label": "使用 SMTPS (465)", "type": "boolean", "widget": "switch", "default": False},
                {"name": "use_starttls", "label": "启用 STARTTLS", "type": "boolean", "widget": "switch", "default": True},
                {"name": "timeout", "label": "超时（秒）", "type": "number", "widget": "input", "required": False},
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict, context: JsonDict = None) -> JsonDict:
        server = params.get("server")
        if not server:
            raise ValueError("邮件发送节点缺少 server 参数")

        raw_to = inputs.get("to")
        recipients = cls._normalize_recipients(raw_to)
        if not recipients:
            raise ValueError("邮件发送节点缺少收件人 to")
        
        if not context:
            raise ValueError("EmailSend节点需要运行时上下文")

        subject = inputs.get("subject") or ""
        body = inputs.get("body")
        body_text = body if isinstance(body, str) else json.dumps(body, ensure_ascii=False) if body is not None else ""
        
        # 预加载附件数据 (Async Step)
        attachments_data: list[tuple[bytes, str, str]] = [] # (content, filename, mime)
        raw_attachments = inputs.get("attachments")
        if raw_attachments:
            list_attachments = []
            if isinstance(raw_attachments, list):
                list_attachments = raw_attachments
            elif isinstance(raw_attachments, dict):
                list_attachments = [raw_attachments]
            
            for att in list_attachments:
                if is_file_ref(att):
                    # Await IO here before going into sync executor
                    content, name, mime = await FileIoService.get_file_bytes(context, att)
                    attachments_data.append((content, name, mime or "application/octet-stream"))

        port = int(params.get("port") or 587)
        use_ssl = bool(params.get("use_ssl", False))
        use_starttls = bool(params.get("use_starttls", True))
        timeout_val = params.get("timeout")
        try:
            timeout = float(timeout_val) if timeout_val is not None else 10.0
        except (TypeError, ValueError):
            timeout = 10.0

        username = params.get("username") or None
        password = params.get("password") or None
        from_addr = params.get("from") or username or ""

        if use_ssl and use_starttls:
            use_starttls = False

        msg = EmailMessage()
        msg["Subject"] = str(subject)
        msg["From"] = from_addr
        msg["To"] = ", ".join(recipients)
        msg["Message-ID"] = make_msgid()
        msg.set_content(str(body_text))

        # Attach files
        for content, filename, mime in attachments_data:
            # Simple heuristic for maintype/subtype
            maintype, subtype = mime.split("/", 1) if "/" in mime else ("application", "octet-stream")
            msg.add_attachment(content, maintype=maintype, subtype=subtype, filename=filename)

        loop = asyncio.get_event_loop()

        def _send_email() -> str:
            def _safe_close(smtp: smtplib.SMTP) -> None:
                try:
                    smtp.quit()
                except Exception:
                    try:
                        smtp.close()
                    except Exception:
                        pass

            try:
                if use_ssl:
                    context_ssl = ssl.create_default_context()
                    smtp = smtplib.SMTP_SSL(server, port, timeout=timeout, context=context_ssl)
                    try:
                        cls._maybe_login(smtp, username, password)
                        smtp.send_message(msg)
                    finally:
                        _safe_close(smtp)
                else:
                    smtp = smtplib.SMTP(server, port, timeout=timeout)
                    try:
                        smtp.ehlo()
                        if use_starttls:
                            context_ssl = ssl.create_default_context()
                            smtp.starttls(context=context_ssl)
                            smtp.ehlo()
                        cls._maybe_login(smtp, username, password)
                        smtp.send_message(msg)
                    finally:
                        _safe_close(smtp)
                return msg["Message-ID"] or ""
            except Exception as exc:
                raise ValueError(f"邮件发送失败：{exc}") from None

        message_id = await loop.run_in_executor(None, _send_email)

        return {"ok": True, "message_id": message_id, "log": f"sent to {len(recipients)} recipient(s)"}