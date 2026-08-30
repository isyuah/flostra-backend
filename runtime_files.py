from __future__ import annotations

import base64
import hashlib
import uuid
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import httpx

from security.egress import check_outbound_url

JsonDict = Dict[str, Any]


def new_file_id() -> str:
    return f"file_{uuid.uuid4().hex}"


def is_file_ref(obj: Any) -> bool:
    return isinstance(obj, dict) and obj.get("__kind") == "file" and bool(obj.get("id"))


def build_file_ref(
    *,
    file_id: str,
    name: Optional[str] = None,
    mime: Optional[str] = None,
    size: Optional[int] = None,
    sha256: Optional[str] = None,
    source: Optional[str] = None,
    content_base64: Optional[str] = None,
) -> JsonDict:
    ref: JsonDict = {"__kind": "file", "id": str(file_id)}
    if name is not None:
        ref["name"] = str(name)
    if mime is not None:
        ref["mime"] = str(mime)
    if size is not None:
        ref["size"] = int(size)
    if sha256 is not None:
        ref["sha256"] = str(sha256)
    if source is not None:
        ref["source"] = str(source)
    if content_base64 is not None:
        ref["content_base64"] = str(content_base64)
    return ref


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def to_base64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def from_base64(b64_str: str) -> bytes:
    try:
        return base64.b64decode(b64_str or "", validate=False)
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"非法 base64 数据：{exc}") from exc


def get_cache_map(ctx: Any, *, create: bool = True) -> Optional[JsonDict]:
    if not isinstance(ctx, dict):
        return None
    cache_map = ctx.get("cacheMap")
    if cache_map is None:
        if not create:
            return None
        cache_map = {}
        ctx["cacheMap"] = cache_map
    if not isinstance(cache_map, dict):
        if not create:
            return None
        cache_map = {}
        ctx["cacheMap"] = cache_map
    return cache_map


def get_files_map(ctx: Any, *, create: bool = True) -> Optional[JsonDict]:
    cache_map = get_cache_map(ctx, create=create)
    if cache_map is None:
        return None
    files = cache_map.get("files")
    if files is None:
        if not create:
            return None
        files = {}
        cache_map["files"] = files
    if not isinstance(files, dict):
        if not create:
            return None
        files = {}
        cache_map["files"] = files
    return files


def get_file_record(ctx: Any, file_id: str) -> Optional[JsonDict]:
    files = get_files_map(ctx, create=False)
    if not files:
        return None
    rec = files.get(str(file_id))
    return rec if isinstance(rec, dict) else None


def upsert_file_record(ctx: Any, file_id: str, **fields: Any) -> Optional[JsonDict]:
    files = get_files_map(ctx, create=True)
    if files is None:
        return None
    key = str(file_id)
    rec = files.get(key)
    if not isinstance(rec, dict):
        rec = {"id": key}
        files[key] = rec
    for k, v in fields.items():
        if v is None:
            continue
        rec[k] = v
    return rec


async def download_url_bytes(url: str, *, max_bytes: int = 8_000_000) -> bytes:
    if not isinstance(url, str) or not url:
        raise ValueError("url 不能为空")
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("仅支持 http/https URL")

    # 出站网络策略：下载目标必须通过校验。
    check_outbound_url(url)

    try:
        max_bytes_int = int(max_bytes)
        if max_bytes_int <= 0:
            max_bytes_int = 8_000_000
    except Exception:
        max_bytes_int = 8_000_000

    timeout = httpx.Timeout(20.0, connect=10.0)
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            async with client.stream("GET", url) as resp:
                # 重定向后的最终 URL 必须再次校验，防止跳入内网。
                if str(resp.url) != url:
                    check_outbound_url(str(resp.url))
                resp.raise_for_status()
                buf = bytearray()
                async for chunk in resp.aiter_bytes():
                    if not chunk:
                        continue
                    buf.extend(chunk)
                    if len(buf) > max_bytes_int:
                        raise ValueError(f"下载文件过大（>{max_bytes_int} bytes），已中止")
                return bytes(buf)
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        raise ValueError(f"下载失败：HTTP {status}") from exc
    except httpx.RequestError as exc:
        raise ValueError(f"下载失败：{exc}") from exc
