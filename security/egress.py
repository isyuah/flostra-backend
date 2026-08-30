"""Worker 出站网络策略（egress）。

默认拒绝私网 / 回环 / 链路本地 / 云 metadata 地址，防止工作流节点把
Worker 当作跳板访问内部服务。所有执行外部出站 I/O 的节点都应通过
``check_outbound_url`` / ``check_outbound_host`` 校验目标，而不是各自实现。

策略可通过环境变量调整（默认全拒绝）：

- ``WORKER_EGRESS_ALLOW_PRIVATE``: "0"/"false"/"off" 之外的值表示允许私网（不推荐）。
- ``WORKER_EGRESS_ALLOW_METADATA``: 同上，允许云 metadata 地址（不推荐）。

实现要点：

- 同时校验域名与解析后的每个 IP，防止 DNS rebinding。
- 禁止自动跳转时的目标地址校验由调用方在重定向后重新调用本模块完成。
- 域名本身为 IP 字面量时直接校验 IP。
"""

from __future__ import annotations

import ipaddress
import os
import socket
from urllib.parse import urlparse

# 云厂商 metadata 端点（常见固定 IP 段）。
_METADATA_NETS = (
    ipaddress.ip_network("169.254.169.254/32"),
    ipaddress.ip_network("fd00:ec2::254/128"),
)

# 默认拒绝的特殊用途地址（RFC 5735 / 6890 摘要）。
_PRIVATE_NETS = (
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("100.64.0.0/10"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.0.0.0/24"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("198.18.0.0/15"),
    ipaddress.ip_network("224.0.0.0/4"),
    ipaddress.ip_network("240.0.0.0/4"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
    ipaddress.ip_network("ff00::/8"),
)

# 链接本地组播（RFC 4291）以及 IPv4-mapped IPv6 也需要拦截。
_EXTRA_NETS = (
    ipaddress.ip_network("::/128"),
    ipaddress.ip_network("::ffff:0:0/96"),
    ipaddress.ip_network("2001:db8::/32"),
)


class EgressError(ValueError):
    """目标地址被 egress 策略拒绝。"""


def _truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() not in {"", "0", "false", "no", "off"}


def _allow_private() -> bool:
    return _truthy(os.getenv("WORKER_EGRESS_ALLOW_PRIVATE"))


def _allow_metadata() -> bool:
    return _truthy(os.getenv("WORKER_EGRESS_ALLOW_METADATA"))


def _classify(ip: ipaddress._BaseAddress) -> tuple[bool, str]:
    """返回 (是否允许, 拒绝原因)。"""
    if not _allow_private():
        for net in _PRIVATE_NETS + _EXTRA_NETS:
            if ip in net:
                return False, f"private/unsafe network {net}"
    if not _allow_metadata():
        for net in _METADATA_NETS:
            if ip in net:
                return False, f"cloud metadata address {net}"
    return True, ""


def _parse_host(host: str) -> ipaddress._BaseAddress | None:
    """host 若是 IP 字面量则返回 ip 对象，否则返回 None。"""
    try:
        return ipaddress.ip_address(host.strip("[]"))
    except ValueError:
        return None


def check_outbound_host(host: str, port: int | None = None) -> None:
    """校验单个出站主机（域名或 IP）。

    域名会解析为所有 A/AAAA 记录并逐一校验；解析失败视为拒绝，
    避免“解析不到就放行”的 fail-open。
    """
    if not host or not host.strip():
        raise EgressError("empty outbound host is not allowed")

    host = host.strip().rstrip(".").lower()
    literal = _parse_host(host)
    if literal is not None:
        allowed, reason = _classify(literal)
        if not allowed:
            raise EgressError(f"outbound host {host!r} rejected: {reason}")
        return

    try:
        infos = socket.getaddrinfo(host, port or 0, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise EgressError(f"cannot resolve outbound host {host!r}: {exc}") from exc

    seen: set[str] = set()
    for info in infos:
        ip = ipaddress.ip_address(info[4][0])
        key = str(ip)
        if key in seen:
            continue
        seen.add(key)
        allowed, reason = _classify(ip)
        if not allowed:
            raise EgressError(f"outbound host {host!r} resolves to {key}: {reason}")


def check_outbound_url(url: str) -> None:
    """校验 URL 目标。

    - scheme 仅允许 http/https；
    - host 非空；
    - hostname 为域名或 IP 字面量，经 ``check_outbound_host`` 校验。
    """
    if not isinstance(url, str) or not url.strip():
        raise EgressError("empty outbound URL is not allowed")

    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise EgressError(f"outbound URL scheme {parsed.scheme!r} is not allowed")

    host = parsed.hostname
    if not host:
        raise EgressError("outbound URL must include a host")

    try:
        port = parsed.port
    except ValueError as exc:
        raise EgressError(f"invalid outbound URL port: {exc}") from exc

    check_outbound_host(host, port)
