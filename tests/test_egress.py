"""出站网络策略测试。"""


import pytest

from security.egress import EgressError, check_outbound_host, check_outbound_url


def test_rejects_private_ipv4():
    for ip in ("127.0.0.1", "10.0.0.5", "192.168.1.1", "172.16.0.1", "169.254.169.254"):
        with pytest.raises(EgressError):
            check_outbound_host(ip, 80)


def test_rejects_private_ipv6():
    for ip in ("::1", "fc00::1", "fe80::1"):
        with pytest.raises(EgressError):
            check_outbound_host(ip, 80)


def test_rejects_metadata_ip():
    with pytest.raises(EgressError):
        check_outbound_url("http://169.254.169.254/latest/meta-data/")


def test_rejects_domain_resolving_to_private():
    # 使用本地回环作为解析目标：localhost 解析到 127.0.0.1/::1，应被拒绝。
    with pytest.raises(EgressError):
        check_outbound_host("localhost", 80)


def test_rejects_unsafe_scheme():
    with pytest.raises(EgressError):
        check_outbound_url("ftp://example.com/file")


def test_accepts_public_ip():
    # 明确公共 IP 应当放行。
    check_outbound_host("8.8.8.8", 443)
    check_outbound_url("https://8.8.8.8/")
