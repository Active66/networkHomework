"""UDP 局域网广播自动发现模块。

原理：
1. 服务端启动后在后台监听 UDP 端口（默认 9999）。
2. 客户端向局域网广播地址 255.255.255.255 发送探测报文。
3. 服务端收到探测报文后，直接向客户端源地址单播回复自己的 TCP 端口与服务名称。
"""

from __future__ import annotations

import json
import socket
import threading
from typing import Optional, Tuple

DISCOVERY_PORT = 9999
DISCOVERY_MAGIC = b"DISCOVER_LAN_CHAT_V1"


class DiscoveryResponder:
    """服务端侧 UDP 发现响应器。"""

    def __init__(
        self,
        tcp_port: int,
        udp_port: int = DISCOVERY_PORT,
        server_name: str = "LAN-ChatRoom",
    ) -> None:
        self.tcp_port = tcp_port
        self.udp_port = udp_port
        self.server_name = server_name
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._sock: Optional[socket.socket] = None

    def start(self) -> int:
        """启动后台 UDP 监听线程，返回实际绑定的 UDP 端口。"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", self.udp_port))
        sock.settimeout(0.5)
        self._sock = sock
        bound_port = sock.getsockname()[1]

        self._thread = threading.Thread(
            target=self._serve_loop,
            name="udp-discovery-responder",
            daemon=True,
        )
        self._thread.start()
        return bound_port

    def _serve_loop(self) -> None:
        assert self._sock is not None
        reply_payload = json.dumps(
            {
                "service": "lan_chat",
                "tcp_port": self.tcp_port,
                "server_name": self.server_name,
            },
            ensure_ascii=False,
        ).encode("utf-8")

        while not self._stop_event.is_set():
            try:
                data, addr = self._sock.recvfrom(1024)
            except socket.timeout:
                continue
            except OSError:
                break

            if data.strip() == DISCOVERY_MAGIC:
                try:
                    self._sock.sendto(reply_payload, addr)
                except OSError:
                    continue

    def stop(self) -> None:
        """停止 UDP 发现服务并释放套接字。"""
        self._stop_event.set()
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=1.5)


def discover_server(
    udp_port: int = DISCOVERY_PORT,
    timeout: float = 2.0,
    broadcast_addr: str = "255.255.255.255",
) -> Optional[Tuple[str, int, str]]:
    """通过 UDP 广播自动发现局域网内的聊天室服务端。

    Returns:
        (server_ip, tcp_port, server_name) 元组；若超时未发现则返回 None。
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.settimeout(timeout)

        # 发送广播探测包；在部分本机测试环境中同时补发 127.0.0.1 兜底
        targets = [broadcast_addr]
        if broadcast_addr == "255.255.255.255":
            targets.append("127.0.0.1")

        for target in targets:
            try:
                sock.sendto(DISCOVERY_MAGIC, (target, udp_port))
            except OSError:
                continue

        try:
            data, (server_ip, _) = sock.recvfrom(2048)
        except socket.timeout:
            return None

        try:
            info = json.loads(data.decode("utf-8"))
            if info.get("service") != "lan_chat":
                return None
            tcp_port = int(info["tcp_port"])
            server_name = str(info.get("server_name", "LAN-ChatRoom"))
            return server_ip, tcp_port, server_name
        except (ValueError, KeyError, UnicodeDecodeError):
            return None
