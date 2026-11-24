"""局域网聊天室 TCP 服务端（多线程并发 + 心跳超时检测 + UDP 自动发现）。

运行方式：
    python3 -m chatroom.server --host 0.0.0.0 --port 8888
"""

from __future__ import annotations

import argparse
import logging
import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from chatroom.discovery import DISCOVERY_PORT, DiscoveryResponder
from chatroom.protocol import (
    MSG_CHAT,
    MSG_ERROR,
    MSG_HEARTBEAT,
    MSG_LOGIN,
    MSG_LOGIN_ACK,
    MSG_LOGOUT,
    MSG_PRIVATE,
    MSG_SYSTEM,
    MSG_USER_LIST,
    ProtocolError,
    current_timestamp,
    recv_packet,
    send_packet,
)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("chatroom.server")


@dataclass
class ClientSession:
    """维护单个在线客户端的连接状态与发送锁。"""

    username: str
    sock: socket.socket
    addr: Tuple[str, int]
    last_seen: float = field(default_factory=time.monotonic)
    send_lock: threading.Lock = field(default_factory=threading.Lock)

    def send(self, msg_type: str, **payload: object) -> bool:
        """线程安全地向该客户端发送一条协议报文。"""
        with self.send_lock:
            try:
                send_packet(self.sock, msg_type, **payload)
                return True
            except OSError:
                return False


def detect_lan_ip() -> str:
    """获取本机局域网出口 IP（不实际发送网络包）。"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"


class ChatServer:
    """多线程 TCP 局域网聊天室服务端。"""

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8888,
        udp_discovery_port: Optional[int] = DISCOVERY_PORT,
        heartbeat_timeout: float = 30.0,
        server_name: str = "LAN-ChatRoom",
    ) -> None:
        self.host = host
        self.port = port
        self.udp_discovery_port = udp_discovery_port
        self.heartbeat_timeout = heartbeat_timeout
        self.server_name = server_name

        self._server_sock: Optional[socket.socket] = None
        self._discovery: Optional[DiscoveryResponder] = None
        self._stop_event = threading.Event()
        self._clients_lock = threading.Lock()
        self._clients: Dict[str, ClientSession] = {}
        self._accept_thread: Optional[threading.Thread] = None
        self._watchdog_thread: Optional[threading.Thread] = None

        self.bound_tcp_port: int = port
        self.bound_udp_port: Optional[int] = None

    def start(self, blocking: bool = True) -> None:
        """启动 TCP 监听、心跳监控与可选的 UDP 自动发现服务。"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        sock.listen(64)
        sock.settimeout(0.5)
        self._server_sock = sock
        self.bound_tcp_port = sock.getsockname()[1]

        if self.udp_discovery_port is not None:
            self._discovery = DiscoveryResponder(
                tcp_port=self.bound_tcp_port,
                udp_port=self.udp_discovery_port,
                server_name=self.server_name,
            )
            self.bound_udp_port = self._discovery.start()

        self._watchdog_thread = threading.Thread(
            target=self._heartbeat_watchdog,
            name="heartbeat-watchdog",
            daemon=True,
        )
        self._watchdog_thread.start()

        lan_ip = detect_lan_ip()
        logger.info(
            "服务端已启动: TCP %s:%d (局域网参考 IP: %s)",
            self.host,
            self.bound_tcp_port,
            lan_ip,
        )
        if self.bound_udp_port is not None:
            logger.info("UDP 自动发现已开启: 端口 %d", self.bound_udp_port)

        if blocking:
            try:
                self._accept_loop()
            except KeyboardInterrupt:
                logger.info("收到退出信号，正在关闭服务端...")
            finally:
                self.stop()
        else:
            self._accept_thread = threading.Thread(
                target=self._accept_loop,
                name="tcp-accept-loop",
                daemon=True,
            )
            self._accept_thread.start()

    def stop(self) -> None:
        """优雅关闭服务端及所有客户端连接。"""
        if self._stop_event.is_set():
            return
        self._stop_event.set()

        if self._discovery is not None:
            self._discovery.stop()

        if self._server_sock is not None:
            try:
                self._server_sock.close()
            except OSError:
                pass

        with self._clients_lock:
            sessions = list(self._clients.values())
            self._clients.clear()

        for session in sessions:
            try:
                session.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                session.sock.close()
            except OSError:
                pass

        if self._accept_thread is not None and self._accept_thread.is_alive():
            self._accept_thread.join(timeout=1.5)
        logger.info("服务端已关闭。")

    def _accept_loop(self) -> None:
        assert self._server_sock is not None
        while not self._stop_event.is_set():
            try:
                conn, addr = self._server_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            worker = threading.Thread(
                target=self._handle_client,
                args=(conn, addr),
                name=f"client-{addr[0]}:{addr[1]}",
                daemon=True,
            )
            worker.start()

    def _heartbeat_watchdog(self) -> None:
        """定期扫描超时未发送心跳/消息的僵尸连接并主动断开。"""
        interval = max(1.0, min(5.0, self.heartbeat_timeout / 3.0))
        while not self._stop_event.wait(interval):
            now = time.monotonic()
            expired: List[ClientSession] = []
            with self._clients_lock:
                for session in self._clients.values():
                    if now - session.last_seen > self.heartbeat_timeout:
                        expired.append(session)
            for session in expired:
                logger.warning("用户 %s 心跳超时，断开连接。", session.username)
                try:
                    session.sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    session.sock.close()
                except OSError:
                    pass

    def _get_usernames(self) -> List[str]:
        with self._clients_lock:
            return sorted(self._clients.keys())

    def _broadcast(
        self,
        msg_type: str,
        exclude: Optional[str] = None,
        **payload: object,
    ) -> None:
        with self._clients_lock:
            targets = [
                s for name, s in self._clients.items() if name != exclude
            ]
        for session in targets:
            session.send(msg_type, **payload)

    def _handle_client(self, conn: socket.socket, addr: Tuple[str, int]) -> None:
        session: Optional[ClientSession] = None
        try:
            # 1. 登录阶段：要求首包为 LOGIN，限制 15 秒内完成
            conn.settimeout(15.0)
            first_pkt = recv_packet(conn)
            if first_pkt is None or first_pkt.get("type") != MSG_LOGIN:
                send_packet(conn, MSG_LOGIN_ACK, ok=False, reason="首包必须为 LOGIN")
                return

            username = str(first_pkt.get("username", "")).strip()
            if not username or len(username) > 20 or any(ch.isspace() for ch in username):
                send_packet(
                    conn,
                    MSG_LOGIN_ACK,
                    ok=False,
                    reason="昵称须为 1~20 个非空白字符",
                )
                return

            with self._clients_lock:
                if username in self._clients:
                    send_packet(conn, MSG_LOGIN_ACK, ok=False, reason="该昵称已被占用")
                    return
                session = ClientSession(username=username, sock=conn, addr=addr)
                self._clients[username] = session
                online_users = sorted(self._clients.keys())

            conn.settimeout(None)
            session.send(
                MSG_LOGIN_ACK,
                ok=True,
                username=username,
                users=online_users,
            )
            logger.info("用户上线: %s (%s:%d)", username, addr[0], addr[1])

            self._broadcast(
                MSG_SYSTEM,
                text=f"{username} 加入了聊天室",
                ts=current_timestamp(),
            )
            self._broadcast(MSG_USER_LIST, users=online_users)

            # 2. 消息循环阶段
            while not self._stop_event.is_set():
                pkt = recv_packet(conn)
                if pkt is None:
                    break
                session.last_seen = time.monotonic()
                msg_type = pkt.get("type")

                if msg_type == MSG_CHAT:
                    text = str(pkt.get("text", "")).strip()
                    if text:
                        ts = current_timestamp()
                        logger.info("[群聊] %s: %s", username, text)
                        self._broadcast(
                            MSG_CHAT,
                            sender=username,
                            text=text,
                            ts=ts,
                        )
                elif msg_type == MSG_PRIVATE:
                    target_name = str(pkt.get("to", "")).strip()
                    text = str(pkt.get("text", "")).strip()
                    if not target_name or not text:
                        session.send(MSG_ERROR, text="私聊格式错误，缺少目标用户或内容")
                        continue
                    with self._clients_lock:
                        target = self._clients.get(target_name)
                    if target is None:
                        session.send(MSG_ERROR, text=f"用户 {target_name} 不在线")
                        continue
                    ts = current_timestamp()
                    target.send(
                        MSG_PRIVATE,
                        sender=username,
                        to=target_name,
                        text=text,
                        ts=ts,
                    )
                    if target_name != username:
                        session.send(
                            MSG_PRIVATE,
                            sender=username,
                            to=target_name,
                            text=text,
                            ts=ts,
                        )
                elif msg_type == MSG_USER_LIST:
                    session.send(MSG_USER_LIST, users=self._get_usernames())
                elif msg_type == MSG_HEARTBEAT:
                    session.send(MSG_HEARTBEAT, status="ok")
                elif msg_type == MSG_LOGOUT:
                    break
                else:
                    session.send(MSG_ERROR, text=f"不支持的消息类型: {msg_type}")

        except (ConnectionError, OSError, ProtocolError) as exc:
            if not self._stop_event.is_set():
                logger.debug("连接 %s 异常结束: %s", addr, exc)
        finally:
            if session is not None:
                removed = False
                with self._clients_lock:
                    if self._clients.get(session.username) is session:
                        del self._clients[session.username]
                        removed = True
                    online_users = sorted(self._clients.keys())
                if removed and not self._stop_event.is_set():
                    logger.info("用户下线: %s", session.username)
                    self._broadcast(
                        MSG_SYSTEM,
                        text=f"{session.username} 离开了聊天室",
                        ts=current_timestamp(),
                    )
                    self._broadcast(MSG_USER_LIST, users=online_users)
            try:
                conn.close()
            except OSError:
                pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="局域网聊天室 TCP 服务端")
    parser.add_argument("--host", default="0.0.0.0", help="绑定地址 (默认 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8888, help="TCP 监听端口 (默认 8888)")
    parser.add_argument(
        "--udp-port",
        type=int,
        default=DISCOVERY_PORT,
        help=f"UDP 广播发现端口 (默认 {DISCOVERY_PORT}，设为 0 禁用)",
    )
    parser.add_argument(
        "--heartbeat-timeout",
        type=float,
        default=30.0,
        help="心跳超时秒数 (默认 30.0)",
    )
    parser.add_argument("--name", default="LAN-ChatRoom", help="聊天室名称")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    udp_port = None if args.udp_port == 0 else args.udp_port
    server = ChatServer(
        host=args.host,
        port=args.port,
        udp_discovery_port=udp_port,
        heartbeat_timeout=args.heartbeat_timeout,
        server_name=args.name,
    )
    server.start(blocking=True)


if __name__ == "__main__":
    main()
