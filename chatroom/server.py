"""局域网聊天室 TCP 服务端（多线程并发 + SQLite 持久化 + 多房间路由 + ACK/已读回执 + UDP 自动发现）。

运行方式：
    python3 -m chatroom.server --host 0.0.0.0 --port 8888
"""

from __future__ import annotations

import argparse
import logging
import socket
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from chatroom.discovery import DISCOVERY_PORT, DiscoveryResponder
from chatroom.protocol import (
    DEFAULT_ROOM_ID,
    MAX_FILE_SIZE,
    MSG_ACK,
    MSG_CHAT,
    MSG_ERROR,
    MSG_FILE_CHUNK,
    MSG_FILE_END,
    MSG_FILE_META,
    MSG_HEARTBEAT,
    MSG_HISTORY_REQ,
    MSG_HISTORY_RESP,
    MSG_LOGIN,
    MSG_LOGIN_ACK,
    MSG_LOGOUT,
    MSG_PRIVATE,
    MSG_READ,
    MSG_ROOM_CREATE,
    MSG_ROOM_JOIN,
    MSG_ROOM_LEAVE,
    MSG_ROOM_LIST,
    MSG_SYSTEM,
    MSG_USER_LIST,
    ProtocolError,
    current_timestamp,
    recv_packet,
    send_packet,
)
from chatroom.storage import ChatStorage, make_private_conv_id

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("chatroom.server")


@dataclass
class ClientSession:
    """维护单个在线客户端的连接状态、所在房间与发送锁。"""

    username: str
    sock: socket.socket
    addr: Tuple[str, int]
    rooms: Set[str] = field(default_factory=lambda: {DEFAULT_ROOM_ID})
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
        db_path: str = "chatroom_data.db",
    ) -> None:
        self.host = host
        self.port = port
        self.udp_discovery_port = udp_discovery_port
        self.heartbeat_timeout = heartbeat_timeout
        self.server_name = server_name
        self.db_path = db_path

        self._storage = ChatStorage(db_path=db_path)
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
            "服务端已启动: TCP %s:%d (局域网参考 IP: %s, SQLite: %s)",
            self.host,
            self.bound_tcp_port,
            lan_ip,
            self.db_path,
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
        """优雅关闭服务端、所有客户端连接与数据库连接。"""
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
        self._storage.close()
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

    def _build_room_list(self, username: str) -> List[Dict[str, Any]]:
        rooms = self._storage.list_rooms()
        with self._clients_lock:
            sessions = list(self._clients.values())
            me = self._clients.get(username)
            my_rooms = set(me.rooms) if me else {DEFAULT_ROOM_ID}
            counts: Dict[str, int] = {}
            for s in sessions:
                for rid in s.rooms:
                    counts[rid] = counts.get(rid, 0) + 1

        result: List[Dict[str, Any]] = []
        for r in rooms:
            rid = r["room_id"]
            result.append(
                {
                    "room_id": rid,
                    "name": r["name"],
                    "creator": r["creator"],
                    "member_count": counts.get(rid, 0),
                    "joined": rid in my_rooms,
                }
            )
        return result

    def _broadcast_room_lists(self) -> None:
        with self._clients_lock:
            sessions = list(self._clients.values())
        for s in sessions:
            s.send(MSG_ROOM_LIST, rooms=self._build_room_list(s.username))

    def _broadcast(
        self,
        msg_type: str,
        exclude: Optional[str] = None,
        **payload: object,
    ) -> None:
        with self._clients_lock:
            targets = [s for name, s in self._clients.items() if name != exclude]
        for session in targets:
            session.send(msg_type, **payload)

    def _broadcast_room(
        self,
        target_room_id: str,
        msg_type: str,
        exclude: Optional[str] = None,
        **payload: object,
    ) -> None:
        with self._clients_lock:
            targets = [
                s
                for name, s in self._clients.items()
                if name != exclude and target_room_id in s.rooms
            ]
        for session in targets:
            session.send(msg_type, **payload)

    def _route_file_packet(
        self,
        sender_session: ClientSession,
        msg_type: str,
        target_name: Optional[str],
        room_id: str = DEFAULT_ROOM_ID,
        **payload: object,
    ) -> bool:
        """将文件元信息/分块/结束帧转发给指定房间成员或私聊双方。"""
        if target_name:
            with self._clients_lock:
                target = self._clients.get(target_name)
            if target is None:
                sender_session.send(MSG_ERROR, text=f"用户 {target_name} 不在线，文件发送中止")
                return False
            target.send(msg_type, sender=sender_session.username, to=target_name, **payload)
            if target_name != sender_session.username:
                sender_session.send(
                    msg_type, sender=sender_session.username, to=target_name, **payload
                )
            return True

        self._broadcast_room(
            room_id,
            msg_type,
            sender=sender_session.username,
            to=None,
            room_id=room_id,
            **payload,
        )
        return True

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
            rooms_snapshot = self._build_room_list(username)
            session.send(
                MSG_LOGIN_ACK,
                ok=True,
                username=username,
                users=online_users,
                rooms=rooms_snapshot,
            )
            logger.info("用户上线: %s (%s:%d)", username, addr[0], addr[1])

            self._broadcast(
                MSG_SYSTEM,
                room_id=DEFAULT_ROOM_ID,
                text=f"{username} 加入了聊天室",
                ts=current_timestamp(),
            )
            self._broadcast(MSG_USER_LIST, users=online_users)
            self._broadcast_room_lists()

            # 2. 消息循环阶段
            while not self._stop_event.is_set():
                pkt = recv_packet(conn)
                if pkt is None:
                    break
                session.last_seen = time.monotonic()
                msg_type = pkt.get("type")

                if msg_type == MSG_CHAT:
                    text = str(pkt.get("text", "")).strip()
                    room_id = str(pkt.get("room_id") or DEFAULT_ROOM_ID).strip()
                    if not text:
                        continue
                    if self._storage.get_room(room_id) is None:
                        session.send(MSG_ERROR, text=f"房间 {room_id} 不存在")
                        continue
                    with self._clients_lock:
                        session.rooms.add(room_id)

                    msg_id = str(pkt.get("msg_id") or uuid.uuid4().hex)
                    ts = current_timestamp()
                    self._storage.save_message(
                        msg_id=msg_id,
                        conv_type="room",
                        conv_id=room_id,
                        sender=username,
                        ts=ts,
                        text=text,
                    )
                    logger.info("[房间:%s] %s: %s", room_id, username, text)
                    session.send(MSG_ACK, msg_id=msg_id, status="delivered", room_id=room_id, ts=ts)
                    self._broadcast_room(
                        room_id,
                        MSG_CHAT,
                        msg_id=msg_id,
                        room_id=room_id,
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
                    msg_id = str(pkt.get("msg_id") or uuid.uuid4().hex)
                    ts = current_timestamp()
                    self._storage.save_message(
                        msg_id=msg_id,
                        conv_type="private",
                        conv_id=make_private_conv_id(username, target_name),
                        sender=username,
                        target=target_name,
                        ts=ts,
                        text=text,
                    )
                    session.send(MSG_ACK, msg_id=msg_id, status="delivered", to=target_name, ts=ts)
                    target.send(
                        MSG_PRIVATE,
                        msg_id=msg_id,
                        sender=username,
                        to=target_name,
                        text=text,
                        ts=ts,
                    )
                    if target_name != username:
                        session.send(
                            MSG_PRIVATE,
                            msg_id=msg_id,
                            sender=username,
                            to=target_name,
                            text=text,
                            ts=ts,
                        )

                elif msg_type == MSG_READ:
                    raw_ids = pkt.get("msg_ids")
                    if isinstance(raw_ids, list):
                        msg_ids = [str(mid).strip() for mid in raw_ids if str(mid).strip()]
                    elif pkt.get("msg_id"):
                        msg_ids = [str(pkt.get("msg_id")).strip()]
                    else:
                        msg_ids = []
                    newly_read = self._storage.mark_read(msg_ids, username)
                    ts = current_timestamp()
                    for rec in newly_read:
                        if rec["conv_type"] == "private":
                            with self._clients_lock:
                                orig_sender = self._clients.get(rec["sender"])
                            if orig_sender is not None:
                                orig_sender.send(
                                    MSG_READ,
                                    msg_id=rec["msg_id"],
                                    reader=username,
                                    to=username,
                                    ts=ts,
                                )
                        else:
                            self._broadcast_room(
                                rec["conv_id"],
                                MSG_READ,
                                msg_id=rec["msg_id"],
                                reader=username,
                                room_id=rec["conv_id"],
                                ts=ts,
                            )

                elif msg_type == MSG_HISTORY_REQ:
                    limit = int(pkt.get("limit", 50))
                    peer_raw = pkt.get("peer")
                    room_raw = pkt.get("room_id")
                    if peer_raw:
                        peer = str(peer_raw).strip()
                        history = self._storage.get_history(
                            "private", make_private_conv_id(username, peer), limit=limit
                        )
                        session.send(MSG_HISTORY_RESP, peer=peer, room_id=None, messages=history)
                    else:
                        room_id = str(room_raw or DEFAULT_ROOM_ID).strip()
                        history = self._storage.get_history("room", room_id, limit=limit)
                        session.send(MSG_HISTORY_RESP, room_id=room_id, peer=None, messages=history)

                elif msg_type == MSG_ROOM_CREATE:
                    name = str(pkt.get("name", "")).strip()
                    if not name or len(name) > 24:
                        session.send(MSG_ERROR, text="房间名称须为 1~24 个字符")
                        continue
                    room_id = f"room_{uuid.uuid4().hex[:8]}"
                    self._storage.create_room(room_id, name, username)
                    with self._clients_lock:
                        session.rooms.add(room_id)
                    logger.info("创建房间: %s (%s) by %s", name, room_id, username)
                    self._broadcast_room_lists()
                    session.send(
                        MSG_SYSTEM,
                        room_id=room_id,
                        text=f"{username} 创建了房间「{name}」",
                        ts=current_timestamp(),
                    )

                elif msg_type == MSG_ROOM_JOIN:
                    room_id = str(pkt.get("room_id", "")).strip()
                    room = self._storage.get_room(room_id)
                    if room is None:
                        session.send(MSG_ERROR, text="目标房间不存在")
                        continue
                    with self._clients_lock:
                        already_in = room_id in session.rooms
                        session.rooms.add(room_id)
                    self._broadcast_room_lists()
                    if not already_in:
                        self._broadcast_room(
                            room_id,
                            MSG_SYSTEM,
                            room_id=room_id,
                            text=f"{username} 加入了房间「{room['name']}」",
                            ts=current_timestamp(),
                        )

                elif msg_type == MSG_ROOM_LEAVE:
                    room_id = str(pkt.get("room_id", "")).strip()
                    if room_id == DEFAULT_ROOM_ID:
                        session.send(MSG_ERROR, text="不能退出公共大厅")
                        continue
                    with self._clients_lock:
                        was_in = room_id in session.rooms
                        session.rooms.discard(room_id)
                    self._broadcast_room_lists()
                    if was_in:
                        self._broadcast_room(
                            room_id,
                            MSG_SYSTEM,
                            room_id=room_id,
                            text=f"{username} 离开了房间",
                            ts=current_timestamp(),
                        )

                elif msg_type == MSG_ROOM_LIST:
                    session.send(MSG_ROOM_LIST, rooms=self._build_room_list(username))

                elif msg_type == MSG_FILE_META:
                    file_id = str(pkt.get("file_id", "")).strip()
                    filename = str(pkt.get("filename", "")).strip()
                    size = int(pkt.get("size", -1))
                    total_chunks = int(pkt.get("total_chunks", 0))
                    target_raw = pkt.get("to")
                    target_name = str(target_raw).strip() if target_raw else None
                    room_id = str(pkt.get("room_id") or DEFAULT_ROOM_ID).strip()
                    if not file_id or not filename or size < 0 or size > MAX_FILE_SIZE or total_chunks < 1:
                        session.send(MSG_ERROR, text="文件元信息非法或超出 20 MB 大小限制")
                        continue
                    logger.info(
                        "[文件传输开始] %s -> %s: %s (%d bytes, %d chunks)",
                        username,
                        target_name or f"房间:{room_id}",
                        filename,
                        size,
                        total_chunks,
                    )
                    self._route_file_packet(
                        session,
                        MSG_FILE_META,
                        target_name,
                        room_id=room_id,
                        file_id=file_id,
                        filename=filename,
                        size=size,
                        mime_type=str(pkt.get("mime_type", "application/octet-stream")),
                        is_image=bool(pkt.get("is_image", False)),
                        sha256=str(pkt.get("sha256", "")),
                        total_chunks=total_chunks,
                        ts=current_timestamp(),
                    )

                elif msg_type == MSG_FILE_CHUNK:
                    file_id = str(pkt.get("file_id", "")).strip()
                    data = pkt.get("data")
                    seq = int(pkt.get("seq", -1))
                    total_chunks = int(pkt.get("total_chunks", 0))
                    target_raw = pkt.get("to")
                    target_name = str(target_raw).strip() if target_raw else None
                    room_id = str(pkt.get("room_id") or DEFAULT_ROOM_ID).strip()
                    if not file_id or not isinstance(data, str) or seq < 0 or total_chunks < 1:
                        continue
                    self._route_file_packet(
                        session,
                        MSG_FILE_CHUNK,
                        target_name,
                        room_id=room_id,
                        file_id=file_id,
                        seq=seq,
                        total_chunks=total_chunks,
                        data=data,
                    )

                elif msg_type == MSG_FILE_END:
                    file_id = str(pkt.get("file_id", "")).strip()
                    target_raw = pkt.get("to")
                    target_name = str(target_raw).strip() if target_raw else None
                    room_id = str(pkt.get("room_id") or DEFAULT_ROOM_ID).strip()
                    if not file_id:
                        continue
                    logger.info("[文件传输完成] %s (file_id=%s)", username, file_id[:8])
                    self._route_file_packet(
                        session,
                        MSG_FILE_END,
                        target_name,
                        room_id=room_id,
                        file_id=file_id,
                        sha256=str(pkt.get("sha256", "")),
                        ts=current_timestamp(),
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
                        room_id=DEFAULT_ROOM_ID,
                        text=f"{session.username} 离开了聊天室",
                        ts=current_timestamp(),
                    )
                    self._broadcast(MSG_USER_LIST, users=online_users)
                    self._broadcast_room_lists()
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
    parser.add_argument("--db", default="chatroom_data.db", help="SQLite 数据库文件路径 (默认 chatroom_data.db)")
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
        db_path=args.db,
    )
    server.start(blocking=True)


if __name__ == "__main__":
    main()
