"""局域网聊天室现代简约 IM Web 界面客户端（零第三方依赖，底层走自定义 TCP/UDP 协议）。

支持：
- 多房间群聊与点对点私聊
- SQLite 历史消息同步
- 消息可靠送达回执（ACK）与已读状态（READ）
- 断线指数退避自动重连（1s -> 2s -> 4s -> 8s）
- UDP 局域网广播自动发现服务端
- TCP 分块文件 / 图片传输（48 KB/帧）与 SHA-256 完整性校验

运行方式：
    python3 -m chatroom.web_client --web-port 8080
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import mimetypes
import os
import queue
import socket
import tempfile
import threading
import urllib.parse
import uuid
import webbrowser
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from chatroom.discovery import DISCOVERY_PORT, discover_server
from chatroom.protocol import (
    DEFAULT_ROOM_ID,
    FILE_CHUNK_SIZE,
    MAX_FILE_SIZE,
    MSG_CHAT,
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
    MSG_USER_LIST,
    ProtocolError,
    recv_packet,
    send_packet,
)

STATIC_INDEX = Path(__file__).resolve().parent / "web" / "index.html"


def sanitize_filename(name: str) -> str:
    """提取安全文件名，防止路径穿越。"""
    base = os.path.basename(name.strip().replace("\\", "/"))
    return base or "unnamed_file"


class WebChatBridge:
    """将浏览器 HTTP/SSE 请求桥接到原生 TCP 局域网聊天室服务端。"""

    def __init__(
        self,
        udp_port: int = DISCOVERY_PORT,
        reconnect_base_delay: float = 1.0,
    ) -> None:
        self.udp_port = udp_port
        self.reconnect_base_delay = reconnect_base_delay

        self._lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._sock: Optional[socket.socket] = None
        self._stop_event = threading.Event()
        self._closed = False
        self._subscribers: List[queue.Queue[Dict[str, Any]]] = []

        self._auto_reconnect = False
        self._reconnecting = False
        self._reconnect_cancel = threading.Event()
        self._last_conn: Optional[Tuple[str, int, str]] = None
        self._joined_rooms: Set[str] = {DEFAULT_ROOM_ID}

        self._temp_dir = tempfile.TemporaryDirectory(prefix="lanchat_files_")
        self._incoming_files: Dict[str, Dict[str, Any]] = {}
        self._completed_files: Dict[str, Dict[str, Any]] = {}

        self.connected: bool = False
        self.host: str = ""
        self.port: int = 8888
        self.username: str = ""
        self.users: List[str] = []
        self.rooms: List[Dict[str, Any]] = []

    def close(self) -> None:
        self._closed = True
        self.disconnect(send_logout=True, manual=True)
        self._temp_dir.cleanup()

    def subscribe(self) -> queue.Queue[Dict[str, Any]]:
        q: queue.Queue[Dict[str, Any]] = queue.Queue()
        with self._lock:
            self._subscribers.append(q)
        return q

    def unsubscribe(self, q: queue.Queue[Dict[str, Any]]) -> None:
        with self._lock:
            if q in self._subscribers:
                self._subscribers.remove(q)

    def _publish(self, event: Dict[str, Any]) -> None:
        with self._lock:
            targets = list(self._subscribers)
        for q in targets:
            q.put(event)

    def discover(self) -> Optional[Dict[str, Any]]:
        found = discover_server(udp_port=self.udp_port, timeout=2.0)
        if found is None:
            return None
        ip, tcp_port, server_name = found
        return {"ok": True, "host": ip, "port": tcp_port, "server_name": server_name}

    def _connect_socket(self, host: str, port: int, username: str) -> Dict[str, Any]:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(8.0)
        try:
            sock.connect((host, port))
            send_packet(sock, MSG_LOGIN, username=username)
            ack = recv_packet(sock)
        except (OSError, ProtocolError) as exc:
            sock.close()
            return {"ok": False, "error": f"连接失败: {exc}"}

        if not ack or ack.get("type") != MSG_LOGIN_ACK or not ack.get("ok"):
            reason = ack.get("reason", "服务端无响应") if ack else "服务端无响应"
            sock.close()
            return {"ok": False, "error": f"登录失败: {reason}"}

        sock.settimeout(None)
        with self._lock:
            self._sock = sock
            self._stop_event.clear()
            self.connected = True
            self.host = host
            self.port = port
            self.username = username
            self.users = list(ack.get("users", []))
            self.rooms = list(ack.get("rooms", []))
            self._last_conn = (host, port, username)

        threading.Thread(target=self._recv_loop, args=(sock,), daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, args=(sock,), daemon=True).start()
        return {
            "ok": True,
            "username": self.username,
            "users": self.users,
            "rooms": self.rooms,
            "host": host,
            "port": port,
        }

    def connect(self, host: str, port: int, username: str) -> Dict[str, Any]:
        self.disconnect(send_logout=True, manual=True)
        res = self._connect_socket(host, port, username)
        if res.get("ok"):
            with self._lock:
                self._auto_reconnect = True
                self._reconnect_cancel.clear()
                self._joined_rooms = {DEFAULT_ROOM_ID}
        return res

    def disconnect(self, send_logout: bool = False, manual: bool = True) -> None:
        with self._lock:
            if manual:
                self._auto_reconnect = False
                self._reconnect_cancel.set()
            sock = self._sock
            was_connected = self.connected
            self._sock = None
            self.connected = False
            self._stop_event.set()
            self._incoming_files.clear()

        if sock is not None:
            if send_logout and was_connected:
                try:
                    with self._send_lock:
                        send_packet(sock, MSG_LOGOUT)
                except OSError:
                    pass
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass

    def _trigger_auto_reconnect(self) -> None:
        with self._lock:
            if self._closed or not self._auto_reconnect or self._reconnecting or self._last_conn is None:
                return
            self._reconnecting = True
            self._reconnect_cancel.clear()
            target = self._last_conn

        def _worker() -> None:
            host, port, username = target
            attempt = 0
            try:
                while not self._closed and self._auto_reconnect and not self._reconnect_cancel.is_set():
                    delay = min(10.0, self.reconnect_base_delay * (2 ** min(attempt, 4)))
                    attempt += 1
                    self._publish(
                        {
                            "type": "RECONNECTING",
                            "attempt": attempt,
                            "delay": round(delay, 2),
                            "text": f"网络已断开，{delay:.1f}s 后进行第 {attempt} 次自动重连...",
                        }
                    )
                    if self._reconnect_cancel.wait(delay):
                        break
                    if self._closed or not self._auto_reconnect:
                        break

                    res = self._connect_socket(host, port, username)
                    if res.get("ok"):
                        with self._lock:
                            rooms_to_rejoin = [
                                rid for rid in self._joined_rooms if rid != DEFAULT_ROOM_ID
                            ]
                        for rid in rooms_to_rejoin:
                            self.join_room(rid)
                        self._publish(
                            {
                                "type": "RECONNECTED",
                                "username": self.username,
                                "users": self.users,
                                "rooms": self.rooms,
                                "host": host,
                                "port": port,
                                "text": "已自动重新连接到局域网服务端",
                            }
                        )
                        return
            finally:
                with self._lock:
                    self._reconnecting = False

        threading.Thread(target=_worker, name="tcp-auto-reconnect", daemon=True).start()

    def _send_raw(self, msg_type: str, **payload: object) -> Dict[str, Any]:
        with self._lock:
            sock = self._sock
            connected = self.connected
        if not connected or sock is None:
            return {"ok": False, "error": "尚未连接到服务端"}
        try:
            with self._send_lock:
                send_packet(sock, msg_type, **payload)
            return {"ok": True}
        except OSError as exc:
            return {"ok": False, "error": str(exc)}

    def send_message(
        self,
        text: str,
        to: Optional[str] = None,
        room_id: str = DEFAULT_ROOM_ID,
        msg_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        actual_id = msg_id or uuid.uuid4().hex
        if to:
            res = self._send_raw(MSG_PRIVATE, to=to, text=text, msg_id=actual_id)
        else:
            res = self._send_raw(MSG_CHAT, room_id=room_id, text=text, msg_id=actual_id)
        if res.get("ok"):
            res["msg_id"] = actual_id
        return res

    def mark_read(self, msg_ids: List[str]) -> Dict[str, Any]:
        cleaned = [m for m in msg_ids if m]
        if not cleaned:
            return {"ok": True}
        return self._send_raw(MSG_READ, msg_ids=cleaned)

    def request_history(
        self,
        room_id: Optional[str] = None,
        peer: Optional[str] = None,
        limit: int = 50,
    ) -> Dict[str, Any]:
        return self._send_raw(MSG_HISTORY_REQ, room_id=room_id, peer=peer, limit=limit)

    def create_room(self, name: str) -> Dict[str, Any]:
        return self._send_raw(MSG_ROOM_CREATE, name=name)

    def join_room(self, room_id: str) -> Dict[str, Any]:
        res = self._send_raw(MSG_ROOM_JOIN, room_id=room_id)
        if res.get("ok"):
            with self._lock:
                self._joined_rooms.add(room_id)
        return res

    def leave_room(self, room_id: str) -> Dict[str, Any]:
        res = self._send_raw(MSG_ROOM_LEAVE, room_id=room_id)
        if res.get("ok"):
            with self._lock:
                self._joined_rooms.discard(room_id)
        return res

    def send_file(
        self,
        filename: str,
        raw_bytes: bytes,
        mime_type: Optional[str] = None,
        is_image: Optional[bool] = None,
        to: Optional[str] = None,
        room_id: str = DEFAULT_ROOM_ID,
    ) -> Dict[str, Any]:
        """按 48 KB/块通过自定义 TCP 协议分块发送文件或图片，并附带 SHA-256 校验值。"""
        with self._lock:
            sock = self._sock
            connected = self.connected
        if not connected or sock is None:
            return {"ok": False, "error": "尚未连接到服务端"}

        size = len(raw_bytes)
        if size > MAX_FILE_SIZE:
            return {
                "ok": False,
                "error": f"文件超过 {MAX_FILE_SIZE // (1024 * 1024)} MB 大小限制",
            }

        clean_name = sanitize_filename(filename)
        detected_mime = mime_type or mimetypes.guess_type(clean_name)[0] or "application/octet-stream"
        img_flag = (
            bool(is_image)
            if is_image is not None
            else detected_mime.startswith("image/")
        )
        file_id = uuid.uuid4().hex
        sha256_hex = hashlib.sha256(raw_bytes).hexdigest()
        total_chunks = max(1, math.ceil(size / FILE_CHUNK_SIZE))

        def _tx_worker() -> None:
            try:
                with self._send_lock:
                    send_packet(
                        sock,
                        MSG_FILE_META,
                        file_id=file_id,
                        filename=clean_name,
                        size=size,
                        mime_type=detected_mime,
                        is_image=img_flag,
                        sha256=sha256_hex,
                        total_chunks=total_chunks,
                        to=to,
                        room_id=room_id,
                    )

                for seq in range(total_chunks):
                    start = seq * FILE_CHUNK_SIZE
                    chunk = raw_bytes[start : start + FILE_CHUNK_SIZE]
                    b64 = base64.b64encode(chunk).decode("ascii")
                    with self._send_lock:
                        send_packet(
                            sock,
                            MSG_FILE_CHUNK,
                            file_id=file_id,
                            seq=seq,
                            total_chunks=total_chunks,
                            data=b64,
                            to=to,
                            room_id=room_id,
                        )

                with self._send_lock:
                    send_packet(
                        sock,
                        MSG_FILE_END,
                        file_id=file_id,
                        sha256=sha256_hex,
                        to=to,
                        room_id=room_id,
                    )
            except OSError as exc:
                self._publish({"type": "ERROR", "text": f"文件 {clean_name} 发送失败: {exc}"})

        threading.Thread(
            target=_tx_worker,
            name=f"tcp-file-tx-{file_id[:8]}",
            daemon=True,
        ).start()
        return {
            "ok": True,
            "file_id": file_id,
            "filename": clean_name,
            "size": size,
            "sha256": sha256_hex,
            "total_chunks": total_chunks,
        }

    def get_completed_file(self, file_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._completed_files.get(file_id)

    def _handle_file_packet(self, pkt: Dict[str, Any]) -> None:
        msg_type = pkt.get("type")
        file_id = str(pkt.get("file_id", ""))
        if not file_id:
            return

        if msg_type == MSG_FILE_META:
            meta = {
                "file_id": file_id,
                "filename": sanitize_filename(str(pkt.get("filename", "file"))),
                "size": int(pkt.get("size", 0)),
                "mime_type": str(pkt.get("mime_type", "application/octet-stream")),
                "is_image": bool(pkt.get("is_image", False)),
                "sha256": str(pkt.get("sha256", "")),
                "total_chunks": max(1, int(pkt.get("total_chunks", 1))),
                "sender": str(pkt.get("sender", "")),
                "to": pkt.get("to"),
                "room_id": pkt.get("room_id") or DEFAULT_ROOM_ID,
                "ts": pkt.get("ts", ""),
                "chunks": {},
            }
            with self._lock:
                self._incoming_files[file_id] = meta
            self._publish(
                {
                    "type": "FILE_START",
                    "file_id": file_id,
                    "filename": meta["filename"],
                    "size": meta["size"],
                    "mime_type": meta["mime_type"],
                    "is_image": meta["is_image"],
                    "total_chunks": meta["total_chunks"],
                    "sender": meta["sender"],
                    "to": meta["to"],
                    "room_id": meta["room_id"],
                    "ts": meta["ts"],
                }
            )
            return

        if msg_type == MSG_FILE_CHUNK:
            seq = int(pkt.get("seq", -1))
            b64 = pkt.get("data", "")
            with self._lock:
                entry = self._incoming_files.get(file_id)
                if entry is None or seq < 0:
                    return
                try:
                    entry["chunks"][seq] = base64.b64decode(b64)
                except ValueError:
                    return
                received = len(entry["chunks"])
                total = entry["total_chunks"]
                sender = entry["sender"]
                to = entry["to"]
                room_id = entry["room_id"]

            percent = min(100, int(received * 100 / total))
            self._publish(
                {
                    "type": "FILE_PROGRESS",
                    "file_id": file_id,
                    "received_chunks": received,
                    "total_chunks": total,
                    "percent": percent,
                    "sender": sender,
                    "to": to,
                    "room_id": room_id,
                }
            )
            return

        if msg_type == MSG_FILE_END:
            with self._lock:
                entry = self._incoming_files.pop(file_id, None)
            if entry is None:
                return

            chunks = entry["chunks"]
            total = entry["total_chunks"]
            buf = bytearray()
            missing = False
            for seq in range(total):
                part = chunks.get(seq)
                if part is None:
                    missing = True
                    break
                buf.extend(part)

            raw = bytes(buf)
            actual_sha256 = hashlib.sha256(raw).hexdigest()
            expected_sha256 = str(pkt.get("sha256") or entry["sha256"])
            verified = (not missing) and (
                not expected_sha256 or actual_sha256 == expected_sha256
            )

            save_path = Path(self._temp_dir.name) / f"{file_id}_{entry['filename']}"
            save_path.write_bytes(raw)
            record = {
                "file_id": file_id,
                "filename": entry["filename"],
                "size": len(raw),
                "mime_type": entry["mime_type"],
                "is_image": entry["is_image"],
                "sha256": actual_sha256,
                "verified": verified,
                "path": save_path,
            }
            with self._lock:
                self._completed_files[file_id] = record

            self._publish(
                {
                    "type": "FILE_COMPLETE",
                    "file_id": file_id,
                    "filename": entry["filename"],
                    "size": len(raw),
                    "mime_type": entry["mime_type"],
                    "is_image": entry["is_image"],
                    "total_chunks": total,
                    "sha256": actual_sha256,
                    "verified": verified,
                    "url": f"/api/files/{file_id}",
                    "sender": entry["sender"],
                    "to": entry["to"],
                    "room_id": entry["room_id"],
                    "ts": pkt.get("ts") or entry["ts"],
                }
            )

    def _recv_loop(self, sock: socket.socket) -> None:
        while not self._stop_event.is_set():
            try:
                pkt = recv_packet(sock)
            except (ConnectionError, OSError, ProtocolError):
                if not self._stop_event.is_set():
                    self.disconnect(send_logout=False, manual=False)
                    self._publish(
                        {
                            "type": "DISCONNECTED",
                            "reconnecting": self._auto_reconnect,
                            "text": "与服务端的连接意外断开",
                        }
                    )
                    self._trigger_auto_reconnect()
                break

            if pkt is None:
                if not self._stop_event.is_set():
                    self.disconnect(send_logout=False, manual=False)
                    self._publish(
                        {
                            "type": "DISCONNECTED",
                            "reconnecting": self._auto_reconnect,
                            "text": "服务端已关闭连接",
                        }
                    )
                    self._trigger_auto_reconnect()
                break

            msg_type = pkt.get("type")
            if msg_type == MSG_USER_LIST:
                with self._lock:
                    self.users = list(pkt.get("users", []))
                self._publish(pkt)
            elif msg_type == MSG_ROOM_LIST:
                rooms = list(pkt.get("rooms", []))
                with self._lock:
                    self.rooms = rooms
                    self._joined_rooms = {
                        r["room_id"] for r in rooms if r.get("joined")
                    } or {DEFAULT_ROOM_ID}
                self._publish(pkt)
            elif msg_type in (MSG_FILE_META, MSG_FILE_CHUNK, MSG_FILE_END):
                self._handle_file_packet(pkt)
            elif msg_type != MSG_HEARTBEAT:
                self._publish(pkt)

    def _heartbeat_loop(self, sock: socket.socket) -> None:
        while not self._stop_event.wait(10.0):
            with self._lock:
                if self._sock is not sock or not self.connected:
                    break
            try:
                with self._send_lock:
                    send_packet(sock, MSG_HEARTBEAT)
            except OSError:
                break


def create_handler(bridge: WebChatBridge):
    class ChatHTTPHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            return

        def _send_json(self, data: Dict[str, Any], status: int = 200) -> None:
            body = json.dumps(data, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self) -> Dict[str, Any]:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length > 0 else b"{}"
            return json.loads(raw.decode("utf-8"))

        def do_GET(self) -> None:  # noqa: N802
            parsed = urllib.parse.urlsplit(self.path)
            path = parsed.path

            if path in ("/", "/index.html"):
                html = STATIC_INDEX.read_bytes()
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(html)))
                self.end_headers()
                self.wfile.write(html)
                return

            if path == "/api/status":
                self._send_json(
                    {
                        "connected": bridge.connected,
                        "host": bridge.host,
                        "port": bridge.port,
                        "username": bridge.username,
                        "users": bridge.users,
                        "rooms": bridge.rooms,
                    }
                )
                return

            if path == "/api/discover":
                found = bridge.discover()
                self._send_json(found or {"ok": False})
                return

            if path.startswith("/api/files/"):
                file_id = path.removeprefix("/api/files/").strip("/")
                record = bridge.get_completed_file(file_id)
                if record is None or not Path(record["path"]).exists():
                    self.send_error(HTTPStatus.NOT_FOUND)
                    return

                data = Path(record["path"]).read_bytes()
                qs = urllib.parse.parse_qs(parsed.query)
                force_download = qs.get("download", ["0"])[0] == "1"

                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", record["mime_type"])
                self.send_header("Content-Length", str(len(data)))
                if force_download or not record["is_image"]:
                    encoded_name = urllib.parse.quote(record["filename"])
                    self.send_header(
                        "Content-Disposition",
                        f"attachment; filename*=UTF-8''{encoded_name}",
                    )
                self.end_headers()
                self.wfile.write(data)
                return

            if path == "/api/events":
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "keep-alive")
                self.end_headers()

                sub = bridge.subscribe()
                try:
                    while True:
                        try:
                            event = sub.get(timeout=15.0)
                            line = f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                            self.wfile.write(line.encode("utf-8"))
                            self.wfile.flush()
                        except queue.Empty:
                            self.wfile.write(b": keep-alive\n\n")
                            self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass
                finally:
                    bridge.unsubscribe(sub)
                return

            self.send_error(HTTPStatus.NOT_FOUND)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urllib.parse.urlsplit(self.path)
            path = parsed.path

            if path == "/api/connect":
                payload = self._read_json()
                res = bridge.connect(
                    host=str(payload.get("host", "127.0.0.1")).strip(),
                    port=int(payload.get("port", 8888)),
                    username=str(payload.get("username", "")).strip(),
                )
                self._send_json(res)
                return

            if path == "/api/send":
                payload = self._read_json()
                text = str(payload.get("text", "")).strip()
                to = payload.get("to")
                to_str = str(to).strip() if to else None
                room_id = str(payload.get("room_id") or DEFAULT_ROOM_ID).strip()
                msg_id = payload.get("msg_id")
                res = bridge.send_message(
                    text=text,
                    to=to_str,
                    room_id=room_id,
                    msg_id=str(msg_id).strip() if msg_id else None,
                )
                self._send_json(res)
                return

            if path == "/api/read":
                payload = self._read_json()
                msg_ids = payload.get("msg_ids") or []
                res = bridge.mark_read([str(m) for m in msg_ids])
                self._send_json(res)
                return

            if path == "/api/history":
                payload = self._read_json()
                room_id = payload.get("room_id")
                peer = payload.get("peer")
                limit = int(payload.get("limit", 50))
                res = bridge.request_history(
                    room_id=str(room_id).strip() if room_id else None,
                    peer=str(peer).strip() if peer else None,
                    limit=limit,
                )
                self._send_json(res)
                return

            if path == "/api/rooms/create":
                payload = self._read_json()
                res = bridge.create_room(str(payload.get("name", "")).strip())
                self._send_json(res)
                return

            if path == "/api/rooms/join":
                payload = self._read_json()
                res = bridge.join_room(str(payload.get("room_id", "")).strip())
                self._send_json(res)
                return

            if path == "/api/rooms/leave":
                payload = self._read_json()
                res = bridge.leave_room(str(payload.get("room_id", "")).strip())
                self._send_json(res)
                return

            if path == "/api/upload":
                length = int(self.headers.get("Content-Length", "0"))
                if length > MAX_FILE_SIZE:
                    self._send_json(
                        {
                            "ok": False,
                            "error": f"文件超出 {MAX_FILE_SIZE // (1024 * 1024)} MB 限制",
                        },
                        status=HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                    )
                    return

                raw_bytes = self.rfile.read(length) if length > 0 else b""
                filename = urllib.parse.unquote(
                    self.headers.get("X-Filename", "unnamed_file")
                )
                mime_type = self.headers.get("X-Mime-Type") or None
                is_image_hdr = self.headers.get("X-Is-Image", "").lower()
                is_image = True if is_image_hdr == "true" else (False if is_image_hdr == "false" else None)
                to_hdr = urllib.parse.unquote(self.headers.get("X-To", "")).strip()
                to_target = to_hdr or None
                room_id = urllib.parse.unquote(
                    self.headers.get("X-Room-Id", DEFAULT_ROOM_ID)
                ).strip() or DEFAULT_ROOM_ID

                res = bridge.send_file(
                    filename=filename,
                    raw_bytes=raw_bytes,
                    mime_type=mime_type,
                    is_image=is_image,
                    to=to_target,
                    room_id=room_id,
                )
                self._send_json(res)
                return

            if path == "/api/disconnect":
                bridge.disconnect(send_logout=True, manual=True)
                self._send_json({"ok": True})
                return

            self.send_error(HTTPStatus.NOT_FOUND)

    return ChatHTTPHandler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="局域网聊天室现代简约 IM Web 客户端")
    parser.add_argument("--web-host", default="127.0.0.1", help="本地 Web 界面绑定地址 (默认 127.0.0.1)")
    parser.add_argument("--web-port", type=int, default=8080, help="本地 Web 界面端口 (默认 8080)")
    parser.add_argument(
        "--udp-port",
        type=int,
        default=DISCOVERY_PORT,
        help=f"UDP 自动发现端口 (默认 {DISCOVERY_PORT})",
    )
    parser.add_argument("--no-browser", action="store_true", help="启动时不自动打开浏览器")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    bridge = WebChatBridge(udp_port=args.udp_port)
    httpd = ThreadingHTTPServer((args.web_host, args.web_port), create_handler(bridge))
    url = f"http://{args.web_host}:{args.web_port}"
    print(f"[Web 客户端已启动] 请在浏览器访问: {url}")
    if not args.no_browser:
        threading.Timer(0.4, lambda: webbrowser.open(url)).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n正在关闭 Web 客户端...")
    finally:
        bridge.close()
        httpd.server_close()


if __name__ == "__main__":
    main()
