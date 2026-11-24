"""局域网聊天室玻璃拟态 Web 界面客户端（零第三方依赖，底层仍走自定义 TCP/UDP 协议）。

运行方式：
    python3 -m chatroom.web_client --web-port 8080
启动后会自动在浏览器打开极简玻璃拟态界面，并通过自定义 TCP 协议连接 chatroom.server。
"""

from __future__ import annotations

import argparse
import json
import queue
import socket
import threading
import webbrowser
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional

from chatroom.discovery import DISCOVERY_PORT, discover_server
from chatroom.protocol import (
    MSG_CHAT,
    MSG_HEARTBEAT,
    MSG_LOGIN,
    MSG_LOGIN_ACK,
    MSG_LOGOUT,
    MSG_PRIVATE,
    MSG_USER_LIST,
    ProtocolError,
    recv_packet,
    send_packet,
)

STATIC_INDEX = Path(__file__).resolve().parent / "web" / "index.html"


class WebChatBridge:
    """将浏览器 HTTP/SSE 请求桥接到原生 TCP 局域网聊天室服务端。"""

    def __init__(self, udp_port: int = DISCOVERY_PORT) -> None:
        self.udp_port = udp_port
        self._lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._sock: Optional[socket.socket] = None
        self._stop_event = threading.Event()
        self._subscribers: List[queue.Queue[Dict[str, Any]]] = []

        self.connected: bool = False
        self.host: str = ""
        self.port: int = 8888
        self.username: str = ""
        self.users: List[str] = []

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

    def connect(self, host: str, port: int, username: str) -> Dict[str, Any]:
        self.disconnect(send_logout=True)

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

        threading.Thread(target=self._recv_loop, args=(sock,), daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, args=(sock,), daemon=True).start()
        return {
            "ok": True,
            "username": self.username,
            "users": self.users,
            "host": host,
            "port": port,
        }

    def disconnect(self, send_logout: bool = False) -> None:
        with self._lock:
            sock = self._sock
            was_connected = self.connected
            self._sock = None
            self.connected = False
            self._stop_event.set()

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

    def send_message(self, text: str, to: Optional[str] = None) -> Dict[str, Any]:
        with self._lock:
            sock = self._sock
            connected = self.connected
        if not connected or sock is None:
            return {"ok": False, "error": "尚未连接到服务端"}
        try:
            with self._send_lock:
                if to:
                    send_packet(sock, MSG_PRIVATE, to=to, text=text)
                else:
                    send_packet(sock, MSG_CHAT, text=text)
            return {"ok": True}
        except OSError as exc:
            return {"ok": False, "error": str(exc)}

    def _recv_loop(self, sock: socket.socket) -> None:
        while not self._stop_event.is_set():
            try:
                pkt = recv_packet(sock)
            except (ConnectionError, OSError, ProtocolError):
                if not self._stop_event.is_set():
                    self.disconnect(send_logout=False)
                    self._publish({"type": "DISCONNECTED", "text": "与服务端的连接已断开"})
                break

            if pkt is None:
                if not self._stop_event.is_set():
                    self.disconnect(send_logout=False)
                    self._publish({"type": "DISCONNECTED", "text": "服务端已关闭连接"})
                break

            if pkt.get("type") == MSG_USER_LIST:
                with self._lock:
                    self.users = list(pkt.get("users", []))
            if pkt.get("type") != MSG_HEARTBEAT:
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
            if self.path in ("/", "/index.html"):
                html = STATIC_INDEX.read_bytes()
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(html)))
                self.end_headers()
                self.wfile.write(html)
                return

            if self.path == "/api/status":
                self._send_json(
                    {
                        "connected": bridge.connected,
                        "host": bridge.host,
                        "port": bridge.port,
                        "username": bridge.username,
                        "users": bridge.users,
                    }
                )
                return

            if self.path == "/api/discover":
                found = bridge.discover()
                self._send_json(found or {"ok": False})
                return

            if self.path == "/api/events":
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
            if self.path == "/api/connect":
                payload = self._read_json()
                res = bridge.connect(
                    host=str(payload.get("host", "127.0.0.1")).strip(),
                    port=int(payload.get("port", 8888)),
                    username=str(payload.get("username", "")).strip(),
                )
                self._send_json(res)
                return

            if self.path == "/api/send":
                payload = self._read_json()
                text = str(payload.get("text", "")).strip()
                to = payload.get("to")
                to_str = str(to).strip() if to else None
                res = bridge.send_message(text=text, to=to_str)
                self._send_json(res)
                return

            if self.path == "/api/disconnect":
                bridge.disconnect(send_logout=True)
                self._send_json({"ok": True})
                return

            self.send_error(HTTPStatus.NOT_FOUND)

    return ChatHTTPHandler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="局域网聊天室玻璃拟态 Web 客户端")
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
        bridge.disconnect(send_logout=True)
        httpd.server_close()


if __name__ == "__main__":
    main()
