"""局域网聊天室命令行客户端（支持 UDP 自动发现、群聊、@私聊、在线列表与心跳保活）。

运行方式：
    # 自动发现局域网服务端并连接
    python3 -m chatroom.client --name Alice

    # 手动指定服务端 IP 和端口
    python3 -m chatroom.client --host 192.168.1.100 --port 8888 --name Bob
"""

from __future__ import annotations

import argparse
import socket
import sys
import threading
from typing import List, Optional

from chatroom.discovery import DISCOVERY_PORT, discover_server
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
    recv_packet,
    send_packet,
)

HELP_TEXT = """
可用命令：
  直接输入文字          发送群聊消息
  @用户名 消息内容      发送私聊消息（例如: @Bob 你好）
  /pm 用户名 消息内容   发送私聊消息
  /users               查看当前在线用户列表
  /help                显示帮助信息
  /quit                退出聊天室
""".strip()


class ChatClient:
    """局域网聊天室 TCP 客户端。"""

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        heartbeat_interval: float = 10.0,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.heartbeat_interval = heartbeat_interval

        self._sock: Optional[socket.socket] = None
        self._send_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._online_users: List[str] = []

    def _safe_send(self, msg_type: str, **payload: object) -> bool:
        if self._sock is None:
            return False
        with self._send_lock:
            try:
                send_packet(self._sock, msg_type, **payload)
                return True
            except OSError:
                return False

    def connect(self) -> bool:
        """建立 TCP 连接并完成 LOGIN 握手。"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10.0)
        try:
            sock.connect((self.host, self.port))
            send_packet(sock, MSG_LOGIN, username=self.username)
            ack = recv_packet(sock)
        except (OSError, ProtocolError) as exc:
            print(f"[错误] 无法连接到服务端 {self.host}:{self.port}: {exc}")
            sock.close()
            return False

        if not ack or ack.get("type") != MSG_LOGIN_ACK or not ack.get("ok"):
            reason = ack.get("reason", "未知原因") if ack else "服务端无响应"
            print(f"[登录失败] {reason}")
            sock.close()
            return False

        sock.settimeout(None)
        self._sock = sock
        self._online_users = list(ack.get("users", []))
        print(
            f"[已连接] 服务端 {self.host}:{self.port} | 昵称: {self.username} | "
            f"当前在线({len(self._online_users)}): {', '.join(self._online_users)}"
        )
        print("输入 /help 查看命令，输入 /quit 退出。\n")
        return True

    def _print_line(self, line: str) -> None:
        """避免后台接收消息直接粘在输入提示符后。"""
        sys.stdout.write(f"\r{line}\n> ")
        sys.stdout.flush()

    def _recv_loop(self) -> None:
        assert self._sock is not None
        while not self._stop_event.is_set():
            try:
                pkt = recv_packet(self._sock)
            except (ConnectionError, OSError, ProtocolError):
                if not self._stop_event.is_set():
                    self._print_line("[系统] 与服务端的连接已断开。")
                self._stop_event.set()
                break

            if pkt is None:
                if not self._stop_event.is_set():
                    self._print_line("[系统] 服务端已关闭连接。")
                self._stop_event.set()
                break

            msg_type = pkt.get("type")
            ts = pkt.get("ts", "")

            if msg_type == MSG_CHAT:
                self._print_line(f"[{ts}] {pkt.get('sender')}: {pkt.get('text')}")
            elif msg_type == MSG_PRIVATE:
                sender = pkt.get("sender")
                target = pkt.get("to")
                text = pkt.get("text")
                if sender == self.username:
                    self._print_line(f"[{ts}] [私聊 -> {target}] {text}")
                else:
                    self._print_line(f"[{ts}] [私聊 <- {sender}] {text}")
            elif msg_type == MSG_SYSTEM:
                self._print_line(f"[{ts}] [系统] {pkt.get('text')}")
            elif msg_type == MSG_USER_LIST:
                self._online_users = list(pkt.get("users", []))
            elif msg_type == MSG_ERROR:
                self._print_line(f"[提示] {pkt.get('text')}")
            elif msg_type == MSG_HEARTBEAT:
                continue

    def _heartbeat_loop(self) -> None:
        while not self._stop_event.wait(self.heartbeat_interval):
            if not self._safe_send(MSG_HEARTBEAT):
                self._stop_event.set()
                break

    def run(self) -> None:
        """启动接收/心跳线程并进入命令行交互循环。"""
        if not self.connect():
            return

        recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        hb_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        recv_thread.start()
        hb_thread.start()

        try:
            while not self._stop_event.is_set():
                try:
                    raw = input("> ").strip()
                except (EOFError, KeyboardInterrupt):
                    print()
                    break

                if self._stop_event.is_set():
                    break
                if not raw:
                    continue

                if raw == "/quit":
                    self._safe_send(MSG_LOGOUT)
                    break
                if raw == "/help":
                    print(HELP_TEXT)
                    continue
                if raw == "/users":
                    self._safe_send(MSG_USER_LIST)
                    print(
                        f"[在线用户({len(self._online_users)})] "
                        f"{', '.join(self._online_users)}"
                    )
                    continue

                if raw.startswith("@"):
                    parts = raw[1:].split(maxsplit=1)
                    if len(parts) < 2:
                        print("[提示] 私聊格式：@用户名 消息内容")
                        continue
                    self._safe_send(MSG_PRIVATE, to=parts[0], text=parts[1])
                    continue

                if raw.startswith("/pm "):
                    parts = raw[4:].strip().split(maxsplit=1)
                    if len(parts) < 2:
                        print("[提示] 私聊格式：/pm 用户名 消息内容")
                        continue
                    self._safe_send(MSG_PRIVATE, to=parts[0], text=parts[1])
                    continue

                self._safe_send(MSG_CHAT, text=raw)
        finally:
            self.close()

    def close(self) -> None:
        self._stop_event.set()
        if self._sock is not None:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
        print("[已退出聊天室]")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="局域网聊天室客户端")
    parser.add_argument("--host", default=None, help="服务端 IP 地址（不填则优先 UDP 自动发现）")
    parser.add_argument("--port", type=int, default=8888, help="服务端 TCP 端口 (默认 8888)")
    parser.add_argument(
        "--udp-port",
        type=int,
        default=DISCOVERY_PORT,
        help=f"UDP 自动发现端口 (默认 {DISCOVERY_PORT})",
    )
    parser.add_argument("--name", default=None, help="聊天室昵称")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    host = args.host
    port = args.port

    if host is None:
        print(f"[发现中] 正在局域网广播寻找聊天室服务端 (UDP:{args.udp_port})...")
        found = discover_server(udp_port=args.udp_port, timeout=2.0)
        if found is not None:
            host, port, server_name = found
            print(f"[发现成功] {server_name} -> {host}:{port}")
        else:
            host = "127.0.0.1"
            print(f"[未发现广播响应] 回退连接默认地址 {host}:{port}")

    username = args.name
    while not username:
        username = input("请输入昵称（1~20 个非空白字符）: ").strip()

    client = ChatClient(host=host, port=port, username=username)
    client.run()


if __name__ == "__main__":
    main()
