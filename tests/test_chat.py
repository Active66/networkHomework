"""局域网聊天室端到端集成测试。

运行方式：
    python3 -m unittest discover -s tests -v
"""

from __future__ import annotations

import socket
import time
import unittest
from typing import Any, Dict, List

from chatroom.discovery import discover_server
from chatroom.protocol import (
    MSG_CHAT,
    MSG_HEARTBEAT,
    MSG_LOGIN,
    MSG_LOGIN_ACK,
    MSG_LOGOUT,
    MSG_PRIVATE,
    MSG_SYSTEM,
    MSG_USER_LIST,
    encode_packet,
    recv_packet,
    send_packet,
)
from chatroom.server import ChatServer


def connect_and_login(port: int, username: str) -> tuple[socket.socket, Dict[str, Any]]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(3.0)
    sock.connect(("127.0.0.1", port))
    send_packet(sock, MSG_LOGIN, username=username)
    ack = recv_packet(sock)
    assert ack is not None
    return sock, ack


def recv_until(
    sock: socket.socket,
    predicate,
    timeout: float = 3.0,
) -> Dict[str, Any]:
    """在超时时间内循环读取报文，直到满足 predicate 条件。"""
    deadline = time.monotonic() + timeout
    seen: List[Dict[str, Any]] = []
    while time.monotonic() < deadline:
        sock.settimeout(max(0.1, deadline - time.monotonic()))
        pkt = recv_packet(sock)
        if pkt is None:
            raise AssertionError(f"连接提前关闭，已收到报文: {seen}")
        seen.append(pkt)
        if predicate(pkt):
            return pkt
    raise AssertionError(f"等待目标报文超时，已收到报文: {seen}")


class ChatRoomIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        # 使用端口 0 让操作系统自动分配空闲端口，避免端口冲突
        self.server = ChatServer(
            host="127.0.0.1",
            port=0,
            udp_discovery_port=0,
            heartbeat_timeout=15.0,
            server_name="Test-ChatRoom",
        )
        self.server.start(blocking=False)
        self.sockets: List[socket.socket] = []

    def tearDown(self) -> None:
        for sock in self.sockets:
            try:
                sock.close()
            except OSError:
                pass
        self.server.stop()

    def _login(self, username: str) -> tuple[socket.socket, Dict[str, Any]]:
        sock, ack = connect_and_login(self.server.bound_tcp_port, username)
        self.sockets.append(sock)
        return sock, ack

    def test_udp_discovery(self) -> None:
        assert self.server.bound_udp_port is not None
        found = discover_server(
            udp_port=self.server.bound_udp_port,
            timeout=2.0,
            broadcast_addr="127.0.0.1",
        )
        self.assertIsNotNone(found)
        assert found is not None
        _, tcp_port, name = found
        self.assertEqual(tcp_port, self.server.bound_tcp_port)
        self.assertEqual(name, "Test-ChatRoom")

    def test_duplicate_username_rejected(self) -> None:
        _, ack1 = self._login("Alice")
        self.assertEqual(ack1["type"], MSG_LOGIN_ACK)
        self.assertTrue(ack1["ok"])

        _, ack2 = self._login("Alice")
        self.assertEqual(ack2["type"], MSG_LOGIN_ACK)
        self.assertFalse(ack2["ok"])
        self.assertIn("占用", ack2["reason"])

    def test_group_chat_and_private_chat_isolation(self) -> None:
        alice, _ = self._login("Alice")
        bob, _ = self._login("Bob")
        charlie, _ = self._login("Charlie")

        # 群聊：三个人都应收到
        send_packet(alice, MSG_CHAT, text="大家好")
        for sock in (alice, bob, charlie):
            pkt = recv_until(
                sock,
                lambda p: p.get("type") == MSG_CHAT and p.get("text") == "大家好",
            )
            self.assertEqual(pkt["sender"], "Alice")

        # 私聊：只有 Alice 和 Bob 收到，Charlie 不应收到
        send_packet(alice, MSG_PRIVATE, to="Bob", text="悄悄话")
        bob_pm = recv_until(
            bob,
            lambda p: p.get("type") == MSG_PRIVATE and p.get("text") == "悄悄话",
        )
        self.assertEqual(bob_pm["sender"], "Alice")
        self.assertEqual(bob_pm["to"], "Bob")

        # 发送一条心跳给 Charlie，验证期间未收到私聊
        send_packet(charlie, MSG_HEARTBEAT)
        hb = recv_until(charlie, lambda p: p.get("type") == MSG_HEARTBEAT)
        self.assertEqual(hb["type"], MSG_HEARTBEAT)

    def test_tcp_sticky_packets(self) -> None:
        """将 3 个完整报文拼接后一次性 sendall，验证服务端能正确拆包。"""
        alice, _ = self._login("Alice")
        bob, _ = self._login("Bob")

        sticky_payload = (
            encode_packet(MSG_CHAT, text="msg-1")
            + encode_packet(MSG_CHAT, text="msg-2")
            + encode_packet(MSG_CHAT, text="msg-3")
        )
        alice.sendall(sticky_payload)

        received_texts = []
        for expected in ("msg-1", "msg-2", "msg-3"):
            pkt = recv_until(
                bob,
                lambda p, exp=expected: p.get("type") == MSG_CHAT
                and p.get("text") == exp,
            )
            received_texts.append(pkt["text"])
        self.assertEqual(received_texts, ["msg-1", "msg-2", "msg-3"])

    def test_logout_updates_user_list(self) -> None:
        alice, _ = self._login("Alice")
        bob, _ = self._login("Bob")

        send_packet(bob, MSG_LOGOUT)
        leave_notice = recv_until(
            alice,
            lambda p: p.get("type") == MSG_SYSTEM and "Bob" in p.get("text", "") and "离开" in p.get("text", ""),
        )
        self.assertIn("Bob", leave_notice["text"])

        user_list_pkt = recv_until(
            alice,
            lambda p: p.get("type") == MSG_USER_LIST and p.get("users") == ["Alice"],
        )
        self.assertEqual(user_list_pkt["users"], ["Alice"])


if __name__ == "__main__":
    unittest.main()
