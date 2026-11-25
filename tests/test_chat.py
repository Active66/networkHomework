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
            db_path=":memory:",
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

    def test_web_bridge_connect_and_chat(self) -> None:
        from chatroom.web_client import WebChatBridge

        assert self.server.bound_udp_port is not None
        bridge = WebChatBridge(udp_port=self.server.bound_udp_port)
        sub = bridge.subscribe()
        try:
            res = bridge.connect("127.0.0.1", self.server.bound_tcp_port, "WebUser")
            self.assertTrue(res["ok"])
            self.assertEqual(res["username"], "WebUser")

            send_res = bridge.send_message("hello from glass ui")
            self.assertTrue(send_res["ok"])

            received = None
            for _ in range(5):
                pkt = sub.get(timeout=2.0)
                if pkt.get("type") == MSG_CHAT and pkt.get("text") == "hello from glass ui":
                    received = pkt
                    break
            self.assertIsNotNone(received)
            assert received is not None
            self.assertEqual(received["sender"], "WebUser")
        finally:
            bridge.unsubscribe(sub)
            bridge.close()

    def test_tcp_chunked_file_and_image_transfer(self) -> None:
        """验证大于 48 KB 的多块二进制文件/图片经 TCP 分块传输、重组及 SHA-256 校验，并验证私聊隔离。"""
        import hashlib
        from pathlib import Path
        from chatroom.protocol import FILE_CHUNK_SIZE
        from chatroom.web_client import WebChatBridge

        assert self.server.bound_udp_port is not None
        alice = WebChatBridge(udp_port=self.server.bound_udp_port)
        bob = WebChatBridge(udp_port=self.server.bound_udp_port)
        charlie = WebChatBridge(udp_port=self.server.bound_udp_port)
        sub_alice = alice.subscribe()
        sub_bob = bob.subscribe()
        sub_charlie = charlie.subscribe()

        try:
            self.assertTrue(alice.connect("127.0.0.1", self.server.bound_tcp_port, "Alice")["ok"])
            self.assertTrue(bob.connect("127.0.0.1", self.server.bound_tcp_port, "Bob")["ok"])
            self.assertTrue(charlie.connect("127.0.0.1", self.server.bound_tcp_port, "Charlie")["ok"])

            # 1. 群聊发送一个 130 KB 的多块文件（应拆为 3 个 48 KB TCP 分块）
            raw_file = bytes((i * 37) % 256 for i in range(FILE_CHUNK_SIZE * 2 + 12345))
            expected_hash = hashlib.sha256(raw_file).hexdigest()
            tx = alice.send_file("lab_report.bin", raw_file, mime_type="application/octet-stream", is_image=False)
            self.assertTrue(tx["ok"])
            self.assertEqual(tx["total_chunks"], 3)

            def wait_complete(sub, target_file_id: str) -> Dict[str, Any]:
                deadline = time.monotonic() + 5.0
                progress_events = 0
                while time.monotonic() < deadline:
                    ev = sub.get(timeout=max(0.1, deadline - time.monotonic()))
                    if ev.get("type") == "FILE_PROGRESS" and ev.get("file_id") == target_file_id:
                        progress_events += 1
                    if ev.get("type") == "FILE_COMPLETE" and ev.get("file_id") == target_file_id:
                        ev["_progress_count"] = progress_events
                        return ev
                raise AssertionError("等待 FILE_COMPLETE 超时")

            bob_done = wait_complete(sub_bob, tx["file_id"])
            self.assertTrue(bob_done["verified"])
            self.assertEqual(bob_done["sha256"], expected_hash)
            self.assertEqual(bob_done["size"], len(raw_file))
            self.assertEqual(bob_done["_progress_count"], 3)

            saved = bob.get_completed_file(tx["file_id"])
            self.assertIsNotNone(saved)
            assert saved is not None
            self.assertEqual(Path(saved["path"]).read_bytes(), raw_file)

            # 2. 私聊发送一张图片给 Bob，验证 Charlie 不会收到
            fake_png = b"\x89PNG\r\n\x1a\n" + b"imgdata" * 1000
            img_tx = alice.send_file("demo.png", fake_png, mime_type="image/png", is_image=True, to="Bob")
            self.assertTrue(img_tx["ok"])

            bob_img = wait_complete(sub_bob, img_tx["file_id"])
            self.assertTrue(bob_img["verified"])
            self.assertTrue(bob_img["is_image"])
            self.assertEqual(bob_img["to"], "Bob")

            # 发送一条群聊哨兵消息，确认 Charlie 期间没有收到私聊图片
            self.assertTrue(alice.send_message("sentinel")["ok"])
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline:
                ev = sub_charlie.get(timeout=max(0.1, deadline - time.monotonic()))
                self.assertNotEqual(ev.get("file_id"), img_tx["file_id"])
                if ev.get("type") == MSG_CHAT and ev.get("text") == "sentinel":
                    break
        finally:
            alice.unsubscribe(sub_alice)
            bob.unsubscribe(sub_bob)
            charlie.unsubscribe(sub_charlie)
            alice.close()
            bob.close()
            charlie.close()

    def test_multi_room_isolation(self) -> None:
        """验证自定义多房间创建、加入与消息隔离（未加入者不会收到房间消息）。"""
        from chatroom.protocol import MSG_ROOM_CREATE, MSG_ROOM_JOIN, MSG_ROOM_LIST

        alice, _ = self._login("Alice")
        bob, _ = self._login("Bob")
        charlie, _ = self._login("Charlie")

        send_packet(alice, MSG_ROOM_CREATE, name="计网实验组")
        room_list_pkt = recv_until(
            bob,
            lambda p: p.get("type") == MSG_ROOM_LIST
            and any(r["name"] == "计网实验组" for r in p.get("rooms", [])),
        )
        custom_room = next(r for r in room_list_pkt["rooms"] if r["name"] == "计网实验组")
        room_id = custom_room["room_id"]

        # Bob 加入房间，Charlie 不加入
        send_packet(bob, MSG_ROOM_JOIN, room_id=room_id)
        recv_until(
            bob,
            lambda p: p.get("type") == MSG_ROOM_LIST
            and any(r["room_id"] == room_id and r["joined"] for r in p.get("rooms", [])),
        )

        send_packet(alice, MSG_CHAT, room_id=room_id, text="实验组内部消息")
        bob_msg = recv_until(
            bob,
            lambda p: p.get("type") == MSG_CHAT and p.get("text") == "实验组内部消息",
        )
        self.assertEqual(bob_msg["room_id"], room_id)

        # 用心跳哨兵确认 Charlie 未收到实验组消息
        send_packet(charlie, MSG_HEARTBEAT)
        hb = recv_until(charlie, lambda p: p.get("type") == MSG_HEARTBEAT)
        self.assertEqual(hb["type"], MSG_HEARTBEAT)

    def test_ack_and_read_receipts(self) -> None:
        """验证消息送达回执（MSG_ACK）与已读回执（MSG_READ）。"""
        from chatroom.protocol import MSG_ACK, MSG_READ

        alice, _ = self._login("Alice")
        bob, _ = self._login("Bob")

        send_packet(alice, MSG_PRIVATE, to="Bob", text="请查收", msg_id="msg-ack-001")
        ack = recv_until(
            alice,
            lambda p: p.get("type") == MSG_ACK and p.get("msg_id") == "msg-ack-001",
        )
        self.assertEqual(ack["status"], "delivered")

        bob_pkt = recv_until(
            bob,
            lambda p: p.get("type") == MSG_PRIVATE and p.get("msg_id") == "msg-ack-001",
        )
        send_packet(bob, MSG_READ, msg_ids=[bob_pkt["msg_id"]])

        read_pkt = recv_until(
            alice,
            lambda p: p.get("type") == MSG_READ and p.get("msg_id") == "msg-ack-001",
        )
        self.assertEqual(read_pkt["reader"], "Bob")

    def test_sqlite_persistence_and_history_sync(self) -> None:
        """验证 SQLite 持久化在服务端重启后仍能同步历史消息与已读记录。"""
        import tempfile
        from pathlib import Path
        from chatroom.protocol import DEFAULT_ROOM_ID, MSG_HISTORY_REQ, MSG_HISTORY_RESP, MSG_READ

        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = str(Path(tmpdir) / "persist.db")

            # 第一次启动服务端并发送消息
            srv1 = ChatServer(host="127.0.0.1", port=0, udp_discovery_port=None, db_path=db_file)
            srv1.start(blocking=False)
            try:
                s_alice, _ = connect_and_login(srv1.bound_tcp_port, "Alice")
                s_bob, _ = connect_and_login(srv1.bound_tcp_port, "Bob")
                try:
                    send_packet(s_alice, MSG_CHAT, room_id=DEFAULT_ROOM_ID, text="持久化测试消息", msg_id="persist-1")
                    recv_until(s_bob, lambda p: p.get("type") == MSG_CHAT and p.get("msg_id") == "persist-1")
                    send_packet(s_bob, MSG_READ, msg_ids=["persist-1"])
                    recv_until(s_alice, lambda p: p.get("type") == MSG_READ and p.get("msg_id") == "persist-1")
                finally:
                    s_alice.close()
                    s_bob.close()
            finally:
                srv1.stop()

            # 重启新服务端实例，验证历史消息和已读回执完整保留
            srv2 = ChatServer(host="127.0.0.1", port=0, udp_discovery_port=None, db_path=db_file)
            srv2.start(blocking=False)
            try:
                s_alice2, _ = connect_and_login(srv2.bound_tcp_port, "Alice")
                try:
                    send_packet(s_alice2, MSG_HISTORY_REQ, room_id=DEFAULT_ROOM_ID, limit=20)
                    hist = recv_until(s_alice2, lambda p: p.get("type") == MSG_HISTORY_RESP)
                    msgs = hist["messages"]
                    self.assertEqual(len(msgs), 1)
                    self.assertEqual(msgs[0]["msg_id"], "persist-1")
                    self.assertEqual(msgs[0]["text"], "持久化测试消息")
                    self.assertIn("Bob", msgs[0]["read_by"])
                finally:
                    s_alice2.close()
            finally:
                srv2.stop()

    def test_auto_reconnect_after_server_restart(self) -> None:
        """验证服务端中断重启后，WebChatBridge 自动重连并恢复在线状态。"""
        from chatroom.web_client import WebChatBridge

        srv1 = ChatServer(host="127.0.0.1", port=0, udp_discovery_port=None, db_path=":memory:")
        srv1.start(blocking=False)
        tcp_port = srv1.bound_tcp_port

        bridge = WebChatBridge(udp_port=0, reconnect_base_delay=0.15)
        sub = bridge.subscribe()
        srv2 = None
        try:
            res = bridge.connect("127.0.0.1", tcp_port, "ReconnectUser")
            self.assertTrue(res["ok"])

            # 模拟服务端意外关闭
            srv1.stop()

            # 等待进入自动重连状态
            deadline = time.monotonic() + 3.0
            saw_reconnecting = False
            while time.monotonic() < deadline:
                ev = sub.get(timeout=max(0.1, deadline - time.monotonic()))
                if ev.get("type") == "RECONNECTING":
                    saw_reconnecting = True
                    break
            self.assertTrue(saw_reconnecting)

            # 在同一端口重新拉起服务端，验证客户端自动重连成功
            srv2 = ChatServer(host="127.0.0.1", port=tcp_port, udp_discovery_port=None, db_path=":memory:")
            srv2.start(blocking=False)

            deadline = time.monotonic() + 5.0
            saw_reconnected = False
            while time.monotonic() < deadline:
                ev = sub.get(timeout=max(0.1, deadline - time.monotonic()))
                if ev.get("type") == "RECONNECTED":
                    saw_reconnected = True
                    break
            self.assertTrue(saw_reconnected)
            self.assertTrue(bridge.connected)
        finally:
            bridge.unsubscribe(sub)
            bridge.close()
            if srv2 is not None:
                srv2.stop()


if __name__ == "__main__":
    unittest.main()
