"""局域网聊天室图形界面客户端（基于 Python 标准库 tkinter，零外部依赖）。

运行方式：
    python3 -m chatroom.gui_client
"""

from __future__ import annotations

import queue
import socket
import threading
import tkinter as tk
from tkinter import messagebox, scrolledtext, ttk
from typing import Any, Dict, List, Optional

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


class ChatRoomGUI:
    """基于 Tkinter 的局域网聊天室桌面客户端。"""

    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("局域网聊天室 - 计算机网络课程作业")
        self.root.geometry("820x560")
        self.root.minsize(680, 460)

        self._sock: Optional[socket.socket] = None
        self._send_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._ui_queue: queue.Queue[tuple[str, Any]] = queue.Queue()
        self._username: str = ""
        self._connected: bool = False

        self._build_ui()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(50, self._poll_ui_queue)

    def _build_ui(self) -> None:
        # 顶部连接栏
        top_frame = ttk.Frame(self.root, padding=(10, 8))
        top_frame.pack(fill=tk.X)

        ttk.Label(top_frame, text="服务端 IP:").pack(side=tk.LEFT)
        self.host_var = tk.StringVar(value="127.0.0.1")
        self.host_entry = ttk.Entry(top_frame, textvariable=self.host_var, width=15)
        self.host_entry.pack(side=tk.LEFT, padx=(4, 10))

        ttk.Label(top_frame, text="端口:").pack(side=tk.LEFT)
        self.port_var = tk.StringVar(value="8888")
        self.port_entry = ttk.Entry(top_frame, textvariable=self.port_var, width=6)
        self.port_entry.pack(side=tk.LEFT, padx=(4, 10))

        ttk.Label(top_frame, text="昵称:").pack(side=tk.LEFT)
        self.name_var = tk.StringVar(value="")
        self.name_entry = ttk.Entry(top_frame, textvariable=self.name_var, width=12)
        self.name_entry.pack(side=tk.LEFT, padx=(4, 10))

        self.discover_btn = ttk.Button(
            top_frame, text="UDP 自动发现", command=self._on_discover_clicked
        )
        self.discover_btn.pack(side=tk.LEFT, padx=(0, 6))

        self.connect_btn = ttk.Button(
            top_frame, text="连接", command=self._on_connect_toggle
        )
        self.connect_btn.pack(side=tk.LEFT)

        # 中间主区域：左侧聊天记录 + 右侧在线用户列表
        main_pane = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        main_pane.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 6))

        chat_frame = ttk.Frame(main_pane)
        self.chat_area = scrolledtext.ScrolledText(
            chat_frame,
            wrap=tk.WORD,
            state=tk.DISABLED,
            font=("Microsoft YaHei UI", 10),
            padx=8,
            pady=6,
        )
        self.chat_area.pack(fill=tk.BOTH, expand=True)
        self.chat_area.tag_configure("system", foreground="#666666")
        self.chat_area.tag_configure("error", foreground="#c62828")
        self.chat_area.tag_configure("private", foreground="#1565c0")
        self.chat_area.tag_configure("self_msg", foreground="#2e7d32")
        main_pane.add(chat_frame, weight=4)

        user_frame = ttk.Frame(main_pane, width=180)
        self.users_label = ttk.Label(user_frame, text="在线用户 (0)")
        self.users_label.pack(anchor=tk.W, pady=(0, 4))

        self.user_listbox = tk.Listbox(
            user_frame,
            font=("Microsoft YaHei UI", 10),
            activestyle="dotbox",
        )
        self.user_listbox.pack(fill=tk.BOTH, expand=True)
        self.user_listbox.bind("<Double-Button-1>", self._on_user_double_click)

        ttk.Label(
            user_frame,
            text="双击用户可快速 @私聊",
            foreground="#666666",
            font=("Microsoft YaHei UI", 8),
        ).pack(anchor=tk.W, pady=(4, 0))
        main_pane.add(user_frame, weight=1)

        # 底部输入栏
        bottom_frame = ttk.Frame(self.root, padding=(10, 0, 10, 8))
        bottom_frame.pack(fill=tk.X)

        self.msg_var = tk.StringVar()
        self.msg_entry = ttk.Entry(bottom_frame, textvariable=self.msg_var)
        self.msg_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 8))
        self.msg_entry.bind("<Return>", lambda _e: self._on_send())
        self.msg_entry.configure(state=tk.DISABLED)

        self.send_btn = ttk.Button(
            bottom_frame, text="发送", command=self._on_send, state=tk.DISABLED
        )
        self.send_btn.pack(side=tk.LEFT)

        # 状态栏
        self.status_var = tk.StringVar(value="未连接 — 可先点击「UDP 自动发现」或输入 IP 后点击「连接」")
        status_bar = ttk.Label(
            self.root,
            textvariable=self.status_var,
            relief=tk.SUNKEN,
            anchor=tk.W,
            padding=(8, 3),
        )
        status_bar.pack(fill=tk.X, side=tk.BOTTOM)

        self._append_chat(
            "欢迎使用局域网聊天室！请先启动服务端 (python3 -m chatroom.server)，再点击「连接」。\n",
            "system",
        )

    def _append_chat(self, text: str, tag: Optional[str] = None) -> None:
        self.chat_area.configure(state=tk.NORMAL)
        if tag:
            self.chat_area.insert(tk.END, text, tag)
        else:
            self.chat_area.insert(tk.END, text)
        self.chat_area.see(tk.END)
        self.chat_area.configure(state=tk.DISABLED)

    def _update_user_list(self, users: List[str]) -> None:
        self.user_listbox.delete(0, tk.END)
        for user in users:
            display = f"{user} (我)" if user == self._username else user
            self.user_listbox.insert(tk.END, display)
        self.users_label.configure(text=f"在线用户 ({len(users)})")

    def _set_connected_state(self, connected: bool) -> None:
        self._connected = connected
        input_state = tk.DISABLED if connected else tk.NORMAL
        chat_state = tk.NORMAL if connected else tk.DISABLED

        self.host_entry.configure(state=input_state)
        self.port_entry.configure(state=input_state)
        self.name_entry.configure(state=input_state)
        self.discover_btn.configure(state=input_state)
        self.connect_btn.configure(text="断开" if connected else "连接")
        self.msg_entry.configure(state=chat_state)
        self.send_btn.configure(state=chat_state)

        if connected:
            self.msg_entry.focus_set()
        else:
            self._update_user_list([])

    def _on_discover_clicked(self) -> None:
        self.discover_btn.configure(state=tk.DISABLED)
        self.status_var.set("正在通过 UDP 广播寻找局域网内的聊天室服务端...")

        def worker() -> None:
            found = discover_server(udp_port=DISCOVERY_PORT, timeout=2.0)
            self._ui_queue.put(("discover_result", found))

        threading.Thread(target=worker, daemon=True).start()

    def _on_connect_toggle(self) -> None:
        if self._connected:
            self._disconnect(send_logout=True)
            return

        host = self.host_var.get().strip()
        port_str = self.port_var.get().strip()
        username = self.name_var.get().strip()

        if not host:
            messagebox.showwarning("提示", "请输入服务端 IP 地址")
            return
        try:
            port = int(port_str)
            if not (1 <= port <= 65535):
                raise ValueError
        except ValueError:
            messagebox.showwarning("提示", "端口必须为 1~65535 的整数")
            return
        if not username or len(username) > 20 or any(ch.isspace() for ch in username):
            messagebox.showwarning("提示", "昵称须为 1~20 个非空白字符")
            return

        self.connect_btn.configure(state=tk.DISABLED)
        self.status_var.set(f"正在连接 {host}:{port} ...")

        def connect_worker() -> None:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(8.0)
            try:
                sock.connect((host, port))
                send_packet(sock, MSG_LOGIN, username=username)
                ack = recv_packet(sock)
            except (OSError, ProtocolError) as exc:
                sock.close()
                self._ui_queue.put(("connect_failed", f"连接失败: {exc}"))
                return

            if not ack or ack.get("type") != MSG_LOGIN_ACK or not ack.get("ok"):
                reason = ack.get("reason", "服务端无响应") if ack else "服务端无响应"
                sock.close()
                self._ui_queue.put(("connect_failed", f"登录失败: {reason}"))
                return

            sock.settimeout(None)
            self._ui_queue.put(
                ("connect_ok", (sock, host, port, username, list(ack.get("users", []))))
            )

        threading.Thread(target=connect_worker, daemon=True).start()

    def _safe_send(self, msg_type: str, **payload: object) -> bool:
        if self._sock is None:
            return False
        with self._send_lock:
            try:
                send_packet(self._sock, msg_type, **payload)
                return True
            except OSError:
                return False

    def _recv_loop(self, sock: socket.socket) -> None:
        while not self._stop_event.is_set():
            try:
                pkt = recv_packet(sock)
            except (ConnectionError, OSError, ProtocolError):
                if not self._stop_event.is_set():
                    self._ui_queue.put(("disconnected", "与服务端的连接已断开"))
                break

            if pkt is None:
                if not self._stop_event.is_set():
                    self._ui_queue.put(("disconnected", "服务端已关闭连接"))
                break

            self._ui_queue.put(("packet", pkt))

    def _heartbeat_loop(self) -> None:
        while not self._stop_event.wait(10.0):
            if not self._safe_send(MSG_HEARTBEAT):
                break

    def _on_send(self) -> None:
        if not self._connected:
            return
        raw = self.msg_var.get().strip()
        if not raw:
            return
        self.msg_var.set("")

        if raw.startswith("@"):
            parts = raw[1:].split(maxsplit=1)
            if len(parts) < 2:
                self._append_chat("[提示] 私聊格式：@用户名 消息内容\n", "error")
                return
            self._safe_send(MSG_PRIVATE, to=parts[0], text=parts[1])
            return

        if raw.startswith("/pm "):
            parts = raw[4:].strip().split(maxsplit=1)
            if len(parts) < 2:
                self._append_chat("[提示] 私聊格式：/pm 用户名 消息内容\n", "error")
                return
            self._safe_send(MSG_PRIVATE, to=parts[0], text=parts[1])
            return

        self._safe_send(MSG_CHAT, text=raw)

    def _on_user_double_click(self, _event: tk.Event) -> None:
        if not self._connected:
            return
        selection = self.user_listbox.curselection()
        if not selection:
            return
        label = self.user_listbox.get(selection[0])
        target = label.removesuffix(" (我)")
        if target == self._username:
            return
        self.msg_var.set(f"@{target} ")
        self.msg_entry.focus_set()
        self.msg_entry.icursor(tk.END)

    def _disconnect(self, send_logout: bool = False, notice: Optional[str] = None) -> None:
        if send_logout and self._connected:
            self._safe_send(MSG_LOGOUT)

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

        if self._connected:
            self._set_connected_state(False)
            self.status_var.set("已断开连接")
            self._append_chat(f"[系统] {notice or '已退出聊天室'}\n", "system")

    def _handle_packet(self, pkt: Dict[str, Any]) -> None:
        msg_type = pkt.get("type")
        ts = pkt.get("ts", "")

        if msg_type == MSG_CHAT:
            sender = pkt.get("sender", "")
            text = pkt.get("text", "")
            tag = "self_msg" if sender == self._username else None
            self._append_chat(f"[{ts}] {sender}: {text}\n", tag)
        elif msg_type == MSG_PRIVATE:
            sender = pkt.get("sender", "")
            target = pkt.get("to", "")
            text = pkt.get("text", "")
            if sender == self._username:
                self._append_chat(f"[{ts}] [私聊 -> {target}] {text}\n", "private")
            else:
                self._append_chat(f"[{ts}] [私聊 <- {sender}] {text}\n", "private")
        elif msg_type == MSG_SYSTEM:
            self._append_chat(f"[{ts}] [系统] {pkt.get('text', '')}\n", "system")
        elif msg_type == MSG_USER_LIST:
            self._update_user_list(list(pkt.get("users", [])))
        elif msg_type == MSG_ERROR:
            self._append_chat(f"[提示] {pkt.get('text', '')}\n", "error")

    def _poll_ui_queue(self) -> None:
        while True:
            try:
                event, payload = self._ui_queue.get_nowait()
            except queue.Empty:
                break

            if event == "discover_result":
                self.discover_btn.configure(state=tk.NORMAL if not self._connected else tk.DISABLED)
                if payload is not None:
                    ip, tcp_port, server_name = payload
                    self.host_var.set(ip)
                    self.port_var.set(str(tcp_port))
                    self.status_var.set(f"已发现服务端: {server_name} ({ip}:{tcp_port})")
                    self._append_chat(f"[系统] UDP 自动发现服务端: {server_name} ({ip}:{tcp_port})\n", "system")
                else:
                    self.status_var.set("未发现局域网服务端，请确认服务端已启动或手动输入 IP")
                    self._append_chat("[提示] UDP 自动发现超时，未收到服务端响应。\n", "error")
            elif event == "connect_failed":
                self.connect_btn.configure(state=tk.NORMAL)
                self.status_var.set(str(payload))
                messagebox.showerror("连接失败", str(payload))
            elif event == "connect_ok":
                sock, host, port, username, users = payload
                self._sock = sock
                self._username = username
                self._stop_event.clear()
                self.connect_btn.configure(state=tk.NORMAL)
                self._set_connected_state(True)
                self._update_user_list(users)
                self.status_var.set(f"已连接 {host}:{port} | 当前用户: {username}")
                self._append_chat(f"[系统] 成功连接到 {host}:{port}，欢迎 {username}！\n", "system")

                threading.Thread(target=self._recv_loop, args=(sock,), daemon=True).start()
                threading.Thread(target=self._heartbeat_loop, daemon=True).start()
            elif event == "packet":
                self._handle_packet(payload)
            elif event == "disconnected":
                self._disconnect(send_logout=False, notice=str(payload))

        self.root.after(50, self._poll_ui_queue)

    def _on_close(self) -> None:
        self._disconnect(send_logout=True)
        self.root.destroy()


def main() -> None:
    root = tk.Tk()
    ChatRoomGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
