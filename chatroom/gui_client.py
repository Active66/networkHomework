"""局域网聊天室现代简约桌面客户端（基于 Python 标准库 tkinter，零外部依赖）。

视觉风格与核心特性：
- 现代简约左右分栏 IM 布局（纯白会话栏 + 柔和浅蓝聊天区 + 左右圆角质感气泡）
- Windows HiDPI 清晰适配（告别老旧模糊字体）
- UDP 局域网广播自动发现 + 独立连接设置弹窗
- 自定义多房间管理、SQLite 历史同步、ACK 送达与已读状态
- 48 KB TCP 分块文件/图片传输、SHA-256 校验与一键保存
- 断线指数退避自动重连

运行方式：
    python3 -m chatroom.gui_client
"""

from __future__ import annotations

import base64
import hashlib
import math
import mimetypes
import os
import platform
import queue
import socket
import tempfile
import threading
import tkinter as tk
import uuid
from pathlib import Path
from tkinter import filedialog, messagebox
from typing import Any, Dict, List, Optional, Set, Tuple

from chatroom.discovery import DISCOVERY_PORT, discover_server
from chatroom.protocol import (
    DEFAULT_ROOM_ID,
    DEFAULT_ROOM_NAME,
    FILE_CHUNK_SIZE,
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

# ---------- 现代简约 IM 配色表 ----------
C_SIDEBAR = "#FFFFFF"
C_CHAT_BG = "#EEF4FF"
C_HEADER_BG = "#F8FAFF"
C_SELECTED = "#E6F2FF"
C_HOVER = "#F4F6FA"
C_INPUT_BG = "#F2F3F5"
C_PRIMARY = "#1677FF"
C_PRIMARY_HOVER = "#0E66E4"
C_TEXT = "#1F2329"
C_MUTED = "#8F959E"
C_BORDER = "#E5EAF2"
C_BUBBLE_PEER = "#FFFFFF"
C_BUBBLE_PEER_BDR = "#DCE6F5"
C_BUBBLE_SELF = "#D6E8FF"
C_BUBBLE_SELF_BDR = "#BBD5FA"
C_ONLINE = "#22C55E"
C_OFFLINE = "#EF4444"
C_WARN_BG = "#FFF7E6"
C_WARN_FG = "#B45309"

AVATAR_COLORS = [
    "#4B8BFF",
    "#3B82F6",
    "#7C63FF",
    "#0EA5E9",
    "#10B981",
    "#EC4899",
    "#6366F1",
]
EMOJIS = ["😊", "😂", "👍", "🎉", "👌", "🚀", "💡", "🔥", "🤝", "✨", "💻", "📡"]


def enable_hidpi() -> None:
    """在 Windows 高分屏下开启 DPI 感知，使 Tkinter 文字和边框清晰锐利。"""
    if platform.system() == "Windows":
        try:
            import ctypes

            ctypes.windll.shcore.SetProcessDpiAwareness(1)
        except Exception:
            pass


def pick_font_family() -> str:
    sys_name = platform.system()
    if sys_name == "Windows":
        return "Microsoft YaHei UI"
    if sys_name == "Darwin":
        return "PingFang SC"
    return "Noto Sans CJK SC"


def avatar_color(name: str, is_lobby: bool = False) -> str:
    if is_lobby:
        return C_PRIMARY
    h = 0
    for ch in name:
        h = (h * 31 + ord(ch)) & 0xFFFFFFFF
    return AVATAR_COLORS[h % len(AVATAR_COLORS)]


def avatar_text(name: str, is_room: bool = False) -> str:
    if is_room:
        return "群" if name == DEFAULT_ROOM_NAME else (name[:1] or "#")
    clean = (name or "?").strip()
    return clean[:2].upper() if clean.isascii() else clean[:2]


def format_size(size: int) -> str:
    if size < 1024:
        return f"{size} B"
    if size < 1024 * 1024:
        return f"{size / 1024:.1f} KB"
    return f"{size / (1024 * 1024):.2f} MB"


def room_key(room_id: str) -> str:
    return f"room:{room_id}"


def peer_key(peer: str) -> str:
    return f"pm:{peer}"


def make_avatar(
    parent: tk.Widget,
    name: str,
    size: int = 38,
    bg: str = C_SIDEBAR,
    is_room: bool = False,
    is_lobby: bool = False,
    online: Optional[bool] = None,
    font_family: str = "Microsoft YaHei UI",
) -> tk.Canvas:
    """绘制圆形纯色头像（可选右下角在线绿点）。"""
    c = tk.Canvas(parent, width=size, height=size, bg=bg, highlightthickness=0, bd=0)
    fill = avatar_color(name, is_lobby=is_lobby)
    c.create_oval(2, 2, size - 2, size - 2, fill=fill, outline="")
    txt = avatar_text(name, is_room=is_room)
    fs = max(9, int(size * 0.30))
    c.create_text(size / 2, size / 2, text=txt, fill="#FFFFFF", font=(font_family, fs, "bold"))
    if online is not None:
        dot_r = 4.5
        dot_col = C_ONLINE if online else "#CBD5E1"
        c.create_oval(
            size - dot_r * 2 - 1,
            size - dot_r * 2 - 1,
            size - 1,
            size - 1,
            fill=dot_col,
            outline=bg,
            width=1.5,
        )
    return c


class FlatButton(tk.Label):
    """带悬停反馈的现代扁平按钮。"""

    def __init__(
        self,
        parent: tk.Widget,
        text: str,
        command,
        bg: str = C_PRIMARY,
        fg: str = "#FFFFFF",
        hover_bg: str = C_PRIMARY_HOVER,
        font: Tuple[str, int, str] | Tuple[str, int] = ("Microsoft YaHei UI", 10, "bold"),
        padx: int = 16,
        pady: int = 6,
        border_color: Optional[str] = None,
    ) -> None:
        super().__init__(
            parent,
            text=text,
            bg=bg,
            fg=fg,
            font=font,
            padx=padx,
            pady=pady,
            cursor="hand2",
            highlightthickness=1 if border_color else 0,
            highlightbackground=border_color or bg,
        )
        self._bg = bg
        self._hover_bg = hover_bg
        self._command = command
        self._enabled = True
        self.bind("<Enter>", self._on_enter)
        self.bind("<Leave>", self._on_leave)
        self.bind("<Button-1>", self._on_click)

    def set_enabled(self, enabled: bool) -> None:
        self._enabled = enabled
        self.configure(cursor="hand2" if enabled else "arrow")

    def _on_enter(self, _e: tk.Event) -> None:
        if self._enabled:
            self.configure(bg=self._hover_bg)

    def _on_leave(self, _e: tk.Event) -> None:
        self.configure(bg=self._bg)

    def _on_click(self, _e: tk.Event) -> None:
        if self._enabled and self._command:
            self._command()


class ChatRoomGUI:
    """现代简约 IM 风格的 Tkinter 局域网聊天室桌面客户端。"""

    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.font = pick_font_family()
        try:
            self._dpi_scale = max(1.0, min(2.2, self.root.winfo_fpixels("1i") / 96.0))
        except Exception:
            self._dpi_scale = 1.0

        self.root.title("局域网聊天室 · 现代简约桌面端")
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        init_w = min(int(1040 * self._dpi_scale), int(sw * 0.88))
        init_h = min(int(680 * self._dpi_scale), int(sh * 0.84))
        self.root.geometry(f"{init_w}x{init_h}")
        self.root.minsize(min(int(820 * self._dpi_scale), init_w), min(int(540 * self._dpi_scale), init_h))
        self.root.configure(bg=C_CHAT_BG)

        # 网络与状态
        self._sock: Optional[socket.socket] = None
        self._send_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._closed = False
        self._ui_queue: queue.Queue[Tuple[str, Any]] = queue.Queue()

        self._connected: bool = False
        self._auto_reconnect: bool = False
        self._reconnecting: bool = False
        self._reconnect_cancel = threading.Event()
        self._last_conn: Optional[Tuple[str, int, str]] = None

        self._host: str = "127.0.0.1"
        self._port: int = 8888
        self._username: str = ""
        self._users: List[str] = []
        self._rooms: List[Dict[str, Any]] = []
        self._joined_rooms: Set[str] = {DEFAULT_ROOM_ID}

        # 会话与消息数据
        self._active_key: str = room_key(DEFAULT_ROOM_ID)
        self._sessions: Dict[str, Dict[str, Any]] = {}
        self._msg_lookup: Dict[str, Tuple[str, Dict[str, Any]]] = {}
        self._sent_read_ids: Set[str] = set()

        # 文件接收与临时存储
        self._temp_dir = tempfile.TemporaryDirectory(prefix="lanchat_tk_")
        self._incoming_files: Dict[str, Dict[str, Any]] = {}
        self._completed_files: Dict[str, Dict[str, Any]] = {}

        self._ensure_room_session(DEFAULT_ROOM_ID, DEFAULT_ROOM_NAME)
        self._build_ui()
        self._render_sidebar()
        self._render_header()
        self._render_messages()

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(40, self._poll_ui_queue)
        self.root.after(180, self._open_connect_modal)

    # ===================== UI 布局搭建 =====================

    def _build_ui(self) -> None:
        # 主体左右分栏
        container = tk.Frame(self.root, bg=C_CHAT_BG)
        container.pack(fill=tk.BOTH, expand=True)

        # ---------- 左侧纯白会话栏 ----------
        self.sidebar = tk.Frame(container, bg=C_SIDEBAR, width=int(282 * self._dpi_scale))
        self.sidebar.pack(side=tk.LEFT, fill=tk.Y)
        self.sidebar.pack_propagate(False)

        # 右侧细分割线
        tk.Frame(container, bg=C_BORDER, width=1).pack(side=tk.LEFT, fill=tk.Y)

        # 左侧顶部：搜索框 + 新建房间按钮
        top_search = tk.Frame(self.sidebar, bg=C_SIDEBAR, padx=14, pady=14)
        top_search.pack(fill=tk.X)

        search_wrap = tk.Frame(top_search, bg=C_INPUT_BG, padx=10, pady=6)
        search_wrap.pack(side=tk.LEFT, fill=tk.X, expand=True)

        tk.Label(search_wrap, text="🔍", bg=C_INPUT_BG, fg=C_MUTED, font=(self.font, 9)).pack(side=tk.LEFT)
        self.search_var = tk.StringVar()
        self.search_var.trace_add("write", lambda *_: self._render_sidebar())
        self.search_entry = tk.Entry(
            search_wrap,
            textvariable=self.search_var,
            bg=C_INPUT_BG,
            fg=C_TEXT,
            relief=tk.FLAT,
            font=(self.font, 10),
            insertbackground=C_PRIMARY,
        )
        self.search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(4, 0))
        self._attach_placeholder(self.search_entry, "搜索房间或联系人")

        self.room_add_btn = FlatButton(
            top_search,
            text="＋",
            command=self._open_room_modal,
            bg=C_INPUT_BG,
            fg=C_TEXT,
            hover_bg=C_SELECTED,
            font=(self.font, 11, "bold"),
            padx=10,
            pady=4,
        )
        self.room_add_btn.pack(side=tk.LEFT, padx=(8, 0))

        # 左侧可滚动会话列表
        list_holder = tk.Frame(self.sidebar, bg=C_SIDEBAR)
        list_holder.pack(fill=tk.BOTH, expand=True)

        self.sidebar_canvas = tk.Canvas(list_holder, bg=C_SIDEBAR, highlightthickness=0, bd=0)
        self.sidebar_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.sidebar_inner = tk.Frame(self.sidebar_canvas, bg=C_SIDEBAR)
        self.sidebar_win = self.sidebar_canvas.create_window((0, 0), window=self.sidebar_inner, anchor="nw")
        self.sidebar_inner.bind(
            "<Configure>",
            lambda _e: self.sidebar_canvas.configure(scrollregion=self.sidebar_canvas.bbox("all")),
        )
        self.sidebar_canvas.bind(
            "<Configure>",
            lambda e: self.sidebar_canvas.itemconfigure(self.sidebar_win, width=e.width),
        )
        self._bind_mousewheel(self.sidebar_canvas)

        # 左侧底部本机身份与连接状态栏
        tk.Frame(self.sidebar, bg=C_BORDER, height=1).pack(fill=tk.X)
        footer = tk.Frame(self.sidebar, bg="#FAFBFD", padx=12, pady=10)
        footer.pack(fill=tk.X)

        self.me_avatar_holder = tk.Frame(footer, bg="#FAFBFD")
        self.me_avatar_holder.pack(side=tk.LEFT)

        me_meta = tk.Frame(footer, bg="#FAFBFD")
        me_meta.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(9, 6))
        self.me_name_lbl = tk.Label(
            me_meta, text="未登录", bg="#FAFBFD", fg=C_TEXT, font=(self.font, 10, "bold"), anchor="w"
        )
        self.me_name_lbl.pack(fill=tk.X)
        self.me_net_lbl = tk.Label(
            me_meta, text="● 未连接服务端", bg="#FAFBFD", fg=C_OFFLINE, font=(self.font, 8), anchor="w"
        )
        self.me_net_lbl.pack(fill=tk.X)

        self.conn_btn = FlatButton(
            footer,
            text="连接设置",
            command=self._open_connect_modal,
            bg="#FAFBFD",
            fg=C_PRIMARY,
            hover_bg=C_SELECTED,
            font=(self.font, 9, "bold"),
            padx=8,
            pady=4,
        )
        self.conn_btn.pack(side=tk.RIGHT)
        self._render_me_footer()

        # ---------- 右侧聊天主窗口 ----------
        right = tk.Frame(container, bg=C_CHAT_BG)
        right.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # 顶栏
        header = tk.Frame(right, bg=C_HEADER_BG, height=62, padx=22)
        header.pack(fill=tk.X)
        header.pack_propagate(False)

        title_wrap = tk.Frame(header, bg=C_HEADER_BG)
        title_wrap.pack(side=tk.LEFT, fill=tk.Y)
        self.chat_title_lbl = tk.Label(
            title_wrap,
            text=DEFAULT_ROOM_NAME,
            bg=C_HEADER_BG,
            fg=C_TEXT,
            font=(self.font, 13, "bold"),
        )
        self.chat_title_lbl.pack(side=tk.LEFT, pady=16)

        self.chat_sub_lbl = tk.Label(
            title_wrap,
            text="群聊房间 · 在线 0 人",
            bg="#FFFFFF",
            fg=C_MUTED,
            font=(self.font, 9),
            padx=10,
            pady=2,
            highlightthickness=1,
            highlightbackground=C_BORDER,
        )
        self.chat_sub_lbl.pack(side=tk.LEFT, padx=(10, 0), pady=18)

        action_wrap = tk.Frame(header, bg=C_HEADER_BG)
        action_wrap.pack(side=tk.RIGHT)
        self.leave_room_btn = FlatButton(
            action_wrap,
            text="退出房间",
            command=self._on_leave_active_room,
            bg="#FFFFFF",
            fg="#DC2626",
            hover_bg="#FEE2E2",
            font=(self.font, 9),
            padx=10,
            pady=4,
            border_color=C_BORDER,
        )
        self.sync_hist_btn = FlatButton(
            action_wrap,
            text="同步历史",
            command=self._on_sync_history_clicked,
            bg="#FFFFFF",
            fg=C_TEXT,
            hover_bg=C_SELECTED,
            font=(self.font, 9),
            padx=10,
            pady=4,
            border_color=C_BORDER,
        )
        self.sync_hist_btn.pack(side=tk.RIGHT, pady=15)

        tk.Frame(right, bg=C_BORDER, height=1).pack(fill=tk.X)

        # 自动重连提示横幅（默认隐藏）
        self.reconnect_banner = tk.Label(
            right,
            text="",
            bg=C_WARN_BG,
            fg=C_WARN_FG,
            font=(self.font, 9),
            pady=6,
        )

        # 中间消息流（Canvas + 动态气泡）
        msg_holder = tk.Frame(right, bg=C_CHAT_BG)
        msg_holder.pack(fill=tk.BOTH, expand=True)

        self.chat_canvas = tk.Canvas(msg_holder, bg=C_CHAT_BG, highlightthickness=0, bd=0)
        self.chat_scroll = tk.Scrollbar(msg_holder, orient=tk.VERTICAL, command=self.chat_canvas.yview)
        self.chat_canvas.configure(yscrollcommand=self.chat_scroll.set)
        self.chat_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.chat_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.chat_inner = tk.Frame(self.chat_canvas, bg=C_CHAT_BG, padx=20, pady=14)
        self.chat_win = self.chat_canvas.create_window((0, 0), window=self.chat_inner, anchor="nw")
        self.chat_inner.bind(
            "<Configure>",
            lambda _e: self.chat_canvas.configure(scrollregion=self.chat_canvas.bbox("all")),
        )
        self.chat_canvas.bind(
            "<Configure>",
            lambda e: self.chat_canvas.itemconfigure(self.chat_win, width=e.width),
        )
        self._bind_mousewheel(self.chat_canvas)

        # ---------- 底部纯白输入工具栏（仅保留真实功能） ----------
        tk.Frame(right, bg=C_BORDER, height=1).pack(fill=tk.X)
        composer = tk.Frame(right, bg="#FFFFFF", padx=18, pady=10)
        composer.pack(fill=tk.X)

        toolbar = tk.Frame(composer, bg="#FFFFFF")
        toolbar.pack(fill=tk.X, pady=(0, 6))

        self.emoji_btn = FlatButton(
            toolbar,
            text="😊 表情",
            command=self._open_emoji_picker,
            bg="#FFFFFF",
            fg="#4E5969",
            hover_bg=C_HOVER,
            font=(self.font, 9),
            padx=8,
            pady=3,
        )
        self.emoji_btn.pack(side=tk.LEFT, padx=(0, 6))

        self.file_btn = FlatButton(
            toolbar,
            text="📁 发送文件",
            command=lambda: self._on_pick_and_send_file(is_image=False),
            bg="#FFFFFF",
            fg="#4E5969",
            hover_bg=C_HOVER,
            font=(self.font, 9),
            padx=8,
            pady=3,
        )
        self.file_btn.pack(side=tk.LEFT, padx=(0, 6))

        self.image_btn = FlatButton(
            toolbar,
            text="🖼 发送图片",
            command=lambda: self._on_pick_and_send_file(is_image=True),
            bg="#FFFFFF",
            fg="#4E5969",
            hover_bg=C_HOVER,
            font=(self.font, 9),
            padx=8,
            pady=3,
        )
        self.image_btn.pack(side=tk.LEFT)

        self.msg_text = tk.Text(
            composer,
            height=3,
            bg="#FFFFFF",
            fg=C_TEXT,
            relief=tk.FLAT,
            font=(self.font, 11),
            wrap=tk.WORD,
            insertbackground=C_PRIMARY,
        )
        self.msg_text.pack(fill=tk.X, pady=(0, 8))
        self.msg_text.bind("<Return>", self._on_enter_key)
        self.msg_text.bind("<Shift-Return>", lambda _e: None)

        bottom_bar = tk.Frame(composer, bg="#FFFFFF")
        bottom_bar.pack(fill=tk.X)

        self.hint_lbl = tk.Label(
            bottom_bar,
            text="Enter 发送 · Shift+Enter 换行 · SQLite 持久化 · TCP 48KB 分块文件与 SHA-256 校验",
            bg="#FFFFFF",
            fg="#B0B6BF",
            font=(self.font, 8),
        )
        self.hint_lbl.pack(side=tk.LEFT)

        self.send_btn = FlatButton(
            bottom_bar,
            text="发 送",
            command=self._on_send_text,
            bg=C_PRIMARY,
            fg="#FFFFFF",
            hover_bg=C_PRIMARY_HOVER,
            font=(self.font, 10, "bold"),
            padx=22,
            pady=6,
        )
        self.send_btn.pack(side=tk.RIGHT)

    def _attach_placeholder(self, entry: tk.Entry, placeholder: str) -> None:
        entry._placeholder = placeholder  # type: ignore[attr-defined]
        entry._showing_ph = True  # type: ignore[attr-defined]
        entry.insert(0, placeholder)
        entry.configure(fg=C_MUTED)

        def on_focus_in(_e: tk.Event) -> None:
            if getattr(entry, "_showing_ph", False):
                entry.delete(0, tk.END)
                entry.configure(fg=C_TEXT)
                entry._showing_ph = False  # type: ignore[attr-defined]

        def on_focus_out(_e: tk.Event) -> None:
            if not entry.get():
                entry._showing_ph = True  # type: ignore[attr-defined]
                entry.insert(0, placeholder)
                entry.configure(fg=C_MUTED)

        entry.bind("<FocusIn>", on_focus_in)
        entry.bind("<FocusOut>", on_focus_out)

    def _get_search_query(self) -> str:
        if getattr(self.search_entry, "_showing_ph", False):
            return ""
        return self.search_var.get().strip().lower()

    def _bind_mousewheel(self, canvas: tk.Canvas) -> None:
        def _on_wheel(event: tk.Event) -> None:
            if event.num == 4:
                canvas.yview_scroll(-3, "units")
            elif event.num == 5:
                canvas.yview_scroll(3, "units")
            elif event.delta:
                step = -1 if event.delta > 0 else 1
                canvas.yview_scroll(step * 3, "units")

        canvas.bind("<Enter>", lambda _e: (
            canvas.bind_all("<MouseWheel>", _on_wheel),
            canvas.bind_all("<Button-4>", _on_wheel),
            canvas.bind_all("<Button-5>", _on_wheel),
        ))
        canvas.bind("<Leave>", lambda _e: (
            canvas.unbind_all("<MouseWheel>"),
            canvas.unbind_all("<Button-4>"),
            canvas.unbind_all("<Button-5>"),
        ))

    # ===================== 数据模型辅助 =====================

    def _ensure_room_session(self, room_id: str, name: Optional[str] = None) -> Dict[str, Any]:
        k = room_key(room_id)
        if k not in self._sessions:
            self._sessions[k] = {
                "key": k,
                "type": "room",
                "id": room_id,
                "name": name or (DEFAULT_ROOM_NAME if room_id == DEFAULT_ROOM_ID else room_id),
                "messages": [],
                "msg_map": {},
                "unread": 0,
                "history_loaded": False,
                "last_text": "点击进入房间群聊",
                "last_time": "",
            }
        elif name:
            self._sessions[k]["name"] = name
        return self._sessions[k]

    def _ensure_peer_session(self, peer: str) -> Dict[str, Any]:
        k = peer_key(peer)
        if k not in self._sessions:
            self._sessions[k] = {
                "key": k,
                "type": "pm",
                "id": peer,
                "name": peer,
                "messages": [],
                "msg_map": {},
                "unread": 0,
                "history_loaded": False,
                "last_text": "在线 · 点击发送私聊",
                "last_time": "",
            }
        return self._sessions[k]

    def _upsert_message(self, session_key: str, msg: Dict[str, Any]) -> None:
        session = self._sessions.get(session_key)
        if session is None:
            return

        msg_id = msg.get("msg_id")
        if msg_id and msg_id in session["msg_map"]:
            existing = session["msg_map"][msg_id]
            if msg.get("status") == "delivered" and existing.get("status") == "sending":
                existing["status"] = "delivered"
            if msg.get("ts"):
                existing["ts"] = msg["ts"]
            for u in msg.get("read_by", []):
                if u not in existing["read_by"]:
                    existing["read_by"].append(u)
            if self._active_key == session_key:
                self._render_messages()
            return

        msg.setdefault("read_by", [])
        session["messages"].append(msg)
        if msg_id:
            session["msg_map"][msg_id] = msg
            self._msg_lookup[msg_id] = (session_key, msg)

        kind = msg.get("kind")
        if kind == "system":
            session["last_text"] = f"[系统] {msg.get('text', '')}"
        elif kind == "file":
            tag = "[图片]" if msg.get("is_image") else "[文件]"
            prefix = f"{msg.get('sender')}: " if session["type"] == "room" else ""
            session["last_text"] = f"{prefix}{tag} {msg.get('filename', '')}"
        else:
            prefix = f"{msg.get('sender')}: " if session["type"] == "room" else ""
            session["last_text"] = f"{prefix}{msg.get('text', '')}"

        session["last_time"] = str(msg.get("ts", ""))[:5]
        if (
            self._active_key != session_key
            and kind != "system"
            and msg.get("sender") != self._username
        ):
            session["unread"] += 1

        self._render_sidebar()
        if self._active_key == session_key:
            self._render_messages()
            self._send_read_receipts_for_active()

    # ===================== 界面渲染 =====================

    def _render_me_footer(self) -> None:
        for w in self.me_avatar_holder.winfo_children():
            w.destroy()
        make_avatar(
            self.me_avatar_holder,
            self._username or "我",
            size=34,
            bg="#FAFBFD",
            font_family=self.font,
        ).pack()
        self.me_name_lbl.configure(text=self._username if self._connected else "未登录")
        if self._connected:
            self.me_net_lbl.configure(text=f"● {self._host}:{self._port}", fg=C_ONLINE)
        else:
            self.me_net_lbl.configure(text="● 未连接服务端", fg=C_OFFLINE)

    def _render_header(self) -> None:
        active = self._sessions.get(self._active_key) or self._ensure_room_session(DEFAULT_ROOM_ID)
        self.chat_title_lbl.configure(text=active["name"])
        if active["type"] == "room":
            meta = next((r for r in self._rooms if r["room_id"] == active["id"]), None)
            cnt = meta["member_count"] if meta else len(self._users)
            self.chat_sub_lbl.configure(text=f"群聊房间 · 在线 {cnt} 人")
            if active["id"] != DEFAULT_ROOM_ID:
                self.leave_room_btn.pack(side=tk.RIGHT, padx=(8, 0), pady=15)
            else:
                self.leave_room_btn.pack_forget()
        else:
            online = active["id"] in self._users
            self.chat_sub_lbl.configure(text="在线 · 点对点私聊" if online else "离线")
            self.leave_room_btn.pack_forget()

    def _render_sidebar(self) -> None:
        for w in self.sidebar_inner.winfo_children():
            w.destroy()

        q = self._get_search_query()
        joined_rooms = [r for r in self._rooms if r.get("joined")]
        if not joined_rooms:
            joined_rooms = [{"room_id": DEFAULT_ROOM_ID, "name": DEFAULT_ROOM_NAME, "joined": True}]

        room_keys = [room_key(r["room_id"]) for r in joined_rooms]
        peer_keys = sorted(k for k in self._sessions if k.startswith("pm:"))

        self._render_sidebar_group("群聊房间", room_keys, q)
        self._render_sidebar_group("在线联系人", peer_keys, q)

    def _render_sidebar_group(self, title: str, keys: List[str], query: str) -> None:
        visible = []
        for k in keys:
            s = self._sessions.get(k)
            if not s:
                continue
            if query and query not in s["name"].lower() and query not in s["last_text"].lower():
                continue
            visible.append(s)

        if not visible:
            return

        tk.Label(
            self.sidebar_inner,
            text=title,
            bg=C_SIDEBAR,
            fg=C_MUTED,
            font=(self.font, 8, "bold"),
            anchor="w",
            padx=16,
            pady=6,
        ).pack(fill=tk.X)

        for s in visible:
            is_active = s["key"] == self._active_key
            row_bg = C_SELECTED if is_active else C_SIDEBAR
            row = tk.Frame(self.sidebar_inner, bg=row_bg, padx=12, pady=9, cursor="hand2")
            row.pack(fill=tk.X, padx=8, pady=2)

            is_room = s["type"] == "room"
            is_online = self._connected if is_room else (s["id"] in self._users)
            av = make_avatar(
                row,
                s["name"],
                size=38,
                bg=row_bg,
                is_room=is_room,
                is_lobby=(is_room and s["id"] == DEFAULT_ROOM_ID),
                online=is_online,
                font_family=self.font,
            )
            av.pack(side=tk.LEFT)

            body = tk.Frame(row, bg=row_bg)
            body.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(10, 0))

            top_line = tk.Frame(body, bg=row_bg)
            top_line.pack(fill=tk.X)
            name_lbl = tk.Label(
                top_line,
                text=s["name"],
                bg=row_bg,
                fg=C_TEXT,
                font=(self.font, 10, "bold" if is_active else "normal"),
                anchor="w",
            )
            name_lbl.pack(side=tk.LEFT, fill=tk.X, expand=True)
            time_lbl = tk.Label(
                top_line, text=s["last_time"], bg=row_bg, fg=C_MUTED, font=(self.font, 8)
            )
            time_lbl.pack(side=tk.RIGHT)

            bot_line = tk.Frame(body, bg=row_bg)
            bot_line.pack(fill=tk.X, pady=(2, 0))
            preview = s["last_text"]
            if len(preview) > 18:
                preview = preview[:18] + "…"
            prev_lbl = tk.Label(
                bot_line, text=preview, bg=row_bg, fg=C_MUTED, font=(self.font, 9), anchor="w"
            )
            prev_lbl.pack(side=tk.LEFT, fill=tk.X, expand=True)

            if s["unread"] > 0:
                badge = tk.Label(
                    bot_line,
                    text=str(s["unread"]) if s["unread"] < 100 else "99+",
                    bg=C_PRIMARY,
                    fg="#FFFFFF",
                    font=(self.font, 8, "bold"),
                    padx=5,
                    pady=0,
                )
                badge.pack(side=tk.RIGHT)

            def _select(_e: tk.Event, sk: str = s["key"]) -> None:
                self._switch_session(sk)

            for widget in (row, av, body, top_line, name_lbl, time_lbl, bot_line, prev_lbl):
                widget.bind("<Button-1>", _select)

    def _switch_session(self, session_key: str) -> None:
        self._active_key = session_key
        s = self._sessions.get(session_key)
        if s:
            s["unread"] = 0
            self._request_history(s, force=False)
        self._render_sidebar()
        self._render_header()
        self._render_messages()
        self._send_read_receipts_for_active()
        self.msg_text.focus_set()

    def _render_messages(self) -> None:
        for w in self.chat_inner.winfo_children():
            w.destroy()

        active = self._sessions.get(self._active_key) or self._ensure_room_session(DEFAULT_ROOM_ID)
        last_min = ""

        for m in active["messages"]:
            minute = str(m.get("ts", ""))[:5]
            if minute and minute != last_min:
                tk.Label(
                    self.chat_inner,
                    text=minute,
                    bg="#E3ECFB",
                    fg=C_MUTED,
                    font=(self.font, 8),
                    padx=10,
                    pady=2,
                ).pack(pady=(6, 4))
                last_min = minute

            kind = m.get("kind")
            if kind == "system":
                tk.Label(
                    self.chat_inner,
                    text=m.get("text", ""),
                    bg="#F6F9FF",
                    fg=C_MUTED,
                    font=(self.font, 9),
                    padx=12,
                    pady=3,
                    highlightthickness=1,
                    highlightbackground=C_BORDER,
                ).pack(pady=4)
                continue

            is_self = m.get("sender") == self._username
            row = tk.Frame(self.chat_inner, bg=C_CHAT_BG)
            row.pack(fill=tk.X, pady=6)

            side = tk.RIGHT if is_self else tk.LEFT
            av = make_avatar(
                row,
                m.get("sender", "?"),
                size=34,
                bg=C_CHAT_BG,
                font_family=self.font,
            )
            av.pack(side=side, anchor="n")

            col = tk.Frame(row, bg=C_CHAT_BG)
            col.pack(side=side, padx=10, anchor="n")

            if active["type"] == "room" and not is_self:
                tk.Label(
                    col,
                    text=m.get("sender", ""),
                    bg=C_CHAT_BG,
                    fg=C_MUTED,
                    font=(self.font, 8),
                    anchor="w",
                ).pack(anchor="w", pady=(0, 2))

            if kind == "file":
                self._render_file_bubble(col, m, is_self)
            else:
                bubble_bg = C_BUBBLE_SELF if is_self else C_BUBBLE_PEER
                bubble_bdr = C_BUBBLE_SELF_BDR if is_self else C_BUBBLE_PEER_BDR
                bubble = tk.Label(
                    col,
                    text=m.get("text", ""),
                    bg=bubble_bg,
                    fg=C_TEXT,
                    font=(self.font, 10),
                    justify=tk.LEFT,
                    wraplength=440,
                    padx=12,
                    pady=8,
                    highlightthickness=1,
                    highlightbackground=bubble_bdr,
                )
                bubble.pack(anchor="e" if is_self else "w")

                if is_self:
                    readers = [u for u in m.get("read_by", []) if u != self._username]
                    if readers:
                        st_txt = "✓✓ 已读" if active["type"] == "pm" else f"✓✓ {len(readers)}人已读"
                        st_fg = C_PRIMARY
                    elif m.get("status") == "sending":
                        st_txt = "发送中..."
                        st_fg = C_MUTED
                    else:
                        st_txt = "✓ 已送达"
                        st_fg = C_MUTED
                    tk.Label(
                        col,
                        text=st_txt,
                        bg=C_CHAT_BG,
                        fg=st_fg,
                        font=(self.font, 8),
                    ).pack(anchor="e", pady=(2, 0))

        self.chat_inner.update_idletasks()
        self.chat_canvas.configure(scrollregion=self.chat_canvas.bbox("all"))
        self.chat_canvas.yview_moveto(1.0)

    def _render_file_bubble(self, parent: tk.Frame, m: Dict[str, Any], is_self: bool) -> None:
        card_bg = "#F0F7FF" if is_self else "#FFFFFF"
        card = tk.Frame(
            parent,
            bg=card_bg,
            padx=12,
            pady=10,
            highlightthickness=1,
            highlightbackground=C_BUBBLE_SELF_BDR if is_self else C_BUBBLE_PEER_BDR,
        )
        card.pack(anchor="e" if is_self else "w")

        icon = "🖼" if m.get("is_image") else "📄"
        fname = m.get("filename", "file")
        size_str = format_size(int(m.get("size", 0)))
        chunks = int(m.get("total_chunks", 1))

        tk.Label(
            card,
            text=f"{icon}  {fname}",
            bg=card_bg,
            fg=C_TEXT,
            font=(self.font, 10, "bold"),
            anchor="w",
        ).pack(anchor="w")
        tk.Label(
            card,
            text=f"{size_str} · {chunks} 个 TCP 48KB 分块",
            bg=card_bg,
            fg=C_MUTED,
            font=(self.font, 8),
            anchor="w",
        ).pack(anchor="w", pady=(2, 6))

        if m.get("status") != "done":
            pct = int(m.get("percent", 0))
            rec = int(m.get("received_chunks", 0))
            tk.Label(
                card,
                text=f"TCP 分块传输中：{rec}/{chunks} ({pct}%)",
                bg=card_bg,
                fg=C_PRIMARY,
                font=(self.font, 9),
            ).pack(anchor="w")
        else:
            verified = bool(m.get("verified"))
            bar = tk.Frame(card, bg=card_bg)
            bar.pack(fill=tk.X)
            tk.Label(
                bar,
                text="✓ SHA-256 校验通过" if verified else "✕ 校验失败",
                bg=card_bg,
                fg="#16A34A" if verified else "#DC2626",
                font=(self.font, 8, "bold"),
            ).pack(side=tk.LEFT)
            file_id = m.get("file_id", "")
            FlatButton(
                bar,
                text="另存为",
                command=lambda fid=file_id: self._save_received_file(fid),
                bg=C_SELECTED,
                fg=C_PRIMARY,
                hover_bg="#D6E8FF",
                font=(self.font, 8, "bold"),
                padx=8,
                pady=2,
            ).pack(side=tk.RIGHT, padx=(12, 0))

    # ===================== 弹窗交互 =====================

    def _fit_and_center_modal(self, win: tk.Toplevel, min_w: int = 440, min_h: int = 320) -> None:
        """按当前 DPI 自动测算弹窗所需尺寸并居中，防止高分屏下底部按钮被裁切。"""
        win.update_idletasks()
        req_w = max(win.winfo_reqwidth() + 28, int(min_w * self._dpi_scale))
        req_h = max(win.winfo_reqheight() + 24, int(min_h * self._dpi_scale))
        sw = win.winfo_screenwidth()
        sh = win.winfo_screenheight()
        w = min(req_w, int(sw * 0.9))
        h = min(req_h, int(sh * 0.9))

        self.root.update_idletasks()
        rx = self.root.winfo_rootx()
        ry = self.root.winfo_rooty()
        rw = self.root.winfo_width()
        rh = self.root.winfo_height()
        x = max(20, min(rx + (rw - w) // 2, sw - w - 20))
        y = max(20, min(ry + (rh - h) // 2, sh - h - 40))

        win.minsize(w, h)
        win.geometry(f"{w}x{h}+{x}+{y}")

    def _open_connect_modal(self) -> None:
        win = tk.Toplevel(self.root)
        win.title("连接局域网聊天室")
        win.configure(bg="#FFFFFF")
        win.transient(self.root)
        win.grab_set()

        body = tk.Frame(win, bg="#FFFFFF", padx=24, pady=20)
        body.pack(fill=tk.BOTH, expand=True)

        tk.Label(body, text="连接局域网聊天室", bg="#FFFFFF", fg=C_TEXT, font=(self.font, 13, "bold")).pack(anchor="w")
        tk.Label(
            body,
            text="支持 UDP 局域网广播自动发现，或手动填写服务端 IP 与端口。",
            bg="#FFFFFF",
            fg=C_MUTED,
            font=(self.font, 9),
            justify=tk.LEFT,
            wraplength=int(390 * self._dpi_scale),
        ).pack(anchor="w", pady=(4, 14))

        row1 = tk.Frame(body, bg="#FFFFFF")
        row1.pack(fill=tk.X, pady=(0, 10))
        row1.columnconfigure(0, weight=2)
        row1.columnconfigure(1, weight=1)

        tk.Label(row1, text="服务端 IP", bg="#FFFFFF", fg=C_MUTED, font=(self.font, 9)).grid(row=0, column=0, sticky="w")
        tk.Label(row1, text="TCP 端口", bg="#FFFFFF", fg=C_MUTED, font=(self.font, 9)).grid(row=0, column=1, sticky="w", padx=(10, 0))

        host_var = tk.StringVar(value=self._host)
        port_var = tk.StringVar(value=str(self._port))
        name_var = tk.StringVar(value=self._username or "Alice")

        host_e = tk.Entry(row1, textvariable=host_var, bg="#F7F8FA", fg=C_TEXT, relief=tk.FLAT, font=(self.font, 10),
                          highlightthickness=1, highlightbackground=C_BORDER, highlightcolor=C_PRIMARY)
        host_e.grid(row=1, column=0, sticky="ew", ipady=6, pady=(4, 0))

        port_e = tk.Entry(row1, textvariable=port_var, bg="#F7F8FA", fg=C_TEXT, relief=tk.FLAT, font=(self.font, 10),
                          highlightthickness=1, highlightbackground=C_BORDER, highlightcolor=C_PRIMARY)
        port_e.grid(row=1, column=1, sticky="ew", ipady=6, padx=(10, 0), pady=(4, 0))

        tk.Label(body, text="我的昵称", bg="#FFFFFF", fg=C_MUTED, font=(self.font, 9)).pack(anchor="w")
        name_e = tk.Entry(body, textvariable=name_var, bg="#F7F8FA", fg=C_TEXT, relief=tk.FLAT, font=(self.font, 10),
                          highlightthickness=1, highlightbackground=C_BORDER, highlightcolor=C_PRIMARY)
        name_e.pack(fill=tk.X, ipady=6, pady=(4, 10))

        status_lbl = tk.Label(
            body, text="", bg="#FFFFFF", fg=C_PRIMARY, font=(self.font, 9),
            anchor="w", justify=tk.LEFT, wraplength=int(390 * self._dpi_scale),
        )
        status_lbl.pack(fill=tk.X, pady=(0, 12))

        btn_row = tk.Frame(body, bg="#FFFFFF")
        btn_row.pack(fill=tk.X, pady=(4, 0))

        def do_discover() -> None:
            status_lbl.configure(text="正在广播 UDP 探测包寻找服务端...")

            def _w() -> None:
                found = discover_server(udp_port=DISCOVERY_PORT, timeout=2.0)
                self.root.after(0, lambda: _apply_found(found))

            def _apply_found(found) -> None:
                if not win.winfo_exists():
                    return
                if found:
                    ip, tcp_p, sname = found
                    host_var.set(ip)
                    port_var.set(str(tcp_p))
                    status_lbl.configure(text=f"已发现 {sname} ({ip}:{tcp_p})")
                else:
                    status_lbl.configure(text="未收到广播响应，请手动输入服务端 IP")

            threading.Thread(target=_w, daemon=True).start()

        def do_connect(_event: Optional[tk.Event] = None) -> None:
            h = host_var.get().strip()
            p_str = port_var.get().strip()
            u = name_var.get().strip()
            if not h or not u or any(ch.isspace() for ch in u) or len(u) > 20:
                status_lbl.configure(text="请填写有效 IP 和 1~20 位非空白昵称")
                return
            try:
                p_num = int(p_str)
                if not (1 <= p_num <= 65535):
                    raise ValueError
            except ValueError:
                status_lbl.configure(text="端口必须为 1~65535 的整数")
                return

            status_lbl.configure(text=f"正在连接 {h}:{p_num} ...")

            def _w() -> None:
                res = self._connect_tcp(h, p_num, u)
                self.root.after(0, lambda: _after_conn(res))

            def _after_conn(res: Dict[str, Any]) -> None:
                if not res.get("ok"):
                    if win.winfo_exists():
                        status_lbl.configure(text=res.get("error", "连接失败"))
                    return
                if win.winfo_exists():
                    win.destroy()
                self._on_connected_ui(res)

            threading.Thread(target=_w, daemon=True).start()

        FlatButton(
            btn_row, text="UDP 自动发现", command=do_discover,
            bg="#F7F8FA", fg=C_TEXT, hover_bg=C_SELECTED, font=(self.font, 9),
            padx=12, pady=6, border_color=C_BORDER,
        ).pack(side=tk.LEFT)

        FlatButton(
            btn_row, text="确定连接", command=do_connect,
            bg=C_PRIMARY, fg="#FFFFFF", hover_bg=C_PRIMARY_HOVER, font=(self.font, 9, "bold"),
            padx=18, pady=6,
        ).pack(side=tk.RIGHT)

        FlatButton(
            btn_row, text="取消", command=win.destroy,
            bg="#F7F8FA", fg=C_TEXT, hover_bg=C_HOVER, font=(self.font, 9),
            padx=12, pady=6, border_color=C_BORDER,
        ).pack(side=tk.RIGHT, padx=(0, 8))

        win.bind("<Return>", do_connect)
        self._fit_and_center_modal(win, min_w=440, min_h=300)
        name_e.focus_set()

    def _open_room_modal(self) -> None:
        if not self._connected:
            self._open_connect_modal()
            return

        win = tk.Toplevel(self.root)
        win.title("房间与群组管理")
        win.configure(bg="#FFFFFF")
        win.transient(self.root)
        win.grab_set()

        body = tk.Frame(win, bg="#FFFFFF", padx=20, pady=18)
        body.pack(fill=tk.BOTH, expand=True)

        tk.Label(body, text="房间与群组管理", bg="#FFFFFF", fg=C_TEXT, font=(self.font, 13, "bold")).pack(anchor="w")
        tk.Label(
            body, text="创建新房间，或加入局域网内已存在的群聊房间。", bg="#FFFFFF", fg=C_MUTED,
            font=(self.font, 9), justify=tk.LEFT, wraplength=int(390 * self._dpi_scale),
        ).pack(anchor="w", pady=(4, 12))

        create_row = tk.Frame(body, bg="#FFFFFF")
        create_row.pack(fill=tk.X, pady=(0, 12))
        name_var = tk.StringVar()
        ent = tk.Entry(
            create_row, textvariable=name_var, bg="#F7F8FA", fg=C_TEXT, relief=tk.FLAT,
            font=(self.font, 10), highlightthickness=1, highlightbackground=C_BORDER, highlightcolor=C_PRIMARY,
        )
        ent.pack(side=tk.LEFT, fill=tk.X, expand=True, ipady=6)

        def do_create(_event: Optional[tk.Event] = None) -> None:
            rname = name_var.get().strip()
            if not rname:
                return
            name_var.set("")
            self._safe_send(MSG_ROOM_CREATE, name=rname)
            win.destroy()

        FlatButton(
            create_row, text="创建房间", command=do_create,
            bg=C_PRIMARY, fg="#FFFFFF", hover_bg=C_PRIMARY_HOVER, font=(self.font, 9, "bold"),
            padx=14, pady=6,
        ).pack(side=tk.LEFT, padx=(8, 0))

        list_box = tk.Frame(body, bg="#FFFFFF")
        list_box.pack(fill=tk.BOTH, expand=True)

        for r in self._rooms:
            rid = r["room_id"]
            rname = r["name"]
            joined = bool(r.get("joined"))
            row = tk.Frame(list_box, bg="#F7F8FA", padx=12, pady=8)
            row.pack(fill=tk.X, pady=3)
            tk.Label(
                row,
                text=f"{rname}  （在线 {r.get('member_count', 0)} 人）",
                bg="#F7F8FA",
                fg=C_TEXT,
                font=(self.font, 10),
            ).pack(side=tk.LEFT)

            if joined:
                def _enter(rid_=rid, rn_=rname) -> None:
                    self._ensure_room_session(rid_, rn_)
                    self._switch_session(room_key(rid_))
                    win.destroy()

                FlatButton(
                    row, text="进入", command=_enter,
                    bg="#FFFFFF", fg=C_PRIMARY, hover_bg=C_SELECTED, font=(self.font, 9),
                    padx=10, pady=3, border_color=C_BORDER,
                ).pack(side=tk.RIGHT)
            else:
                def _join(rid_=rid, rn_=rname) -> None:
                    self._safe_send(MSG_ROOM_JOIN, room_id=rid_)
                    self._joined_rooms.add(rid_)
                    self._ensure_room_session(rid_, rn_)
                    self._switch_session(room_key(rid_))
                    win.destroy()

                FlatButton(
                    row, text="加入", command=_join,
                    bg=C_PRIMARY, fg="#FFFFFF", hover_bg=C_PRIMARY_HOVER, font=(self.font, 9, "bold"),
                    padx=10, pady=3,
                ).pack(side=tk.RIGHT)

        win.bind("<Return>", do_create)
        self._fit_and_center_modal(win, min_w=440, min_h=340)
        ent.focus_set()

    def _open_emoji_picker(self) -> None:
        pop = tk.Toplevel(self.root)
        pop.overrideredirect(True)
        pop.configure(bg="#FFFFFF", highlightthickness=1, highlightbackground=C_BORDER)
        x = self.emoji_btn.winfo_rootx()
        y = self.emoji_btn.winfo_rooty() - 96
        pop.geometry(f"+{x}+{max(40, y)}")

        grid = tk.Frame(pop, bg="#FFFFFF", padx=8, pady=8)
        grid.pack()
        for idx, em in enumerate(EMOJIS):
            def _pick(e_str: str = em) -> None:
                self.msg_text.insert(tk.INSERT, e_str)
                self.msg_text.focus_set()
                pop.destroy()

            FlatButton(
                grid, text=em, command=_pick,
                bg="#FFFFFF", fg=C_TEXT, hover_bg=C_SELECTED,
                font=(self.font, 13), padx=6, pady=3,
            ).grid(row=idx // 6, column=idx % 6, padx=2, pady=2)

        pop.bind("<FocusOut>", lambda _e: pop.destroy())
        pop.focus_force()

    # ===================== 网络通信 =====================

    def _safe_send(self, msg_type: str, **payload: object) -> bool:
        if self._sock is None or not self._connected:
            return False
        with self._send_lock:
            try:
                send_packet(self._sock, msg_type, **payload)
                return True
            except OSError:
                return False

    def _connect_tcp(self, host: str, port: int, username: str) -> Dict[str, Any]:
        self._disconnect(send_logout=True, manual=True)
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
        self._sock = sock
        self._stop_event.clear()
        self._reconnect_cancel.clear()
        self._auto_reconnect = True
        self._connected = True
        self._host = host
        self._port = port
        self._username = username
        self._last_conn = (host, port, username)
        self._joined_rooms = {DEFAULT_ROOM_ID}

        threading.Thread(target=self._recv_loop, args=(sock,), daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, args=(sock,), daemon=True).start()
        return {
            "ok": True,
            "username": username,
            "users": list(ack.get("users", [])),
            "rooms": list(ack.get("rooms", [])),
        }

    def _on_connected_ui(self, res: Dict[str, Any]) -> None:
        self.reconnect_banner.pack_forget()
        self._sync_rooms(res.get("rooms", []))
        self._sync_users(res.get("users", []))
        self._render_me_footer()
        active = self._sessions.get(self._active_key)
        if active:
            self._request_history(active, force=True)
        self.msg_text.focus_set()

    def _disconnect(self, send_logout: bool = False, manual: bool = True) -> None:
        if manual:
            self._auto_reconnect = False
            self._reconnect_cancel.set()
        sock = self._sock
        was_connected = self._connected
        self._sock = None
        self._connected = False
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
        if self._closed or not self._auto_reconnect or self._reconnecting or self._last_conn is None:
            return
        self._reconnecting = True
        self._reconnect_cancel.clear()
        host, port, username = self._last_conn

        def _worker() -> None:
            attempt = 0
            try:
                while not self._closed and self._auto_reconnect and not self._reconnect_cancel.is_set():
                    delay = min(10.0, 1.0 * (2 ** min(attempt, 4)))
                    attempt += 1
                    self._ui_queue.put(("reconnecting", f"网络已断开，{delay:.0f}s 后进行第 {attempt} 次自动重连..."))
                    if self._reconnect_cancel.wait(delay):
                        break
                    if self._closed or not self._auto_reconnect:
                        break

                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(6.0)
                    try:
                        sock.connect((host, port))
                        send_packet(sock, MSG_LOGIN, username=username)
                        ack = recv_packet(sock)
                    except (OSError, ProtocolError):
                        sock.close()
                        continue

                    if not ack or ack.get("type") != MSG_LOGIN_ACK or not ack.get("ok"):
                        sock.close()
                        continue

                    sock.settimeout(None)
                    self._sock = sock
                    self._stop_event.clear()
                    self._connected = True
                    threading.Thread(target=self._recv_loop, args=(sock,), daemon=True).start()
                    threading.Thread(target=self._heartbeat_loop, args=(sock,), daemon=True).start()
                    for rid in list(self._joined_rooms):
                        if rid != DEFAULT_ROOM_ID:
                            self._safe_send(MSG_ROOM_JOIN, room_id=rid)
                    self._ui_queue.put(
                        ("reconnected", {"users": list(ack.get("users", [])), "rooms": list(ack.get("rooms", []))})
                    )
                    return
            finally:
                self._reconnecting = False

        threading.Thread(target=_worker, daemon=True).start()

    def _recv_loop(self, sock: socket.socket) -> None:
        while not self._stop_event.is_set():
            try:
                pkt = recv_packet(sock)
            except (ConnectionError, OSError, ProtocolError):
                if not self._stop_event.is_set():
                    self._disconnect(send_logout=False, manual=False)
                    self._ui_queue.put(("disconnected", "与服务端的连接意外断开"))
                    self._trigger_auto_reconnect()
                break
            if pkt is None:
                if not self._stop_event.is_set():
                    self._disconnect(send_logout=False, manual=False)
                    self._ui_queue.put(("disconnected", "服务端已关闭连接"))
                    self._trigger_auto_reconnect()
                break
            self._ui_queue.put(("packet", pkt))

    def _heartbeat_loop(self, sock: socket.socket) -> None:
        while not self._stop_event.wait(10.0):
            if self._sock is not sock or not self._connected:
                break
            if not self._safe_send(MSG_HEARTBEAT):
                break

    # ===================== 业务动作 =====================

    def _on_enter_key(self, event: tk.Event):
        if event.state & 0x0001:  # Shift pressed
            return None
        self._on_send_text()
        return "break"

    def _on_send_text(self) -> None:
        text = self.msg_text.get("1.0", tk.END).strip()
        if not text:
            return
        if not self._connected:
            self._open_connect_modal()
            return

        self.msg_text.delete("1.0", tk.END)
        active = self._sessions.get(self._active_key)
        if not active:
            return

        msg_id = uuid.uuid4().hex
        ts = current_timestamp()
        is_room = active["type"] == "room"
        self._upsert_message(
            active["key"],
            {
                "msg_id": msg_id,
                "kind": "chat" if is_room else "private",
                "sender": self._username,
                "to": None if is_room else active["id"],
                "text": text,
                "ts": ts,
                "status": "sending",
                "read_by": [],
            },
        )
        if is_room:
            self._safe_send(MSG_CHAT, room_id=active["id"], text=text, msg_id=msg_id)
        else:
            self._safe_send(MSG_PRIVATE, to=active["id"], text=text, msg_id=msg_id)

    def _on_pick_and_send_file(self, is_image: bool) -> None:
        if not self._connected:
            self._open_connect_modal()
            return
        ftypes = (
            [("图片文件", "*.png *.jpg *.jpeg *.gif *.webp *.bmp"), ("所有文件", "*.*")]
            if is_image
            else [("所有文件", "*.*")]
        )
        path_str = filedialog.askopenfilename(title="选择要通过 TCP 分块发送的文件", filetypes=ftypes)
        if not path_str:
            return

        p = Path(path_str)
        raw = p.read_bytes()
        if len(raw) > MAX_FILE_SIZE:
            messagebox.showwarning("提示", f"文件不能超过 {MAX_FILE_SIZE // (1024 * 1024)} MB")
            return

        active = self._sessions.get(self._active_key)
        if not active:
            return
        is_room = active["type"] == "room"
        to_target = None if is_room else active["id"]
        target_room = active["id"] if is_room else DEFAULT_ROOM_ID

        file_id = uuid.uuid4().hex
        sha256_hex = hashlib.sha256(raw).hexdigest()
        total_chunks = max(1, math.ceil(len(raw) / FILE_CHUNK_SIZE))
        mime = mimetypes.guess_type(p.name)[0] or "application/octet-stream"
        img_flag = is_image or mime.startswith("image/")

        def _tx() -> None:
            self._safe_send(
                MSG_FILE_META,
                file_id=file_id,
                filename=p.name,
                size=len(raw),
                mime_type=mime,
                is_image=img_flag,
                sha256=sha256_hex,
                total_chunks=total_chunks,
                to=to_target,
                room_id=target_room,
            )
            for seq in range(total_chunks):
                chunk = raw[seq * FILE_CHUNK_SIZE : (seq + 1) * FILE_CHUNK_SIZE]
                b64 = base64.b64encode(chunk).decode("ascii")
                self._safe_send(
                    MSG_FILE_CHUNK,
                    file_id=file_id,
                    seq=seq,
                    total_chunks=total_chunks,
                    data=b64,
                    to=to_target,
                    room_id=target_room,
                )
            self._safe_send(
                MSG_FILE_END,
                file_id=file_id,
                sha256=sha256_hex,
                to=to_target,
                room_id=target_room,
            )

        threading.Thread(target=_tx, daemon=True).start()

    def _save_received_file(self, file_id: str) -> None:
        rec = self._completed_files.get(file_id)
        if not rec or not Path(rec["path"]).exists():
            messagebox.showwarning("提示", "文件缓存不存在")
            return
        dest = filedialog.asksaveasfilename(title="保存接收到的文件", initialfile=rec["filename"])
        if dest:
            Path(dest).write_bytes(Path(rec["path"]).read_bytes())
            messagebox.showinfo("保存成功", f"已保存到:\n{dest}")

    def _request_history(self, session: Dict[str, Any], force: bool = False) -> None:
        if not self._connected:
            return
        if session["history_loaded"] and not force:
            return
        session["history_loaded"] = True
        if session["type"] == "room":
            self._safe_send(MSG_HISTORY_REQ, room_id=session["id"], limit=50)
        else:
            self._safe_send(MSG_HISTORY_REQ, peer=session["id"], limit=50)

    def _on_sync_history_clicked(self) -> None:
        active = self._sessions.get(self._active_key)
        if active:
            self._request_history(active, force=True)

    def _on_leave_active_room(self) -> None:
        active = self._sessions.get(self._active_key)
        if not active or active["type"] != "room" or active["id"] == DEFAULT_ROOM_ID:
            return
        rid = active["id"]
        self._safe_send(MSG_ROOM_LEAVE, room_id=rid)
        self._joined_rooms.discard(rid)
        self._switch_session(room_key(DEFAULT_ROOM_ID))

    def _send_read_receipts_for_active(self) -> None:
        if not self._connected:
            return
        active = self._sessions.get(self._active_key)
        if not active:
            return
        to_mark = []
        for m in active["messages"]:
            mid = m.get("msg_id")
            if mid and m.get("kind") != "system" and m.get("sender") != self._username and mid not in self._sent_read_ids:
                self._sent_read_ids.add(mid)
                to_mark.append(mid)
        if to_mark:
            self._safe_send(MSG_READ, msg_ids=to_mark)

    # ===================== 报文分发与 UI 队列 =====================

    def _sync_rooms(self, rooms: List[Dict[str, Any]]) -> None:
        self._rooms = rooms or [
            {"room_id": DEFAULT_ROOM_ID, "name": DEFAULT_ROOM_NAME, "member_count": len(self._users), "joined": True}
        ]
        self._joined_rooms = {r["room_id"] for r in self._rooms if r.get("joined")} or {DEFAULT_ROOM_ID}
        for r in self._rooms:
            if r.get("joined"):
                self._ensure_room_session(r["room_id"], r["name"])
        self._render_sidebar()
        self._render_header()

    def _sync_users(self, users: List[str]) -> None:
        self._users = list(users)
        for u in self._users:
            if u != self._username:
                self._ensure_peer_session(u)
        self._render_sidebar()
        self._render_header()

    def _resolve_file_key(self, pkt: Dict[str, Any]) -> str:
        to = pkt.get("to")
        if to:
            peer = to if pkt.get("sender") == self._username else pkt.get("sender", "")
            self._ensure_peer_session(peer)
            return peer_key(peer)
        rid = pkt.get("room_id") or DEFAULT_ROOM_ID
        self._ensure_room_session(rid)
        return room_key(rid)

    def _handle_packet(self, pkt: Dict[str, Any]) -> None:
        mtype = pkt.get("type")
        ts = pkt.get("ts", "")

        if mtype == MSG_CHAT:
            rid = pkt.get("room_id") or DEFAULT_ROOM_ID
            self._ensure_room_session(rid)
            self._upsert_message(
                room_key(rid),
                {
                    "msg_id": pkt.get("msg_id"),
                    "kind": "chat",
                    "sender": pkt.get("sender", ""),
                    "text": pkt.get("text", ""),
                    "ts": ts,
                    "status": "delivered",
                    "read_by": [],
                },
            )
        elif mtype == MSG_PRIVATE:
            peer = pkt.get("to") if pkt.get("sender") == self._username else pkt.get("sender", "")
            self._ensure_peer_session(peer)
            self._upsert_message(
                peer_key(peer),
                {
                    "msg_id": pkt.get("msg_id"),
                    "kind": "private",
                    "sender": pkt.get("sender", ""),
                    "to": pkt.get("to"),
                    "text": pkt.get("text", ""),
                    "ts": ts,
                    "status": "delivered",
                    "read_by": [],
                },
            )
        elif mtype == MSG_ACK:
            entry = self._msg_lookup.get(str(pkt.get("msg_id", "")))
            if entry:
                sk, msg = entry
                msg["status"] = pkt.get("status", "delivered")
                if self._active_key == sk:
                    self._render_messages()
        elif mtype == MSG_READ:
            entry = self._msg_lookup.get(str(pkt.get("msg_id", "")))
            reader = pkt.get("reader")
            if entry and reader:
                sk, msg = entry
                if reader not in msg["read_by"]:
                    msg["read_by"].append(reader)
                    if self._active_key == sk:
                        self._render_messages()
        elif mtype == MSG_HISTORY_RESP:
            peer = pkt.get("peer")
            sk = self._ensure_peer_session(peer)["key"] if peer else self._ensure_room_session(pkt.get("room_id") or DEFAULT_ROOM_ID)["key"]
            for m in pkt.get("messages", []):
                self._upsert_message(
                    sk,
                    {
                        "msg_id": m.get("msg_id"),
                        "kind": "private" if m.get("conv_type") == "private" else "chat",
                        "sender": m.get("sender", ""),
                        "to": m.get("to"),
                        "text": m.get("text", ""),
                        "ts": m.get("ts", ""),
                        "status": m.get("status", "delivered"),
                        "read_by": list(m.get("read_by", [])),
                    },
                )
        elif mtype == MSG_ROOM_LIST:
            self._sync_rooms(list(pkt.get("rooms", [])))
        elif mtype == MSG_USER_LIST:
            self._sync_users(list(pkt.get("users", [])))
        elif mtype == MSG_SYSTEM:
            rid = pkt.get("room_id") or DEFAULT_ROOM_ID
            self._ensure_room_session(rid)
            self._upsert_message(room_key(rid), {"kind": "system", "text": pkt.get("text", ""), "ts": ts})
        elif mtype == MSG_ERROR:
            messagebox.showwarning("提示", str(pkt.get("text", "服务端返回错误")))
        elif mtype == MSG_FILE_META:
            fid = str(pkt.get("file_id", ""))
            if not fid:
                return
            sk = self._resolve_file_key(pkt)
            msg = {
                "kind": "file",
                "file_id": fid,
                "filename": os.path.basename(str(pkt.get("filename", "file"))),
                "size": int(pkt.get("size", 0)),
                "is_image": bool(pkt.get("is_image", False)),
                "total_chunks": max(1, int(pkt.get("total_chunks", 1))),
                "received_chunks": 0,
                "percent": 0,
                "status": "transferring",
                "verified": False,
                "sender": pkt.get("sender", ""),
                "ts": ts,
            }
            self._incoming_files[fid] = {
                "meta": pkt,
                "session_key": sk,
                "msg": msg,
                "chunks": {},
            }
            self._upsert_message(sk, msg)
        elif mtype == MSG_FILE_CHUNK:
            fid = str(pkt.get("file_id", ""))
            entry = self._incoming_files.get(fid)
            if not entry:
                return
            seq = int(pkt.get("seq", -1))
            if seq >= 0:
                entry["chunks"][seq] = base64.b64decode(pkt.get("data", ""))
                rec = len(entry["chunks"])
                tot = entry["msg"]["total_chunks"]
                entry["msg"]["received_chunks"] = rec
                entry["msg"]["percent"] = min(100, int(rec * 100 / tot))
                if self._active_key == entry["session_key"]:
                    self._render_messages()
        elif mtype == MSG_FILE_END:
            fid = str(pkt.get("file_id", ""))
            entry = self._incoming_files.pop(fid, None)
            if not entry:
                return
            tot = entry["msg"]["total_chunks"]
            buf = bytearray()
            missing = False
            for seq in range(tot):
                part = entry["chunks"].get(seq)
                if part is None:
                    missing = True
                    break
                buf.extend(part)
            raw = bytes(buf)
            actual_hash = hashlib.sha256(raw).hexdigest()
            expected_hash = str(pkt.get("sha256") or entry["meta"].get("sha256", ""))
            verified = (not missing) and (not expected_hash or actual_hash == expected_hash)
            save_path = Path(self._temp_dir.name) / f"{fid}_{entry['msg']['filename']}"
            save_path.write_bytes(raw)
            self._completed_files[fid] = {
                "filename": entry["msg"]["filename"],
                "path": save_path,
                "verified": verified,
            }
            entry["msg"]["status"] = "done"
            entry["msg"]["percent"] = 100
            entry["msg"]["verified"] = verified
            if self._active_key == entry["session_key"]:
                self._render_messages()

    def _poll_ui_queue(self) -> None:
        while True:
            try:
                ev, payload = self._ui_queue.get_nowait()
            except queue.Empty:
                break

            if ev == "packet":
                self._handle_packet(payload)
            elif ev == "disconnected":
                self._render_me_footer()
                self.reconnect_banner.configure(text=str(payload))
                self.reconnect_banner.pack(fill=tk.X, before=self.chat_canvas.master)
            elif ev == "reconnecting":
                self._render_me_footer()
                self.reconnect_banner.configure(text=str(payload))
                self.reconnect_banner.pack(fill=tk.X, before=self.chat_canvas.master)
            elif ev == "reconnected":
                self._on_connected_ui(payload)

        if not self._closed:
            self.root.after(40, self._poll_ui_queue)

    def _on_close(self) -> None:
        self._closed = True
        self._disconnect(send_logout=True, manual=True)
        self._temp_dir.cleanup()
        self.root.destroy()


def main() -> None:
    enable_hidpi()
    root = tk.Tk()
    ChatRoomGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
