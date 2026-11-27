"""基于 Python 标准库 sqlite3 的聊天记录与多房间持久化模块。"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional

from chatroom.protocol import DEFAULT_ROOM_ID, DEFAULT_ROOM_NAME


def make_private_conv_id(user_a: str, user_b: str) -> str:
    """为一对私聊用户生成顺序无关的会话 ID。"""
    low, high = sorted((user_a, user_b))
    return f"pm:{low}::{high}"


class ChatStorage:
    """线程安全的 SQLite 持久化层，管理房间、历史消息与已读回执。"""

    def __init__(self, db_path: str = "chatroom_data.db") -> None:
        self.db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        if db_path != ":memory:":
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS rooms (
                    room_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    creator TEXT NOT NULL,
                    created_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    msg_id TEXT UNIQUE NOT NULL,
                    conv_type TEXT NOT NULL,
                    conv_id TEXT NOT NULL,
                    sender TEXT NOT NULL,
                    target TEXT,
                    kind TEXT NOT NULL DEFAULT 'text',
                    text TEXT NOT NULL DEFAULT '',
                    meta_json TEXT,
                    ts TEXT NOT NULL,
                    created_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_messages_conv
                    ON messages(conv_type, conv_id, id);

                CREATE TABLE IF NOT EXISTS read_receipts (
                    msg_id TEXT NOT NULL,
                    username TEXT NOT NULL,
                    read_at REAL NOT NULL,
                    PRIMARY KEY (msg_id, username)
                );
                """
            )
            self._conn.execute(
                """
                INSERT OR IGNORE INTO rooms (room_id, name, creator, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (DEFAULT_ROOM_ID, DEFAULT_ROOM_NAME, "system", time.time()),
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---------- 房间管理 ----------

    def create_room(self, room_id: str, name: str, creator: str) -> Dict[str, Any]:
        now = time.time()
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO rooms (room_id, name, creator, created_at) VALUES (?, ?, ?, ?)",
                (room_id, name, creator, now),
            )
        return {"room_id": room_id, "name": name, "creator": creator, "created_at": now}

    def get_room(self, room_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT room_id, name, creator, created_at FROM rooms WHERE room_id = ?",
                (room_id,),
            ).fetchone()
        return dict(row) if row else None

    def list_rooms(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT room_id, name, creator, created_at
                FROM rooms
                ORDER BY CASE WHEN room_id = ? THEN 0 ELSE 1 END, created_at ASC
                """,
                (DEFAULT_ROOM_ID,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ---------- 消息持久化与历史查询 ----------

    def save_message(
        self,
        msg_id: str,
        conv_type: str,
        conv_id: str,
        sender: str,
        ts: str,
        text: str = "",
        target: Optional[str] = None,
        kind: str = "text",
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        meta_json = json.dumps(meta, ensure_ascii=False) if meta is not None else None
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO messages
                    (msg_id, conv_type, conv_id, sender, target, kind, text, meta_json, ts, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    msg_id,
                    conv_type,
                    conv_id,
                    sender,
                    target,
                    kind,
                    text,
                    meta_json,
                    ts,
                    time.time(),
                ),
            )

    def get_history(
        self,
        conv_type: str,
        conv_id: str,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        limit = max(1, min(limit, 200))
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT msg_id, conv_type, conv_id, sender, target, kind, text, meta_json, ts
                FROM messages
                WHERE conv_type = ? AND conv_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (conv_type, conv_id, limit),
            ).fetchall()
            rows = list(reversed(rows))

            msg_ids = [r["msg_id"] for r in rows]
            reads_map: Dict[str, List[str]] = {mid: [] for mid in msg_ids}
            if msg_ids:
                placeholders = ",".join("?" for _ in msg_ids)
                read_rows = self._conn.execute(
                    f"SELECT msg_id, username FROM read_receipts WHERE msg_id IN ({placeholders})",
                    msg_ids,
                ).fetchall()
                for rr in read_rows:
                    reads_map[rr["msg_id"]].append(rr["username"])

        result: List[Dict[str, Any]] = []
        for r in rows:
            item: Dict[str, Any] = {
                "msg_id": r["msg_id"],
                "conv_type": r["conv_type"],
                "conv_id": r["conv_id"],
                "sender": r["sender"],
                "to": r["target"],
                "kind": r["kind"],
                "text": r["text"],
                "ts": r["ts"],
                "status": "delivered",
                "read_by": reads_map.get(r["msg_id"], []),
            }
            if r["meta_json"]:
                try:
                    item["meta"] = json.loads(r["meta_json"])
                except json.JSONDecodeError:
                    pass
            result.append(item)
        return result

    def get_message_by_id(self, msg_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT msg_id, conv_type, conv_id, sender, target FROM messages WHERE msg_id = ?",
                (msg_id,),
            ).fetchone()
        return dict(row) if row else None

    def mark_read(self, msg_ids: List[str], reader: str) -> List[Dict[str, Any]]:
        """记录已读回执，返回本次新增已读的消息元信息（过滤掉自己发的消息）。"""
        if not msg_ids:
            return []
        now = time.time()
        newly_read: List[Dict[str, Any]] = []
        with self._lock, self._conn:
            for msg_id in msg_ids:
                row = self._conn.execute(
                    "SELECT msg_id, conv_type, conv_id, sender, target FROM messages WHERE msg_id = ?",
                    (msg_id,),
                ).fetchone()
                if row is None or row["sender"] == reader:
                    continue
                cur = self._conn.execute(
                    "INSERT OR IGNORE INTO read_receipts (msg_id, username, read_at) VALUES (?, ?, ?)",
                    (msg_id, reader, now),
                )
                if cur.rowcount > 0:
                    newly_read.append(dict(row))
        return newly_read
