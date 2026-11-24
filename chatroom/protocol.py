"""自定义应用层协议模块。

帧格式（解决 TCP 字节流粘包 / 拆包问题）：
+--------------------------+----------------------------------------+
|  4 字节无符号大端整数 (!I) |            N 字节 UTF-8 JSON           |
|      Body Length (N)     | {"type": "...", "ts": "...", ...}      |
+--------------------------+----------------------------------------+
"""

from __future__ import annotations

import json
import socket
import struct
from datetime import datetime
from typing import Any, Dict, Optional

HEADER_FORMAT = "!I"  # 网络字节序（大端）4字节无符号整数
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
MAX_PACKET_SIZE = 1024 * 1024  # 单包最大 1 MB，防止恶意超大长度头耗尽内存
FILE_CHUNK_SIZE = 48 * 1024  # 文件 TCP 分块大小（48 KB 原始字节，Base64 编码后约 64 KB）
MAX_FILE_SIZE = 20 * 1024 * 1024  # 单文件最大 20 MB

# 消息类型常量
MSG_LOGIN = "LOGIN"
MSG_LOGIN_ACK = "LOGIN_ACK"
MSG_CHAT = "CHAT"
MSG_PRIVATE = "PRIVATE"
MSG_USER_LIST = "USER_LIST"
MSG_SYSTEM = "SYSTEM"
MSG_HEARTBEAT = "HEARTBEAT"
MSG_LOGOUT = "LOGOUT"
MSG_ERROR = "ERROR"
MSG_FILE_META = "FILE_META"
MSG_FILE_CHUNK = "FILE_CHUNK"
MSG_FILE_END = "FILE_END"


class ProtocolError(Exception):
    """应用层协议解析或封包错误。"""


def current_timestamp() -> str:
    """返回 HH:MM:SS 格式的当前时间字符串。"""
    return datetime.now().strftime("%H:%M:%S")


def encode_packet(msg_type: str, **payload: Any) -> bytes:
    """将消息类型和字段编码为「4字节长度头 + JSON体」字节串。"""
    body_dict: Dict[str, Any] = {"type": msg_type, **payload}
    if "ts" not in body_dict:
        body_dict["ts"] = current_timestamp()
    body_bytes = json.dumps(body_dict, ensure_ascii=False).encode("utf-8")
    if len(body_bytes) > MAX_PACKET_SIZE:
        raise ProtocolError(f"消息体过大: {len(body_bytes)} bytes")
    header = struct.pack(HEADER_FORMAT, len(body_bytes))
    return header + body_bytes


def send_packet(sock: socket.socket, msg_type: str, **payload: Any) -> None:
    """发送一条完整的应用层协议报文。"""
    raw = encode_packet(msg_type, **payload)
    sock.sendall(raw)


def recv_exact(sock: socket.socket, size: int) -> Optional[bytes]:
    """循环读取精确 size 字节；若对端正常关闭且尚未读到任何数据则返回 None。"""
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            if not chunks:
                return None
            raise ConnectionError("连接在读取完整数据包前意外断开")
        chunks.extend(chunk)
    return bytes(chunks)


def recv_packet(sock: socket.socket) -> Optional[Dict[str, Any]]:
    """从 TCP 流中读取并解析一条完整报文；对端关闭时返回 None。"""
    header = recv_exact(sock, HEADER_SIZE)
    if header is None:
        return None
    (body_len,) = struct.unpack(HEADER_FORMAT, header)
    if body_len == 0 or body_len > MAX_PACKET_SIZE:
        raise ProtocolError(f"非法数据包长度: {body_len}")

    body_bytes = recv_exact(sock, body_len)
    if body_bytes is None:
        raise ConnectionError("读取消息体时连接关闭")

    try:
        data = json.loads(body_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError(f"JSON 解码失败: {exc}") from exc

    if not isinstance(data, dict) or "type" not in data:
        raise ProtocolError("报文缺少必需的 type 字段")
    return data
