# 局域网聊天室（计算机网络课程作业 Demo）

基于 Python 3 标准库（零第三方依赖）实现的 **TCP (C/S) + UDP 广播自动发现** 局域网聊天室，涵盖计算机网络课程常见核心知识点：

- **传输层双协议结合**：TCP 负责可靠聊天通信，UDP 局域网广播负责自动发现服务端。
- **自定义应用层协议**：采用 `4 字节大端长度头 (!I) + UTF-8 JSON` 帧格式，解决 TCP 字节流粘包与拆包问题。
- **SQLite 持久化与历史消息同步**：使用内置 `sqlite3` 持久化房间、聊天记录与已读回执，服务端重启不丢记录，客户端登录或切换会话自动拉取历史消息。
- **自定义多房间 / 群组管理**：支持创建、加入、退出多个独立群聊房间，服务端维护房间成员路由表，确保未加入房间的节点不会收到该房间消息。
- **可靠送达回执（ACK）、已读状态与断线自动重连**：每条消息带唯一 `msg_id`，支持「发送中 → ✓ 已送达 → ✓✓ 已读」状态流转；连接意外中断时客户端按指数退避（`1s → 2s → 4s → 8s`）自动重连并同步历史。
- **TCP 分块文件与图片传输**：将文件/图片按 `48 KB/块` 切分封装为 `FILE_META` → `FILE_CHUNK` → `FILE_END` 协议帧流式传输，接收端按序号重组并进行 **SHA-256 完整性校验**。
- **服务端并发与心跳保活**：多线程并发模型 + 全局在线用户锁 + 单连接发送锁 + 心跳超时僵尸连接清理。

---

## 目录结构

```text
networkHomework/
├── chatroom/
│   ├── __init__.py
│   ├── protocol.py     # 应用层协议编解码（4字节长度前缀 + JSON + 全量消息常量）
│   ├── storage.py      # SQLite 持久化层（房间表、历史消息表、已读回执表）
│   ├── discovery.py    # UDP 局域网广播发现服务端与客户端探测
│   ├── server.py       # 多线程 TCP 服务端（多房间路由/私聊/ACK/已读/文件分块/心跳检测）
│   ├── web_client.py   # PC 端现代简约 IM Web 客户端（桥接 TCP/UDP、文件校验、指数退避自动重连）
│   ├── web/index.html  # 浅蓝渐变左右分栏 IM 前端界面（无冗余假图标，全真实功能）
│   ├── gui_client.py   # 桌面图形界面客户端（基于 Python 内置 tkinter）
│   └── client.py       # 命令行交互式客户端
├── docs/
│   ├── 计算机网络课程设计实验报告_局域网聊天室.docx  # 15 页完整课程设计实验报告（含小组成员信息预留空）
│   ├── 局域网聊天室_课程设计答辩稿.md               # 逐页答辩讲稿 + 双机演示脚本 + 8 道老师高频 Q&A
│   └── 局域网聊天室_课程设计答辩PPT.pptx            # 12 页 16:9 现代科技风配套答辩 PPT（含演讲者备注）
└── tests/
    └── test_chat.py    # 11 项端到端集成测试（UDP 发现、粘包、多房间、SQLite、ACK/已读、重连、分块文件）
```

---

## 应用层协议帧格式

```text
+--------------------------+-------------------------------------------+
|  4 字节无符号大端整数 (!I) |             N 字节 UTF-8 JSON             |
|      Body Length (N)     | {"type":"CHAT","sender":"Alice","text":"Hi"} |
+--------------------------+-------------------------------------------+
```

支持的消息类型：`LOGIN`、`LOGIN_ACK`、`CHAT`、`PRIVATE`、`MSG_ACK`、`MSG_READ`、`HISTORY_REQ`、`HISTORY_RESP`、`ROOM_CREATE`、`ROOM_JOIN`、`ROOM_LEAVE`、`ROOM_LIST`、`FILE_META`、`FILE_CHUNK`、`FILE_END`、`USER_LIST`、`SYSTEM`、`HEARTBEAT`、`LOGOUT`、`ERROR`。

---

## 快速运行

需要 Python 3.9+（无需安装任何第三方依赖）。

### 1. 启动服务端

```bash
python3 -m chatroom.server --host 0.0.0.0 --port 8888
```

启动后会打印本机局域网参考 IP、TCP/UDP 监听端口以及 SQLite 数据库路径（默认 `chatroom_data.db`，可通过 `--db` 自定义）。

### 2. 启动客户端（三种界面任选）

**方式 A：PC 端现代简约 IM Web 界面（最推荐）**

```bash
python3 -m chatroom.web_client --web-port 8080
```

启动后会自动打开浏览器界面（底层依然走自定义 TCP 协议与 UDP 自动发现）。支持：
- 点击左上角 **`+`** 创建或加入自定义群聊房间，点击联系人进入点对点私聊；
- 消息实时展示 **「发送中 / ✓ 已送达 / ✓✓ 已读」** 状态；
- 底部工具栏仅保留真实可用的 **表情 / 文件 / 图片** 功能（并支持拖拽文件和 `Ctrl+V` 粘贴截图）。

**方式 B：Tkinter 桌面窗口客户端**

```bash
python3 -m chatroom.gui_client
```

**方式 C：命令行客户端**

```bash
# UDP 局域网自动发现
python3 -m chatroom.client --name Alice

# 或手动指定服务端 IP 和端口
python3 -m chatroom.client --host 127.0.0.1 --port 8888 --name Bob
```

> 跨设备局域网联调时，请确保设备在同一 Wi-Fi / 热点下，并在服务端电脑防火墙放行 TCP `8888` 与 UDP `9999` 端口。

### 3. 客户端常用命令

| 输入内容 | 作用 |
| :--- | :--- |
| 直接输入文字 | 发送群聊消息 |
| `@Bob 你好` 或 `/pm Bob 你好` | 向 `Bob` 发送私聊消息 |
| `/users` | 查看当前在线用户列表 |
| `/help` | 查看命令帮助 |
| `/quit` | 正常退出聊天室 |

---

## 运行自动化测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖：UDP 自动发现、重名拦截、群聊广播、私聊隔离、TCP 粘包合并发送拆包验证、下线广播与用户列表同步。

---

## 实验报告抓包参考（Wireshark）

- 过滤聊天室 TCP 通信（查看三次握手、4 字节长度头 + JSON 载荷、四次挥手）：
  ```text
  tcp.port == 8888
  ```
- 过滤 UDP 局域网自动发现广播包：
  ```text
  udp.port == 9999
  ```