# -*- coding: utf-8 -*-
"""
相识北洋 - Python 局域网交友/聊天程序

运行方式：
    python main.py

打开浏览器访问：
    http://127.0.0.1:8765

说明：
- 不需要安装第三方库，只使用 Python 标准库。
- 两台设备必须在同一个局域网/校园网/Wi-Fi 下。
- 如果 Windows 防火墙弹窗，请允许 Python 访问专用网络。
"""

import argparse
import json
import os
import queue
import socket
import sys
import threading
import time
import uuid
import webbrowser
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

APP_NAME = "相识北洋"
UDP_DISCOVERY_PORT = 50123
DEFAULT_TCP_PORT = 50124
DEFAULT_HTTP_PORT = 8765
ONLINE_TIMEOUT = 10
DATA_DIR = Path(os.environ.get("BEIYANG_DATA_DIR", Path(__file__).resolve().parent / "beiyang_data"))


def now_ts() -> float:
    return time.time()


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def make_msg_id() -> str:
    return f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:10]}"


def safe_load_json(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default


def safe_write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def find_free_port(start_port: int) -> int:
    for port in range(start_port, start_port + 80):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind(("", port))
                return port
            except OSError:
                continue
    raise RuntimeError("找不到可用端口，请关闭占用端口的程序后重试。")


def get_local_ip() -> str:
    """尽量获取局域网 IP。没有网络时返回 127.0.0.1。"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        try:
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return "127.0.0.1"


class Store:
    """负责保存个人资料、好友、聊天记录、待发送消息。"""

    def __init__(self, tcp_port: int):
        self.lock = threading.RLock()
        DATA_DIR.mkdir(parents=True, exist_ok=True)

        profile_path = DATA_DIR / "profile.json"
        self.profile = safe_load_json(profile_path, None)
        if not self.profile:
            short = uuid.uuid4().hex[:4].upper()
            self.profile = {
                "user_id": uuid.uuid4().hex,
                "nickname": f"北洋同学-{short}",
                "bio": "你好，我是北洋校园网里的同学。",
                "created_at": now_text(),
            }
        self.profile["tcp_port"] = tcp_port
        self.profile["app"] = APP_NAME

        self.friends = safe_load_json(DATA_DIR / "friends.json", {})
        self.messages = safe_load_json(DATA_DIR / "messages.json", {})
        self.pending = safe_load_json(DATA_DIR / "pending.json", [])
        # relay_pending 保存“帮别人中转但暂时送不到”的消息，满足 A->B->C 的离线传播场景
        self.relay_pending = safe_load_json(DATA_DIR / "relay_pending.json", [])
        self.requests = safe_load_json(DATA_DIR / "requests.json", {})

        # peers 和 seen_ids 不需要长期保存
        self.peers = {}
        self.seen_ids = set()
        self.save_all()

    @property
    def user_id(self) -> str:
        return self.profile["user_id"]

    def save_all(self) -> None:
        with self.lock:
            safe_write_json(DATA_DIR / "profile.json", self.profile)
            safe_write_json(DATA_DIR / "friends.json", self.friends)
            safe_write_json(DATA_DIR / "messages.json", self.messages)
            safe_write_json(DATA_DIR / "pending.json", self.pending)
            safe_write_json(DATA_DIR / "relay_pending.json", self.relay_pending)
            safe_write_json(DATA_DIR / "requests.json", self.requests)

    def update_profile(self, nickname: str, bio: str) -> None:
        with self.lock:
            self.profile["nickname"] = nickname.strip() or self.profile["nickname"]
            self.profile["bio"] = bio.strip()
            self.save_all()

    def public_info(self) -> dict:
        with self.lock:
            return {
                "user_id": self.profile["user_id"],
                "nickname": self.profile.get("nickname", ""),
                "bio": self.profile.get("bio", ""),
                "tcp_port": self.profile.get("tcp_port", DEFAULT_TCP_PORT),
                "app": APP_NAME,
            }

    def update_peer(self, info: dict, ip: str) -> None:
        peer_id = info.get("user_id")
        if not peer_id or peer_id == self.user_id:
            return
        with self.lock:
            peer = {
                "user_id": peer_id,
                "nickname": info.get("nickname", "未命名同学"),
                "bio": info.get("bio", ""),
                "ip": ip,
                "tcp_port": int(info.get("tcp_port", DEFAULT_TCP_PORT)),
                "last_seen": now_ts(),
            }
            self.peers[peer_id] = peer
            if peer_id in self.friends:
                self.friends[peer_id].update({
                    "nickname": peer["nickname"],
                    "bio": peer["bio"],
                    "last_ip": ip,
                    "tcp_port": peer["tcp_port"],
                    "last_seen": peer["last_seen"],
                })
                self.save_all()

    def online_peers(self) -> dict:
        with self.lock:
            cutoff = now_ts() - ONLINE_TIMEOUT
            return {pid: p for pid, p in self.peers.items() if p.get("last_seen", 0) >= cutoff}

    def peer_or_friend_addr(self, user_id: str):
        with self.lock:
            online = self.online_peers()
            if user_id in online:
                p = online[user_id]
                return p.get("ip"), int(p.get("tcp_port", DEFAULT_TCP_PORT))
            f = self.friends.get(user_id)
            if f and f.get("last_ip"):
                return f.get("last_ip"), int(f.get("tcp_port", DEFAULT_TCP_PORT))
        return None, None

    def add_friend(self, info: dict, ip: str = None) -> None:
        friend_id = info.get("user_id")
        if not friend_id or friend_id == self.user_id:
            return
        with self.lock:
            peer = self.peers.get(friend_id, {})
            self.friends[friend_id] = {
                "user_id": friend_id,
                "nickname": info.get("nickname") or peer.get("nickname") or "好友",
                "bio": info.get("bio") or peer.get("bio") or "",
                "last_ip": ip or info.get("ip") or peer.get("ip") or self.friends.get(friend_id, {}).get("last_ip", ""),
                "tcp_port": int(info.get("tcp_port") or peer.get("tcp_port") or self.friends.get(friend_id, {}).get("tcp_port", DEFAULT_TCP_PORT)),
                "created_at": self.friends.get(friend_id, {}).get("created_at", now_text()),
                "last_seen": peer.get("last_seen", self.friends.get(friend_id, {}).get("last_seen", 0)),
                "category": info.get("category") or self.friends.get(friend_id, {}).get("category", "默认分组"),
            }
            self.messages.setdefault(friend_id, [])
            self.save_all()

    def delete_friend(self, friend_id: str) -> None:
        with self.lock:
            self.friends.pop(friend_id, None)
            self.save_all()

    def update_friend_category(self, friend_id: str, category: str) -> bool:
        with self.lock:
            if friend_id not in self.friends:
                return False
            self.friends[friend_id]["category"] = (category or "默认分组").strip() or "默认分组"
            self.save_all()
            return True

    def add_request(self, info: dict, ip: str = None) -> None:
        from_id = info.get("user_id")
        if not from_id or from_id == self.user_id:
            return
        with self.lock:
            self.requests[from_id] = {
                "user_id": from_id,
                "nickname": info.get("nickname", "同学"),
                "bio": info.get("bio", ""),
                "ip": ip or info.get("ip", ""),
                "tcp_port": int(info.get("tcp_port", DEFAULT_TCP_PORT)),
                "time": now_text(),
                "status": "pending",
            }
            self.save_all()

    def set_request_status(self, user_id: str, status: str) -> None:
        with self.lock:
            if user_id in self.requests:
                self.requests[user_id]["status"] = status
                self.save_all()

    def append_message(self, friend_id: str, msg: dict) -> bool:
        """返回是否为新消息。"""
        msg_id = msg.get("msg_id")
        with self.lock:
            if msg_id and msg_id in self.seen_ids:
                return False
            if msg_id:
                self.seen_ids.add(msg_id)
            self.messages.setdefault(friend_id, []).append(msg)
            # 避免聊天记录无限增长，每个好友最多保存最近 300 条
            self.messages[friend_id] = self.messages[friend_id][-300:]
            self.save_all()
            return True

    def mark_message_status(self, msg_id: str, status: str) -> None:
        if not msg_id:
            return
        with self.lock:
            changed = False
            for chat in self.messages.values():
                for msg in chat:
                    if msg.get("msg_id") == msg_id:
                        msg["status"] = status
                        changed = True
            if changed:
                self.save_all()

    def add_pending(self, payload: dict, target_id: str) -> None:
        msg_id = payload.get("msg_id")
        with self.lock:
            if msg_id and any(p.get("msg_id") == msg_id for p in self.pending):
                return
            self.pending.append({
                "msg_id": msg_id or make_msg_id(),
                "target_id": target_id,
                "payload": payload,
                "created_at": now_text(),
                "last_try": 0,
                "relay_try": 0,
            })
            self.save_all()

    def remove_pending(self, msg_id: str) -> None:
        with self.lock:
            before = len(self.pending)
            self.pending = [p for p in self.pending if p.get("msg_id") != msg_id]
            if len(self.pending) != before:
                self.save_all()

    def add_relay_pending(self, payload: dict, target_id: str, source_id: str = None) -> None:
        """保存帮别人中转的离线消息。比如 A 发给 C，B 在线但 C 离线，B 会暂存，等 C 上线再发。"""
        msg_id = payload.get("msg_id")
        if not msg_id or target_id == self.user_id:
            return
        with self.lock:
            if any(p.get("msg_id") == msg_id for p in self.relay_pending):
                return
            self.relay_pending.append({
                "msg_id": msg_id,
                "target_id": target_id,
                "source_id": source_id or payload.get("from_id"),
                "payload": payload,
                "received_at": now_text(),
                "last_try": 0,
                "relay_try": 0,
            })
            self.relay_pending = self.relay_pending[-300:]
            self.save_all()

    def remove_relay_pending(self, msg_id: str) -> None:
        with self.lock:
            before = len(self.relay_pending)
            self.relay_pending = [p for p in self.relay_pending if p.get("msg_id") != msg_id]
            if len(self.relay_pending) != before:
                self.save_all()

    def edit_pending_message(self, msg_id: str, new_text: str) -> bool:
        """允许编辑尚未直接送达的本机待发送消息。"""
        new_text = (new_text or "").strip()
        if not msg_id or not new_text:
            return False
        with self.lock:
            found = False
            for item in self.pending:
                if item.get("msg_id") == msg_id:
                    item.setdefault("payload", {})["text"] = new_text
                    found = True
            for chat in self.messages.values():
                for msg in chat:
                    if msg.get("msg_id") == msg_id and msg.get("direction") == "out":
                        msg["text"] = new_text
                        found = True
            if found:
                self.save_all()
            return found

    def state_snapshot(self) -> dict:
        with self.lock:
            online = self.online_peers()
            friends = []
            for fid, f in self.friends.items():
                item = dict(f)
                item["online"] = fid in online
                if fid in online:
                    item["last_ip"] = online[fid].get("ip", item.get("last_ip", ""))
                    item["tcp_port"] = online[fid].get("tcp_port", item.get("tcp_port", DEFAULT_TCP_PORT))
                friends.append(item)

            classmates = []
            for pid, p in online.items():
                item = dict(p)
                item["is_friend"] = pid in self.friends
                classmates.append(item)

            requests = [r for r in self.requests.values() if r.get("status") == "pending"]
            return {
                "profile": dict(self.profile),
                "local_ip": get_local_ip(),
                "http_port": AppConfig.http_port,
                "udp_port": UDP_DISCOVERY_PORT,
                "classmates": sorted(classmates, key=lambda x: x.get("nickname", "")),
                "friends": sorted(friends, key=lambda x: (not x.get("online"), x.get("nickname", ""))),
                "requests": requests,
                "pending_count": len(self.pending),
                "relay_pending_count": len(self.relay_pending),
            }


class AppConfig:
    http_port = DEFAULT_HTTP_PORT


class Network:
    def __init__(self, app: "BeiyangApp"):
        self.app = app
        self.store = app.store
        self.stop_event = threading.Event()

    def start(self) -> None:
        threads = [
            threading.Thread(target=self.udp_broadcast_loop, daemon=True),
            threading.Thread(target=self.udp_listen_loop, daemon=True),
            threading.Thread(target=self.tcp_server_loop, daemon=True),
            threading.Thread(target=self.pending_loop, daemon=True),
        ]
        for t in threads:
            t.start()

    def udp_broadcast_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                payload = self.store.public_info()
                payload["type"] = "hello"
                data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    s.sendto(data, ("255.255.255.255", UDP_DISCOVERY_PORT))
            except Exception:
                pass
            time.sleep(2)

    def udp_listen_loop(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind(("", UDP_DISCOVERY_PORT))
            except OSError as e:
                print(f"[提示] UDP 发现端口 {UDP_DISCOVERY_PORT} 绑定失败：{e}")
                return
            while not self.stop_event.is_set():
                try:
                    data, addr = s.recvfrom(8192)
                    info = json.loads(data.decode("utf-8", errors="ignore"))
                    if info.get("type") == "hello" and info.get("app") == APP_NAME:
                        self.store.update_peer(info, addr[0])
                except Exception:
                    continue

    def tcp_server_loop(self) -> None:
        port = self.store.profile["tcp_port"]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                server.bind(("", port))
                server.listen(50)
                print(f"[网络] TCP 消息服务已启动：0.0.0.0:{port}")
            except OSError as e:
                print(f"[错误] TCP 端口 {port} 绑定失败：{e}")
                return
            while not self.stop_event.is_set():
                try:
                    conn, addr = server.accept()
                    threading.Thread(target=self.handle_tcp_client, args=(conn, addr), daemon=True).start()
                except Exception:
                    continue

    def handle_tcp_client(self, conn: socket.socket, addr) -> None:
        with conn:
            try:
                conn.settimeout(5)
                buf = b""
                while True:
                    chunk = conn.recv(65536)
                    if not chunk:
                        break
                    buf += chunk
                    if b"\n" in buf:
                        break
                line = buf.split(b"\n", 1)[0]
                payload = json.loads(line.decode("utf-8", errors="ignore"))
                self.app.handle_payload(payload, addr[0])
                conn.sendall(json.dumps({"ok": True}, ensure_ascii=False).encode("utf-8") + b"\n")
            except Exception as e:
                try:
                    conn.sendall(json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False).encode("utf-8") + b"\n")
                except Exception:
                    pass

    @staticmethod
    def send_tcp(ip: str, port: int, payload: dict, timeout: float = 2.5) -> bool:
        if not ip or not port:
            return False
        try:
            with socket.create_connection((ip, int(port)), timeout=timeout) as s:
                s.settimeout(timeout)
                data = json.dumps(payload, ensure_ascii=False).encode("utf-8") + b"\n"
                s.sendall(data)
                try:
                    s.recv(1024)
                except Exception:
                    pass
            return True
        except Exception:
            return False

    def send_direct_to_user(self, target_id: str, payload: dict) -> bool:
        ip, port = self.store.peer_or_friend_addr(target_id)
        return self.send_tcp(ip, port, payload)

    def relay_to_online_friends(self, target_id: str, payload: dict, exclude_id: str = None) -> bool:
        online = self.store.online_peers()
        sent = False
        relay_msg = {
            "type": "relay",
            "from_id": self.store.user_id,
            "target_id": target_id,
            "msg_id": payload.get("msg_id") or make_msg_id(),
            "payload": payload,
            "hops": int(payload.get("hops", 0)) + 1,
        }
        for fid in list(self.store.friends.keys()):
            if fid == target_id or fid == exclude_id:
                continue
            p = online.get(fid)
            if not p:
                continue
            if self.send_tcp(p.get("ip"), p.get("tcp_port"), relay_msg, timeout=1.5):
                sent = True
        return sent

    def send_or_queue(self, target_id: str, payload: dict, queue_if_fail: bool = True) -> str:
        if self.send_direct_to_user(target_id, payload):
            return "direct"
        if self.relay_to_online_friends(target_id, payload):
            if queue_if_fail:
                self.store.add_pending(payload, target_id)
            return "relay"
        if queue_if_fail:
            self.store.add_pending(payload, target_id)
        return "queued"

    def pending_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                with self.store.lock:
                    pending_copy = list(self.store.pending)
                for item in pending_copy:
                    target_id = item.get("target_id")
                    payload = item.get("payload") or {}
                    msg_id = item.get("msg_id") or payload.get("msg_id")
                    # 每 5 秒尝试一次直连，每 12 秒尝试一次中转，避免疯狂刷屏
                    now = now_ts()
                    if now - float(item.get("last_try", 0)) < 5:
                        continue
                    item["last_try"] = now
                    if self.send_direct_to_user(target_id, payload):
                        self.store.remove_pending(msg_id)
                        self.store.mark_message_status(msg_id, "已直接送达")
                        continue
                    if now - float(item.get("relay_try", 0)) >= 12:
                        item["relay_try"] = now
                        if self.relay_to_online_friends(target_id, payload):
                            self.store.mark_message_status(msg_id, "已交给中转好友")
                    with self.store.lock:
                        for p in self.store.pending:
                            if p.get("msg_id") == msg_id:
                                p["last_try"] = item.get("last_try", 0)
                                p["relay_try"] = item.get("relay_try", 0)
                        self.store.save_all()

                # 再处理“帮别人中转”的离线消息。只要目标或下一跳在线，就继续传。
                with self.store.lock:
                    relay_copy = list(self.store.relay_pending)
                for item in relay_copy:
                    target_id = item.get("target_id")
                    payload = item.get("payload") or {}
                    msg_id = item.get("msg_id") or payload.get("msg_id")
                    now = now_ts()
                    if now - float(item.get("last_try", 0)) < 5:
                        continue
                    item["last_try"] = now
                    if self.send_direct_to_user(target_id, payload):
                        self.store.remove_relay_pending(msg_id)
                        continue
                    if now - float(item.get("relay_try", 0)) >= 12:
                        item["relay_try"] = now
                        self.relay_to_online_friends(target_id, payload, exclude_id=item.get("source_id"))
                    with self.store.lock:
                        for p in self.store.relay_pending:
                            if p.get("msg_id") == msg_id:
                                p["last_try"] = item.get("last_try", 0)
                                p["relay_try"] = item.get("relay_try", 0)
                        self.store.save_all()
            except Exception:
                pass
            time.sleep(2)


class BeiyangApp:
    def __init__(self, http_port: int, tcp_port: int):
        AppConfig.http_port = http_port
        self.store = Store(tcp_port)
        self.network = Network(self)

    def start(self) -> None:
        self.network.start()

    def handle_payload(self, payload: dict, ip: str) -> None:
        mtype = payload.get("type")

        # 所有带 from_id 的消息，都顺手更新一次在线信息，解决 IP 改变问题
        from_id = payload.get("from_id") or payload.get("user_id")
        if from_id:
            self.store.update_peer({
                "user_id": from_id,
                "nickname": payload.get("from_name") or payload.get("nickname") or "同学",
                "bio": payload.get("from_bio") or payload.get("bio") or "",
                "tcp_port": payload.get("from_port") or payload.get("tcp_port") or DEFAULT_TCP_PORT,
            }, ip)

        if mtype == "friend_request":
            self.store.add_request({
                "user_id": payload.get("from_id"),
                "nickname": payload.get("from_name"),
                "bio": payload.get("from_bio", ""),
                "tcp_port": payload.get("from_port", DEFAULT_TCP_PORT),
            }, ip)
            return

        if mtype == "friend_response":
            if payload.get("accepted"):
                self.store.add_friend({
                    "user_id": payload.get("from_id"),
                    "nickname": payload.get("from_name", "好友"),
                    "bio": payload.get("from_bio", ""),
                    "tcp_port": payload.get("from_port", DEFAULT_TCP_PORT),
                }, ip)
                self.store.append_message(payload.get("from_id"), {
                    "msg_id": make_msg_id(),
                    "direction": "system",
                    "text": f"你和 {payload.get('from_name', '对方')} 已成为好友。",
                    "time": now_text(),
                    "status": "系统消息",
                })
            return

        if mtype == "chat":
            self.handle_chat_payload(payload, ip)
            return

        if mtype == "delivery_ack":
            target_id = payload.get("to_id")
            if target_id == self.store.user_id:
                ack_msg_id = payload.get("ack_msg_id")
                self.store.remove_pending(ack_msg_id)
                self.store.mark_message_status(ack_msg_id, "已送达")
            else:
                # 帮别人转发回执
                self.network.send_or_queue(target_id, payload, queue_if_fail=False)
            return

        if mtype == "relay":
            relay_id = payload.get("msg_id")
            if relay_id in self.store.seen_ids:
                return
            self.store.seen_ids.add(relay_id)
            target_id = payload.get("target_id")
            inner = payload.get("payload") or {}
            if target_id == self.store.user_id:
                self.handle_payload(inner, ip)
            elif int(payload.get("hops", 0)) <= 3:
                # 我知道目标在线就直发；如果目标不在线，就暂存起来，等目标或其他中间好友上线后继续传播
                if not self.network.send_direct_to_user(target_id, inner):
                    self.store.add_relay_pending(inner, target_id, payload.get("from_id"))
                    self.network.relay_to_online_friends(target_id, inner, exclude_id=payload.get("from_id"))
            return

    def handle_chat_payload(self, payload: dict, ip: str) -> None:
        to_id = payload.get("to_id")
        from_id = payload.get("from_id")
        if to_id != self.store.user_id:
            # 不是发给我的，尝试帮忙转发
            self.network.send_or_queue(to_id, payload, queue_if_fail=False)
            return
        if from_id not in self.store.friends:
            # 未成为好友时不展示普通聊天，避免陌生人骚扰
            return
        is_new = self.store.append_message(from_id, {
            "msg_id": payload.get("msg_id"),
            "from_id": from_id,
            "to_id": to_id,
            "direction": "in",
            "text": payload.get("text", ""),
            "time": payload.get("time") or now_text(),
            "status": "已接收",
        })
        if is_new:
            # 发送送达回执，能直连就直连，不行就找共同在线好友中转
            ack = {
                "type": "delivery_ack",
                "from_id": self.store.user_id,
                "from_name": self.store.profile.get("nickname", ""),
                "from_bio": self.store.profile.get("bio", ""),
                "from_port": self.store.profile.get("tcp_port", DEFAULT_TCP_PORT),
                "to_id": from_id,
                "ack_msg_id": payload.get("msg_id"),
                "msg_id": make_msg_id(),
            }
            self.network.send_or_queue(from_id, ack, queue_if_fail=False)

    def send_friend_request(self, peer_id: str) -> tuple[bool, str]:
        online = self.store.online_peers()
        peer = online.get(peer_id)
        if not peer:
            return False, "对方当前不在线，无法发送好友申请。"
        payload = {
            "type": "friend_request",
            "from_id": self.store.user_id,
            "from_name": self.store.profile.get("nickname", ""),
            "from_bio": self.store.profile.get("bio", ""),
            "from_port": self.store.profile.get("tcp_port", DEFAULT_TCP_PORT),
            "msg_id": make_msg_id(),
        }
        ok = Network.send_tcp(peer.get("ip"), peer.get("tcp_port"), payload)
        return ok, "好友申请已发送。" if ok else "发送失败，请确认在同一局域网并允许防火墙。"

    def respond_friend_request(self, user_id: str, accept: bool) -> tuple[bool, str]:
        req = self.store.requests.get(user_id)
        if not req:
            return False, "没有找到这条好友申请。"
        self.store.set_request_status(user_id, "accepted" if accept else "rejected")
        if accept:
            self.store.add_friend(req, req.get("ip"))
        payload = {
            "type": "friend_response",
            "from_id": self.store.user_id,
            "from_name": self.store.profile.get("nickname", ""),
            "from_bio": self.store.profile.get("bio", ""),
            "from_port": self.store.profile.get("tcp_port", DEFAULT_TCP_PORT),
            "accepted": bool(accept),
            "msg_id": make_msg_id(),
        }
        ok = Network.send_tcp(req.get("ip"), req.get("tcp_port"), payload)
        return True, "已同意好友申请。" if accept else "已拒绝好友申请。"

    def send_chat(self, friend_id: str, text: str) -> tuple[bool, str]:
        text = text.strip()
        if not text:
            return False, "消息不能为空。"
        if friend_id not in self.store.friends:
            return False, "只能给好友发送消息。"
        msg_id = make_msg_id()
        payload = {
            "type": "chat",
            "from_id": self.store.user_id,
            "from_name": self.store.profile.get("nickname", ""),
            "from_bio": self.store.profile.get("bio", ""),
            "from_port": self.store.profile.get("tcp_port", DEFAULT_TCP_PORT),
            "to_id": friend_id,
            "text": text,
            "time": now_text(),
            "msg_id": msg_id,
        }
        self.store.append_message(friend_id, {
            "msg_id": msg_id,
            "from_id": self.store.user_id,
            "to_id": friend_id,
            "direction": "out",
            "text": text,
            "time": payload["time"],
            "status": "发送中",
        })
        result = self.network.send_or_queue(friend_id, payload, queue_if_fail=True)
        if result == "direct":
            self.store.mark_message_status(msg_id, "已直接送达")
            return True, "消息已直接发送。"
        if result == "relay":
            self.store.mark_message_status(msg_id, "已交给中转好友")
            return True, "对方暂时不在线，消息已尝试交给在线好友中转。"
        self.store.mark_message_status(msg_id, "等待对方上线")
        return True, "对方暂时不在线，消息已进入待发送队列。"


HTML_PAGE = r"""
<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>相识北洋 - Python版</title>
<style>
:root{--bg:#f5f7fb;--card:#ffffff;--line:#e7eaf1;--text:#1d2433;--muted:#687386;--blue:#2f6bff;--green:#14a06f;--red:#e04f5f;--shadow:0 10px 30px rgba(16,24,40,.08)}
*{box-sizing:border-box}body{margin:0;background:linear-gradient(135deg,#eef4ff,#f8fbff 40%,#f7f3ff);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Microsoft YaHei",Arial,sans-serif;color:var(--text)}
header{padding:22px 28px;display:flex;align-items:center;justify-content:space-between}header h1{margin:0;font-size:28px;letter-spacing:.5px}.tag{font-size:13px;color:var(--muted)}
.container{display:grid;grid-template-columns:330px 1fr;gap:18px;padding:0 22px 22px;max-width:1250px;margin:0 auto}.card{background:rgba(255,255,255,.88);backdrop-filter:blur(12px);border:1px solid rgba(255,255,255,.75);box-shadow:var(--shadow);border-radius:18px;padding:16px;margin-bottom:16px}.card h2{font-size:17px;margin:0 0 12px}.small{font-size:12px;color:var(--muted);line-height:1.6}.row{display:flex;gap:8px;align-items:center}.col{display:flex;flex-direction:column;gap:8px}input,textarea{width:100%;border:1px solid var(--line);border-radius:12px;padding:10px 12px;font-size:14px;background:#fff;color:var(--text)}textarea{min-height:66px;resize:vertical}button{border:0;border-radius:12px;background:var(--blue);color:white;padding:9px 12px;cursor:pointer;font-weight:600}button.secondary{background:#edf2ff;color:#1c4fd7}button.danger{background:#ffeef1;color:#bf2037}button.green{background:#e9fff6;color:#087a54}button:disabled{opacity:.45;cursor:not-allowed}.list{display:flex;flex-direction:column;gap:10px}.item{padding:12px;border:1px solid var(--line);border-radius:14px;background:#fff}.item-title{font-weight:700;display:flex;justify-content:space-between;gap:8px}.bio{font-size:13px;color:var(--muted);margin-top:4px;word-break:break-word}.online{color:var(--green);font-size:12px}.offline{color:var(--muted);font-size:12px}.main{min-width:0}.chat-card{height:calc(100vh - 150px);min-height:620px;display:flex;flex-direction:column}.chat-top{padding-bottom:12px;border-bottom:1px solid var(--line);display:flex;align-items:center;justify-content:space-between}.chat-title{font-size:20px;font-weight:800}.messages{flex:1;overflow:auto;padding:18px 4px;display:flex;flex-direction:column;gap:12px}.msg{max-width:75%;padding:10px 12px;border-radius:16px;line-height:1.45;word-break:break-word}.msg.in{align-self:flex-start;background:#fff;border:1px solid var(--line)}.msg.out{align-self:flex-end;background:#2f6bff;color:white}.msg.system{align-self:center;background:#eef2f7;color:var(--muted);font-size:13px}.meta{font-size:11px;opacity:.75;margin-top:5px}.sendbar{display:flex;gap:10px;border-top:1px solid var(--line);padding-top:12px}.sendbar input{flex:1}.pill{display:inline-block;border-radius:999px;background:#eef2ff;color:#2f55ca;padding:3px 8px;font-size:12px}.toast{position:fixed;left:50%;bottom:24px;transform:translateX(-50%);background:#1d2433;color:#fff;padding:10px 14px;border-radius:12px;box-shadow:var(--shadow);display:none;z-index:5}.empty{color:var(--muted);text-align:center;padding:55px 0}.friend-btn{width:100%;text-align:left;background:#fff;color:var(--text);border:1px solid var(--line);font-weight:500}.friend-btn.active{border-color:#2f6bff;background:#edf2ff}.split{display:grid;grid-template-columns:1fr 1fr;gap:8px}@media(max-width:850px){.container{grid-template-columns:1fr;padding:0 12px 18px}header{padding:18px 14px}.chat-card{height:70vh;min-height:520px}.msg{max-width:88%}}
</style>
</head>
<body>
<header>
  <div><h1>相识北洋</h1><div class="tag">Python 局域网交友 / 好友聊天 / 离线中转</div></div>
  <div class="tag" id="netInfo">正在读取网络状态...</div>
</header>
<div class="container">
  <aside>
    <section class="card">
      <h2>我的介绍</h2>
      <div class="col">
        <input id="nickname" placeholder="昵称" />
        <textarea id="bio" placeholder="自我介绍，例如：计科大三，喜欢 Java / Python / 健身"></textarea>
        <button onclick="saveProfile()">保存个人介绍</button>
        <div class="small" id="myId"></div>
      </div>
    </section>

    <section class="card">
      <h2>好友申请</h2>
      <div class="list" id="requests"></div>
    </section>

    <section class="card">
      <h2>在线同学</h2>
      <div class="small">同一校园网/Wi-Fi 下会自动出现。若看不到对方，请检查防火墙。</div><br>
      <div class="list" id="classmates"></div>
    </section>

    <section class="card">
      <h2>我的好友</h2>
      <div class="list" id="friends"></div>
    </section>
  </aside>

  <main class="main">
    <section class="card chat-card">
      <div class="chat-top">
        <div><div class="chat-title" id="chatTitle">请选择一个好友</div><div class="small" id="chatSub">成为好友后，即使 IP 改变，也会通过固定身份 ID 自动识别。</div></div>
        <span class="pill" id="pendingInfo">待发送 0</span>
      </div>
      <div class="messages" id="messages"><div class="empty">左侧选择好友开始聊天</div></div>
      <div class="sendbar">
        <input id="messageInput" placeholder="输入消息，回车发送" onkeydown="if(event.key==='Enter')sendMessage()" />
        <button onclick="sendMessage()">发送</button>
      </div>
    </section>
  </main>
</div>
<div class="toast" id="toast"></div>
<script>
let state = null;
let currentFriendId = null;
let lastMessageCount = 0;

function esc(s){return String(s ?? '').replace(/[&<>'"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c]));}
function toast(msg){const t=document.getElementById('toast');t.textContent=msg;t.style.display='block';setTimeout(()=>t.style.display='none',2200);}
async function api(path, data){
  const opt = data===undefined ? {} : {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)};
  const r = await fetch(path,opt); return await r.json();
}
async function refresh(){
  try{
    state = await api('/api/state');
    renderState();
    if(currentFriendId) await loadChat(currentFriendId, false);
  }catch(e){console.error(e)}
}
function renderState(){
  const p=state.profile;
  if(document.activeElement.id!=='nickname') document.getElementById('nickname').value=p.nickname||'';
  if(document.activeElement.id!=='bio') document.getElementById('bio').value=p.bio||'';
  document.getElementById('myId').innerHTML=`身份ID：${esc(p.user_id.slice(0,12))}...<br>本机：${esc(state.local_ip)}:${state.http_port}，消息端口：${p.tcp_port}`;
  document.getElementById('netInfo').textContent=`本机访问：http://${state.local_ip}:${state.http_port}`;
  document.getElementById('pendingInfo').textContent=`待发送 ${state.pending_count}｜中转 ${state.relay_pending_count||0}`;

  const reqBox=document.getElementById('requests');
  reqBox.innerHTML = state.requests.length ? state.requests.map(r=>`
    <div class="item"><div class="item-title">${esc(r.nickname)}<span class="online">申请中</span></div><div class="bio">${esc(r.bio)}</div><div class="row" style="margin-top:8px"><button class="green" onclick="respond('${r.user_id}',true)">同意</button><button class="danger" onclick="respond('${r.user_id}',false)">拒绝</button></div></div>
  `).join('') : '<div class="small">暂无好友申请</div>';

  const cBox=document.getElementById('classmates');
  const classmates=state.classmates.filter(x=>x.user_id!==p.user_id);
  cBox.innerHTML = classmates.length ? classmates.map(c=>`
    <div class="item"><div class="item-title">${esc(c.nickname)}<span class="online">在线</span></div><div class="bio">${esc(c.bio)}<br>${esc(c.ip)}:${c.tcp_port}</div><div style="margin-top:8px"><button class="secondary" ${c.is_friend?'disabled':''} onclick="addFriend('${c.user_id}')">${c.is_friend?'已是好友':'发送好友申请'}</button></div></div>
  `).join('') : '<div class="small">还没有发现在线同学。请让对方也运行本程序，并连接同一 Wi-Fi。</div>';

  const fBox=document.getElementById('friends');
  const categories=['默认分组','同班同学','室友','社团朋友','课程搭子','其他'];
  fBox.innerHTML = state.friends.length ? state.friends.map(f=>`
    <div class="item friend-btn ${currentFriendId===f.user_id?'active':''}">
      <div onclick="selectFriend('${f.user_id}')" style="cursor:pointer">
        <div class="item-title">${esc(f.nickname)}<span class="${f.online?'online':'offline'}">${f.online?'在线':'离线'}</span></div>
        <div class="bio">${esc(f.bio || '暂无介绍')}<br>分类：${esc(f.category||'默认分组')}</div>
      </div>
      <div class="row" style="margin-top:8px">
        <select onchange="setCategory('${f.user_id}',this.value)" style="flex:1;border:1px solid var(--line);border-radius:10px;padding:7px;background:#fff">
          ${categories.map(c=>`<option value="${esc(c)}" ${(f.category||'默认分组')===c?'selected':''}>${esc(c)}</option>`).join('')}
        </select>
        <button class="danger" onclick="deleteFriend('${f.user_id}')">删除</button>
      </div>
    </div>
  `).join('') : '<div class="small">暂无好友。先从“在线同学”里发送好友申请。</div>';

  if(currentFriendId){
    const f=state.friends.find(x=>x.user_id===currentFriendId);
    if(f){
      document.getElementById('chatTitle').textContent=f.nickname;
      document.getElementById('chatSub').textContent=(f.online?'当前在线，可以直接发送':'当前离线，消息会等待上线或尝试通过共同好友中转');
    }
  }
}
async function saveProfile(){
  const res=await api('/api/profile',{nickname:document.getElementById('nickname').value,bio:document.getElementById('bio').value});
  toast(res.message||'已保存'); refresh();
}
async function addFriend(id){const res=await api('/api/friend_request',{peer_id:id}); toast(res.message); refresh();}
async function respond(id,accept){const res=await api('/api/respond_friend',{user_id:id,accept}); toast(res.message); refresh();}
async function setCategory(id,category){const res=await api('/api/update_category',{friend_id:id,category}); toast(res.message); refresh();}
async function deleteFriend(id){if(!confirm('确定删除这个好友吗？'))return; const res=await api('/api/delete_friend',{friend_id:id}); toast(res.message); if(currentFriendId===id)currentFriendId=null; refresh();}
async function editPending(msgId,oldText){const text=prompt('修改这条待发送消息：',oldText||''); if(text===null)return; const res=await api('/api/edit_pending',{msg_id:msgId,text}); toast(res.message); if(currentFriendId)loadChat(currentFriendId,true); refresh();}
function selectFriend(id){currentFriendId=id; lastMessageCount=0; renderState(); loadChat(id,true);}
async function loadChat(id, forceScroll){
  const data=await api('/api/chat?friend_id='+encodeURIComponent(id));
  const box=document.getElementById('messages');
  if(!data.messages.length){box.innerHTML='<div class="empty">还没有聊天记录，发一句打招呼吧</div>';return;}
  const oldCount=lastMessageCount; lastMessageCount=data.messages.length;
  box.innerHTML=data.messages.map(m=>{
    const cls=m.direction==='out'?'out':(m.direction==='system'?'system':'in');
    const canEdit = m.direction==='out' && ['发送中','等待对方上线'].includes(m.status||'');
    const editBtn = canEdit ? `<button class="secondary" style="margin-top:6px;padding:5px 8px" onclick="editPending('${esc(m.msg_id)}','${esc(String(m.text||'').replace(/\n/g,' '))}')">编辑待发</button>` : '';
    return `<div class="msg ${cls}"><div>${esc(m.text)}</div><div class="meta">${esc(m.time||'')} ${esc(m.status||'')}</div>${editBtn}</div>`;
  }).join('');
  if(forceScroll || data.messages.length!==oldCount) box.scrollTop=box.scrollHeight;
}
async function sendMessage(){
  if(!currentFriendId){toast('请先选择好友');return;}
  const inp=document.getElementById('messageInput'); const text=inp.value.trim(); if(!text)return;
  inp.value=''; const res=await api('/api/send',{friend_id:currentFriendId,text}); toast(res.message); await loadChat(currentFriendId,true); refresh();
}
refresh(); setInterval(refresh,2000);
</script>
</body>
</html>
"""


class WebHandler(BaseHTTPRequestHandler):
    app: BeiyangApp = None

    def log_message(self, format, *args):
        # 减少控制台日志干扰
        return

    def _send_json(self, data, code=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/":
            body = HTML_PAGE.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path == "/api/state":
            self._send_json(self.app.store.state_snapshot())
            return
        if parsed.path == "/api/chat":
            qs = parse_qs(parsed.query)
            fid = qs.get("friend_id", [""])[0]
            with self.app.store.lock:
                messages = self.app.store.messages.get(fid, [])
            self._send_json({"messages": messages})
            return
        self._send_json({"ok": False, "message": "not found"}, 404)

    def do_POST(self):
        parsed = urlparse(self.path)
        try:
            data = self._read_json()
            if parsed.path == "/api/profile":
                self.app.store.update_profile(data.get("nickname", ""), data.get("bio", ""))
                self._send_json({"ok": True, "message": "个人介绍已保存。"})
                return
            if parsed.path == "/api/friend_request":
                ok, msg = self.app.send_friend_request(data.get("peer_id", ""))
                self._send_json({"ok": ok, "message": msg})
                return
            if parsed.path == "/api/respond_friend":
                ok, msg = self.app.respond_friend_request(data.get("user_id", ""), bool(data.get("accept")))
                self._send_json({"ok": ok, "message": msg})
                return
            if parsed.path == "/api/send":
                ok, msg = self.app.send_chat(data.get("friend_id", ""), data.get("text", ""))
                self._send_json({"ok": ok, "message": msg})
                return
            if parsed.path == "/api/delete_friend":
                self.app.store.delete_friend(data.get("friend_id", ""))
                self._send_json({"ok": True, "message": "好友已删除。"})
                return
            if parsed.path == "/api/update_category":
                ok = self.app.store.update_friend_category(data.get("friend_id", ""), data.get("category", "默认分组"))
                self._send_json({"ok": ok, "message": "好友分类已更新。" if ok else "没有找到该好友。"})
                return
            if parsed.path == "/api/edit_pending":
                ok = self.app.store.edit_pending_message(data.get("msg_id", ""), data.get("text", ""))
                self._send_json({"ok": ok, "message": "待发送消息已修改。" if ok else "这条消息可能已经送达，不能再编辑。"})
                return
            self._send_json({"ok": False, "message": "not found"}, 404)
        except Exception as e:
            self._send_json({"ok": False, "message": str(e)}, 500)


def main():
    parser = argparse.ArgumentParser(description="相识北洋 Python 局域网聊天程序")
    parser.add_argument("--http-port", type=int, default=DEFAULT_HTTP_PORT, help="网页界面端口，默认 8765")
    parser.add_argument("--tcp-port", type=int, default=0, help="消息服务端口，默认自动寻找 50124 附近可用端口")
    parser.add_argument("--no-browser", action="store_true", help="启动时不自动打开浏览器")
    args = parser.parse_args()

    tcp_port = args.tcp_port or find_free_port(DEFAULT_TCP_PORT)
    http_port = args.http_port
    app = BeiyangApp(http_port=http_port, tcp_port=tcp_port)
    app.start()

    WebHandler.app = app
    server = ThreadingHTTPServer(("", http_port), WebHandler)
    local_ip = get_local_ip()
    print("=" * 60)
    print("相识北洋 Python 版已启动")
    print(f"本机浏览器打开：http://127.0.0.1:{http_port}")
    print(f"同设备局域网地址：http://{local_ip}:{http_port}")
    print(f"UDP 发现端口：{UDP_DISCOVERY_PORT}，TCP 消息端口：{tcp_port}")
    print("提示：两台设备需要连接同一 Wi-Fi/校园网；Windows 防火墙请点允许。")
    print("按 Ctrl + C 退出。")
    print("=" * 60)

    if not args.no_browser:
        try:
            webbrowser.open(f"http://127.0.0.1:{http_port}")
        except Exception:
            pass

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已退出。")


if __name__ == "__main__":
    main()
