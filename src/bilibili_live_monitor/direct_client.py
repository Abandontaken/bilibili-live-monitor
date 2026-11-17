"""免 Chrome 匿名直连采集客户端：游客凭证与 WBI 签名缓存、全局建连限速、RFC 6455 弹幕连接及流地址解析。"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import math
import os
import ssl
import struct
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import certifi
import httpx

from .browser import dispatch_messages, room_url
from .rotation import RoomStatusRefused, index_rooms
from .store import utc_now

USER_AGENT = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
)

MIXIN_KEY_ENC_TAB = (
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35, 27, 43, 5, 49,
    33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13, 37, 48, 7, 16, 24, 55, 40,
    61, 26, 17, 0, 1, 60, 51, 30, 4, 22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11,
    36, 20, 34, 44, 52,
)

GUEST_TTL_SECONDS = 12 * 3600.0
MAX_WS_MESSAGE_BYTES = 8 * 1024 * 1024
_WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'
_SHANGHAI = ZoneInfo('Asia/Shanghai')


def get_mixin_key(orig: str) -> str:
    if len(orig) < 64:
        raise ValueError('WBI 原始密钥长度不足64字符')
    return ''.join(orig[i] for i in MIXIN_KEY_ENC_TAB)[:32]


def sign_wbi(params: dict[str, Any], img_key: str, sub_key: str, *, now: float | None = None) -> dict[str, str]:
    mixin_key = get_mixin_key(img_key + sub_key)
    wts = round(time.time() if now is None else now)
    signed = dict(sorted({**params, 'wts': wts}.items()))
    cleaned = {
        str(k): ''.join(ch for ch in str(v) if ch not in "!'()*")
        for k, v in signed.items()
    }
    query = urlencode(cleaned)
    cleaned['w_rid'] = hashlib.md5((query + mixin_key).encode('utf-8')).hexdigest()
    return cleaned


def pack_bili_packet(operation: int, body: bytes, protover: int = 1, seq: int = 1) -> bytes:
    return struct.pack('>IHHII', 16 + len(body), 16, protover, operation, seq) + body


def iter_bili_envelopes(frame: bytes):
    """按 B 站 16 字节头部逐包遍历外层二进制帧，产出 (operation, body_bytes)。"""
    offset = 0
    while offset + 16 <= len(frame):
        size, hdr, _, bop, _ = struct.unpack_from('>IHHII', frame, offset)
        if not 16 <= hdr <= size or offset + size > len(frame):
            return
        yield bop, frame[offset + hdr:offset + size]
        offset += size


def _format_shanghai_live_time(raw_value: Any) -> str:
    if isinstance(raw_value, str):
        text = raw_value.strip()
        if text and text != '0000-00-00 00:00:00':
            try:
                datetime.strptime(text, '%Y-%m-%d %H:%M:%S')
                return text
            except ValueError:
                if text.isdigit() and int(text) > 0:
                    return datetime.fromtimestamp(int(text), _SHANGHAI).strftime('%Y-%m-%d %H:%M:%S')
        return ''
    if isinstance(raw_value, (int, float)) and not isinstance(raw_value, bool) and raw_value > 0:
        return datetime.fromtimestamp(int(raw_value), _SHANGHAI).strftime('%Y-%m-%d %H:%M:%S')
    return ''


@dataclass(frozen=True)
class GuestIdentity:
    buvid3: str
    buvid4: str
    img_key: str
    sub_key: str
    fetched_mono: float


class MinimalAsyncWebSocket:
    """零额外第三方依赖的异步客户端 RFC 6455 实现（支持分片重组、控制帧穿插与并发写锁）。"""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        self._frag_opcode: int | None = None
        self._frag_buf = bytearray()
        self._write_lock = asyncio.Lock()
        self._closed = False

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        path: str = '/sub',
        *,
        timeout: float = 15.0,
        use_tls: bool = True,
        open_connection: Callable[..., Any] | None = None,
    ) -> MinimalAsyncWebSocket:
        connector = open_connection or asyncio.open_connection
        kwargs: dict[str, Any] = {}
        if use_tls:
            kwargs['ssl'] = ssl.create_default_context(cafile=certifi.where())
            kwargs['server_hostname'] = host
        reader, writer = await asyncio.wait_for(connector(host, port, **kwargs), timeout=timeout)
        try:
            ws_key = base64.b64encode(os.urandom(16)).decode('ascii')
            request = (
                f'GET {path} HTTP/1.1\r\n'
                f'Host: {host}:{port}\r\n'
                'Upgrade: websocket\r\n'
                'Connection: Upgrade\r\n'
                f'Sec-WebSocket-Key: {ws_key}\r\n'
                'Sec-WebSocket-Version: 13\r\n'
                'Origin: https://live.bilibili.com\r\n'
                f'User-Agent: {USER_AGENT}\r\n\r\n'
            )
            writer.write(request.encode('ascii'))
            await asyncio.wait_for(writer.drain(), timeout=timeout)

            status_raw = await asyncio.wait_for(reader.readline(), timeout=timeout)
            if len(status_raw) > 8192:
                raise RuntimeError('WebSocket 握手状态行超长')
            status_line = status_raw.decode('latin1', errors='replace').strip()
            headers: dict[str, str] = {}
            for _ in range(64):
                line = await asyncio.wait_for(reader.readline(), timeout=timeout)
                if len(line) > 8192:
                    raise RuntimeError('WebSocket 握手响应头超长')
                if line in (b'\r\n', b'\n', b''):
                    break
                text = line.decode('latin1', errors='replace').strip()
                if ':' in text:
                    key, val = text.split(':', 1)
                    headers[key.strip().lower()] = val.strip()
            else:
                raise RuntimeError('WebSocket 握手响应头行数过多')

            parts = status_line.split(' ', 2)
            code = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else 0
            if code in (403, 412, 429):
                raise RuntimeError(f'recording_refused: WebSocket HTTP {code}')
            if code != 101:
                raise RuntimeError(f'WebSocket 握手失败: {status_line}')
            expected_accept = base64.b64encode(
                hashlib.sha1((ws_key + _WS_GUID).encode('ascii')).digest()
            ).decode('ascii')
            if headers.get('sec-websocket-accept') != expected_accept:
                raise RuntimeError('WebSocket Sec-WebSocket-Accept 校验不匹配')
            return cls(reader, writer)
        except BaseException:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            raise

    async def send_bytes(self, payload: bytes, opcode: int = 0x2) -> None:
        mask_key = os.urandom(4)
        header = bytearray([0x80 | (opcode & 0x0F)])
        length = len(payload)
        if length < 126:
            header.append(0x80 | length)
        elif length < 65536:
            header.append(0x80 | 126)
            header.extend(struct.pack('>H', length))
        else:
            header.append(0x80 | 127)
            header.extend(struct.pack('>Q', length))
        header.extend(mask_key)
        masked = bytes(b ^ mask_key[i & 3] for i, b in enumerate(payload))
        async with self._write_lock:
            self.writer.write(bytes(header) + masked)
            await self.writer.drain()

    async def recv_message(self) -> tuple[int, bytes]:
        while True:
            b1, b2 = await self.reader.readexactly(2)
            fin = bool(b1 & 0x80)
            rsv = (b1 >> 4) & 0x07
            opcode = b1 & 0x0F
            masked = bool(b2 & 0x80)
            length = b2 & 0x7F
            if rsv != 0:
                raise ValueError(f'WebSocket 收到未协商的 RSV 标志位: {rsv}')
            if masked:
                raise ValueError('WebSocket 服务端帧不得携带掩码')
            if opcode not in (0x0, 0x1, 0x2, 0x8, 0x9, 0xA):
                raise ValueError(f'WebSocket 收到保留或未知操作码: {opcode}')
            if opcode >= 0x8 and (not fin or length > 125):
                raise ValueError(f'WebSocket 控制帧违反 RFC 6455 约束: opcode={opcode} fin={fin} len={length}')
            if length == 126:
                length = struct.unpack('>H', await self.reader.readexactly(2))[0]
            elif length == 127:
                length = struct.unpack('>Q', await self.reader.readexactly(8))[0]
            if length > MAX_WS_MESSAGE_BYTES:
                raise ValueError(f'WebSocket 单帧超过大小上限: {length}')
            data = await self.reader.readexactly(length) if length else b''

            # RFC 6455 控制帧可穿插在分片消息中间，优先独立处理。
            if opcode == 0x9:
                await self.send_bytes(data[:125], opcode=0xA)
                continue
            if opcode == 0xA:
                continue
            if opcode == 0x8:
                code = struct.unpack('>H', data[:2])[0] if len(data) >= 2 else 1000
                reason = data[2:].decode('utf-8', errors='replace') if len(data) > 2 else ''
                try:
                    await self.send_bytes(struct.pack('>H', 1000), opcode=0x8)
                except Exception:
                    pass
                raise EOFError(f'WebSocket 服务端关闭连接: code={code} reason={reason}')

            if opcode == 0x0:
                if self._frag_opcode is None:
                    raise ValueError('收到无起始帧的 WebSocket 延续帧')
                if len(self._frag_buf) + len(data) > MAX_WS_MESSAGE_BYTES:
                    raise ValueError('WebSocket 分片重组超过大小上限')
                self._frag_buf.extend(data)
                if fin:
                    full = bytes(self._frag_buf)
                    op = self._frag_opcode
                    self._frag_buf.clear()
                    self._frag_opcode = None
                    return op, full
                continue

            if self._frag_opcode is not None:
                raise ValueError('WebSocket 分片未结束时收到新数据帧')
            if not fin:
                self._frag_opcode = opcode
                self._frag_buf = bytearray(data)
                continue
            return opcode, data

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await asyncio.wait_for(self.send_bytes(struct.pack('>H', 1000), opcode=0x8), timeout=2.0)
        except Exception:
            pass
        try:
            self.writer.close()
            await asyncio.wait_for(self.writer.wait_closed(), timeout=2.0)
        except Exception:
            pass


class DirectClient:
    """免浏览器 B 站直播只读直连客户端（强制 uid=0 匿名态）。"""

    def __init__(
        self,
        *,
        handshake_interval: float = 2.5,
        guest_ttl: float = GUEST_TTL_SECONDS,
        http_client: httpx.AsyncClient | None = None,
        ws_connector: Callable[..., Any] | None = None,
    ):
        self.handshake_interval = max(0.0, float(handshake_interval))
        self.guest_ttl = guest_ttl
        self._external_http = http_client is not None
        self._http = http_client
        self._ws_connector = ws_connector
        self._guest: GuestIdentity | None = None
        self._guest_lock = asyncio.Lock()
        self._handshake_lock = asyncio.Lock()
        self._last_handshake_mono: float | None = None
        self._ws_connect_lock = asyncio.Lock()
        self._last_ws_connect_mono: float | None = None

    def _client(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(
                headers={'User-Agent': USER_AGENT, 'Referer': 'https://live.bilibili.com/'},
                timeout=20.0,
                follow_redirects=True,
            )
        return self._http

    async def _acquire_interval_slot(self, lock: asyncio.Lock, attr_name: str) -> float:
        async with lock:
            now = time.monotonic()
            waited = 0.0
            last_mono = getattr(self, attr_name)
            if last_mono is not None and self.handshake_interval > 0:
                deficit = self.handshake_interval - (now - last_mono)
                if deficit > 0:
                    await asyncio.sleep(deficit)
                    waited = deficit
                    now = time.monotonic()
            setattr(self, attr_name, now)
            return waited

    async def acquire_handshake_slot(self) -> float:
        """全局异步 HTTP 握手限速锁：确保多房间并发调鉴权/流地址接口时至少间隔 handshake_interval 秒。"""
        return await self._acquire_interval_slot(self._handshake_lock, '_last_handshake_mono')

    async def acquire_ws_connect_slot(self) -> float:
        """全局异步 WSS 建连限速锁：确保每一次实际 WebSocket 建连（含多房错峰与备用主机回退）至少间隔 handshake_interval 秒。"""
        return await self._acquire_interval_slot(self._ws_connect_lock, '_last_ws_connect_mono')

    async def ensure_guest(self, *, force_refresh: bool = False) -> GuestIdentity:
        async with self._guest_lock:
            now = time.monotonic()
            if (
                not force_refresh
                and self._guest is not None
                and (now - self._guest.fetched_mono) < self.guest_ttl
            ):
                return self._guest
            client = self._client()
            spi_resp = await client.get('https://api.bilibili.com/x/frontend/finger/spi')
            if spi_resp.status_code in (403, 412, 429):
                raise RuntimeError(f'recording_refused: finger/spi HTTP {spi_resp.status_code}')
            spi_resp.raise_for_status()
            spi_payload = spi_resp.json()
            if spi_payload.get('code') != 0:
                raise RuntimeError(f'获取游客 buvid 失败: code={spi_payload.get("code")}')
            spi_data = spi_payload.get('data') or {}
            buvid3 = str(spi_data.get('b_3') or '').strip()
            buvid4 = str(spi_data.get('b_4') or '').strip()
            if not buvid3:
                raise RuntimeError('finger/spi 未返回有效 buvid3')

            nav_resp = await client.get(
                'https://api.bilibili.com/x/web-interface/nav',
                headers={'Cookie': f'buvid3={buvid3}; buvid4={buvid4}'},
            )
            if nav_resp.status_code in (403, 412, 429):
                raise RuntimeError(f'recording_refused: nav HTTP {nav_resp.status_code}')
            nav_resp.raise_for_status()
            nav_payload = nav_resp.json()
            nav_data = nav_payload.get('data') or {}
            if nav_data.get('isLogin') is True:
                raise RuntimeError('direct 模式检测到意外登录态，拒绝继续')
            wbi_img = nav_data.get('wbi_img') or {}
            img_url = str(wbi_img.get('img_url') or '')
            sub_url = str(wbi_img.get('sub_url') or '')
            img_key = img_url.rsplit('/', 1)[-1].split('.', 1)[0]
            sub_key = sub_url.rsplit('/', 1)[-1].split('.', 1)[0]
            if len(img_key) < 32 or len(sub_key) < 32:
                raise RuntimeError('nav 未返回有效 WBI 密钥')

            self._guest = GuestIdentity(
                buvid3=buvid3,
                buvid4=buvid4,
                img_key=img_key,
                sub_key=sub_key,
                fetched_mono=time.monotonic(),
            )
            return self._guest

    async def resolve_room(self, value: str) -> dict[str, Any]:
        """解析输入房间链接或房号，返回真实长号、短号映射、采集主键及基础元数据。"""
        source = room_url(value)
        requested_id = source.rsplit('/', 1)[-1]
        client = self._client()
        init_resp = await client.get(
            'https://api.live.bilibili.com/room/v1/Room/room_init',
            params={'id': requested_id},
        )
        if init_resp.status_code in (403, 412, 429):
            raise RuntimeError(f'recording_refused: HTTP {init_resp.status_code}')
        if init_resp.status_code != 200:
            raise RuntimeError(f'room_init HTTP {init_resp.status_code}')
        init_payload = init_resp.json()
        code = init_payload.get('code')
        if code in (-352, -412, -403):
            raise RuntimeError(f'recording_refused: room_init code={code}')
        if code != 0:
            raise RuntimeError(f'room_init 失败: code={code} message={init_payload.get("message")}')
        init_data = init_payload.get('data') or {}
        real_room_id = int(init_data.get('room_id') or 0)
        if real_room_id <= 0:
            raise RuntimeError('room_init 未返回有效 room_id')
        short_id = int(init_data.get('short_id') or 0)
        anchor_uid = int(init_data.get('uid') or 0)
        live_status = int(init_data.get('live_status') or 0)

        info_resp = await client.get(
            'https://api.live.bilibili.com/room/v1/Room/get_info',
            params={'room_id': real_room_id},
        )
        info_data: dict[str, Any] = {}
        if info_resp.status_code in (403, 412, 429):
            raise RuntimeError(f'recording_refused: HTTP {info_resp.status_code}')
        if info_resp.status_code == 200:
            info_payload = info_resp.json()
            if info_payload.get('code') == 0 and isinstance(info_payload.get('data'), dict):
                info_data = info_payload['data']

        # 老房间短号保持与网页跳转一致的采集主键，真实长号用于拉流与弹幕鉴权。
        capture_id = str(short_id) if short_id > 0 else str(real_room_id)
        live_time = (
            _format_shanghai_live_time(info_data.get('live_time'))
            or _format_shanghai_live_time(init_data.get('live_time'))
        )
        title = str(info_data.get('title') or capture_id)
        cover_url = str(info_data.get('user_cover') or info_data.get('cover') or info_data.get('keyframe') or '')
        return {
            'requested_id': requested_id,
            'real_room_id': real_room_id,
            'short_id': short_id,
            'capture_room_id': capture_id,
            'url': f'https://live.bilibili.com/{capture_id}',
            'source_url': source,
            'anchor_uid': str(anchor_uid) if anchor_uid > 0 else '',
            'live_status': live_status,
            'live_time': live_time,
            'title': title,
            'cover_url': cover_url,
        }

    async def room_statuses(self, candidates: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        """免 CDP 批量查询候选开播状态与长短房号映射，契约对齐 room_status.room_statuses。"""
        queries = list(candidates) + [{'url': c['room_id']} for c in candidates if c.get('room_id')]
        ids = list(dict.fromkeys(room_url(str(c['url'])).rsplit('/', 1)[-1] for c in queries))
        if not ids:
            return {}
        client = self._client()
        result: dict[str, dict[str, Any]] = {}
        for offset in range(0, len(ids), 50):
            batch = ids[offset:offset + 50]
            params = [('room_ids', rid) for rid in batch] + [
                ('req_biz', 'web_room_componet'),
                ('use_cache', 'false'),
            ]
            resp = await client.get(
                'https://api.live.bilibili.com/xlive/web-room/v1/index/getRoomBaseInfo',
                params=params,
            )
            if resp.status_code != 200:
                err_cls = RoomStatusRefused if resp.status_code in (401, 403, 412, 429) else RuntimeError
                raise err_cls(f'房间状态接口HTTP {resp.status_code}；不改动现有来源')
            result.update(index_rooms(resp.json()))

        # 若个别短号未被 getRoomBaseInfo 直接返回，或在线房间缺少格式化开播时间，用 room_init 精确补齐。
        checked: set[str] = set()
        for rid in ids:
            if rid not in result:
                init_resp = await client.get(
                    'https://api.live.bilibili.com/room/v1/Room/room_init',
                    params={'id': rid},
                )
                if init_resp.status_code in (401, 403, 412, 429):
                    raise RoomStatusRefused(f'房间状态接口HTTP {init_resp.status_code}；不改动现有来源')
                if init_resp.status_code == 200:
                    payload = init_resp.json()
                    if payload.get('code') == 0 and isinstance(payload.get('data'), dict):
                        data = payload['data']
                        canonical = str(data.get('room_id') or '')
                        if canonical.isdigit() and canonical != '0':
                            row = {
                                'room_id': int(canonical),
                                'short_id': int(data.get('short_id') or 0),
                                'uid': data.get('uid'),
                                'live_status': data.get('live_status'),
                                'live_time': _format_shanghai_live_time(data.get('live_time')),
                                'title': '',
                            }
                            result[canonical] = row
                            result[rid] = row
                            if row['short_id'] > 0:
                                result[str(row['short_id'])] = row

        for row in result.values():
            rid = str(row['room_id'])
            if row.get('live_status') != 1 or rid in checked:
                continue
            formatted = _format_shanghai_live_time(row.get('live_time'))
            if formatted:
                row['live_time'] = formatted
                continue
            checked.add(rid)
            resp = await client.get(
                'https://api.live.bilibili.com/room/v1/Room/room_init',
                params={'id': rid},
            )
            if resp.status_code != 200:
                raise RuntimeError('开播时间查询失败，保留上一确认状态')
            payload = resp.json()
            if payload.get('code') != 0:
                raise RuntimeError('开播时间查询未成功，保留上一确认状态')
            data = payload.get('data') or {}
            if str(data.get('room_id')) != rid or data.get('live_status') != 1:
                raise RuntimeError('查播期间房间状态变化，等待下次确认')
            live_str = _format_shanghai_live_time(data.get('live_time'))
            if live_str:
                row['live_time'] = live_str
        return result

    async def guard_snapshot(self, room_id: str | int, anchor_uid: str | int) -> dict[str, Any]:
        """免 CDP 拉取大航海榜单快照，输出契约严格对齐 room_status.guard_snapshot。"""
        client = self._client()
        members: dict[str, dict[str, Any]] = {}
        unresolved: list[dict[str, Any]] = []
        raw_pages: list[dict[str, Any]] = []
        total: int | None = None
        pages = 1
        for page in range(1, 101):
            resp = await client.get(
                'https://api.live.bilibili.com/xlive/app-room/v2/guardTab/topList',
                params={'roomid': str(room_id), 'ruid': str(anchor_uid), 'page': page, 'page_size': 30},
            )
            if resp.status_code != 200:
                err_cls = RoomStatusRefused if resp.status_code in (401, 403, 412, 429) else RuntimeError
                raise err_cls(f'舰队快照HTTP {resp.status_code}')
            payload = resp.json()
            if payload.get('code') != 0:
                raise RoomStatusRefused(f'舰队快照拒绝：code={payload.get("code")}')
            data = payload.get('data') or {}
            info = data.get('info') or {}
            if not isinstance(data.get('list'), list) or not isinstance(data.get('top3'), list):
                raise RuntimeError('舰队快照结构异常')
            raw_pages.append(payload)
            total = int(info['num'])
            pages = max(1, int(info['page']), math.ceil(total / 30))
            for row in data['top3'] + data['list']:
                uinfo = row.get('uinfo') if isinstance(row.get('uinfo'), dict) else {}
                base = uinfo.get('base') if isinstance(uinfo.get('base'), dict) else {}
                uid = str(row.get('uid') or uinfo.get('uid') or '')
                normalized = dict(row)
                if not normalized.get('uid') and uid.isdigit():
                    normalized['uid'] = int(uid)
                if not normalized.get('username') and base.get('name'):
                    normalized['username'] = base['name']
                if not normalized.get('face') and base.get('face'):
                    normalized['face'] = base['face']
                if uid.isdigit() and int(uid) > 0:
                    members[uid] = {**normalized, 'sender_uid': uid, 'identity_status': 'uid'}
                else:
                    unresolved.append({**normalized, 'sender_uid': None, 'identity_status': 'unresolved'})
            if page >= pages:
                break
        return {
            'room_id': str(room_id),
            'observed_utc': utc_now(),
            'reported_total': total,
            'complete': page >= pages and not unresolved and len(members) == total,
            'members': list(members.values()) + unresolved,
            'unresolved_count': len(unresolved),
            'raw_pages': raw_pages,
            'source': 'guardTab/topList',
            'meaning': '当前榜单快照，不是购买流水或观看名单',
        }

    async def fetch_danmu_info(self, real_room_id: int | str) -> tuple[GuestIdentity, str, list[dict[str, Any]]]:
        """带 buvid3 Cookie 与 WBI 签名请求 getDanmuInfo，遇签名失效自动刷新一次重试。"""
        rid = int(real_room_id)
        client = self._client()
        for attempt in range(2):
            guest = await self.ensure_guest(force_refresh=attempt > 0)
            params = sign_wbi({'id': rid, 'type': 0}, guest.img_key, guest.sub_key)
            resp = await client.get(
                'https://api.live.bilibili.com/xlive/web-room/v1/index/getDanmuInfo',
                params=params,
                headers={
                    'Referer': f'https://live.bilibili.com/{rid}',
                    'Cookie': f'buvid3={guest.buvid3}; buvid4={guest.buvid4}',
                },
            )
            if resp.status_code in (403, 412, 429):
                raise RuntimeError(f'recording_refused: getDanmuInfo HTTP {resp.status_code}')
            if resp.status_code != 200:
                raise RuntimeError(f'getDanmuInfo HTTP {resp.status_code}')
            payload = resp.json()
            code = payload.get('code')
            if code == 0:
                data = payload.get('data') or {}
                token = str(data.get('token') or '')
                hosts = [h for h in (data.get('host_list') or []) if isinstance(h, dict) and h.get('host')]
                if not token or not hosts:
                    raise RuntimeError('getDanmuInfo 缺少 token 或 host_list')
                return guest, token, hosts
            if attempt == 0 and code in (-352, -403):
                continue
            if code in (-352, -412, -403):
                raise RuntimeError(f'recording_refused: getDanmuInfo code={code}')
            raise RuntimeError(f'getDanmuInfo 失败: code={code} message={payload.get("message")}')
        raise RuntimeError('getDanmuInfo 签名刷新重试后仍失败')

    async def fetch_stream_url(self, real_room_id: int | str) -> str:
        """获取直播流地址，优先 HTTP-FLV（单流低开销），回退 HLS。"""
        rid = int(real_room_id)
        guest = await self.ensure_guest()
        client = self._client()
        params = {
            'room_id': rid,
            'protocol': '0,1',
            'format': '0,1,2',
            'codec': '0,1',
            'qn': 10000,
            'platform': 'web',
            'ptype': 8,
            'dolby': 5,
            'panorama': 1,
        }
        resp = await client.get(
            'https://api.live.bilibili.com/xlive/web-room/v2/index/getRoomPlayInfo',
            params=params,
            headers={
                'Referer': f'https://live.bilibili.com/{rid}',
                'Cookie': f'buvid3={guest.buvid3}; buvid4={guest.buvid4}',
            },
        )
        if resp.status_code in (403, 412, 429):
            raise RuntimeError(f'recording_refused: getRoomPlayInfo HTTP {resp.status_code}')
        if resp.status_code != 200:
            raise RuntimeError(f'getRoomPlayInfo HTTP {resp.status_code}')
        payload = resp.json()
        code = payload.get('code')
        if code in (-352, -412, -403):
            raise RuntimeError(f'recording_refused: getRoomPlayInfo code={code}')
        if code != 0:
            raise RuntimeError(f'getRoomPlayInfo 失败: code={code} message={payload.get("message")}')
        data = payload.get('data') or {}
        if data.get('live_status') not in (1, None) and not data.get('playurl_info'):
            raise RuntimeError(f'房间当前未在播: live_status={data.get("live_status")}')
        streams = (((data.get('playurl_info') or {}).get('playurl') or {}).get('stream')) or []
        flv_avc: list[str] = []
        flv_any: list[str] = []
        hls_any: list[str] = []
        fallback: list[str] = []
        for st in streams:
            proto_name = st.get('protocol_name')
            for fmt in st.get('format') or []:
                fmt_name = fmt.get('format_name')
                for cd in fmt.get('codec') or []:
                    codec_name = cd.get('codec_name')
                    base_url = cd.get('base_url') or ''
                    for u in cd.get('url_info') or []:
                        host = u.get('host') or ''
                        if not host or not base_url:
                            continue
                        full = f'{host}{base_url}{u.get("extra") or ""}'
                        if proto_name == 'http_stream' and fmt_name == 'flv':
                            if codec_name == 'avc':
                                flv_avc.append(full)
                            else:
                                flv_any.append(full)
                        elif proto_name == 'http_hls':
                            hls_any.append(full)
                        else:
                            fallback.append(full)
        chosen = (flv_avc or flv_any or hls_any or fallback)
        if not chosen:
            raise RuntimeError(f'recording_url_missing：未找到可用直播流地址 room={rid}')
        return chosen[0]

    async def stream_danmaku(
        self,
        real_room_id: int | str,
        on_danmaku: Callable[[dict[str, Any]], None],
        *,
        danmu_info: tuple[GuestIdentity, str, list[dict[str, Any]]] | None = None,
        heartbeat_seconds: float = 30.0,
        idle_timeout: float = 120.0,
    ) -> None:
        """建立 WSS 弹幕连接，完成 Op7/Op8 认证、Op2 心跳及 Op5 原始广播流式解包。"""
        rid = int(real_room_id)
        guest, token, hosts = danmu_info if danmu_info is not None else await self.fetch_danmu_info(rid)
        ws: MinimalAsyncWebSocket | None = None
        last_conn_error: Exception | None = None
        for entry in hosts:
            host = str(entry['host'])
            port = int(entry.get('wss_port') or 443)
            use_tls = bool(entry.get('use_tls', True))
            await self.acquire_ws_connect_slot()
            try:
                ws = await MinimalAsyncWebSocket.connect(
                    host,
                    port,
                    '/sub',
                    timeout=15.0,
                    use_tls=use_tls,
                    open_connection=self._ws_connector,
                )
                break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_conn_error = exc
                if 'recording_refused' in str(exc):
                    raise
                continue
        if ws is None:
            raise RuntimeError(f'弹幕服务器连接失败: {last_conn_error}')

        auth_body = json.dumps(
            {
                'uid': 0,
                'roomid': rid,
                'protover': 3,
                'platform': 'web',
                'type': 2,
                'buvid': guest.buvid3,
                'key': token,
            },
            separators=(',', ':'),
        ).encode('utf-8')

        heartbeat_task: asyncio.Task[None] | None = None
        connected_reported = False
        try:
            await ws.send_bytes(pack_bili_packet(7, auth_body))
            # 等待 Op8 认证回执；若同一二进制消息合包携带后续 Op5，认证后一并解包不丢消息
            auth_deadline = time.monotonic() + 10.0
            while True:
                remaining = auth_deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError('等待弹幕服务器 Op8 认证回执超时')
                ws_op, raw = await asyncio.wait_for(ws.recv_message(), timeout=remaining)
                if ws_op != 0x2 or len(raw) < 16:
                    continue
                auth_ok = False
                for bop, body in iter_bili_envelopes(raw):
                    if bop == 8:
                        reply_raw = body.decode('utf-8', errors='replace')
                        reply = json.loads(reply_raw) if reply_raw.strip().startswith('{') else {'code': -1}
                        if reply.get('code') != 0:
                            raise RuntimeError(f'recording_refused: WebSocket Op8 认证拒绝 code={reply.get("code")}')
                        auth_ok = True
                if auth_ok:
                    on_danmaku({'kind': 'transport', 'state': 'connected', 'frame_received': True})
                    connected_reported = True
                    dispatch_messages(raw, on_danmaku, include_raw=True)
                    break

            last_report = time.monotonic()
            await ws.send_bytes(pack_bili_packet(2, b'[object Object]'))

            async def _heartbeat_loop() -> None:
                while True:
                    await asyncio.sleep(heartbeat_seconds)
                    await ws.send_bytes(pack_bili_packet(2, b'[object Object]'))

            heartbeat_task = asyncio.create_task(_heartbeat_loop())

            while True:
                if heartbeat_task.done() and not heartbeat_task.cancelled():
                    exc = heartbeat_task.exception()
                    if exc is not None:
                        raise RuntimeError(f'弹幕心跳发送失败: {exc}') from exc
                try:
                    ws_op, frame = await asyncio.wait_for(ws.recv_message(), timeout=idle_timeout)
                except asyncio.TimeoutError as exc:
                    raise RuntimeError(f'chat_stalled：{int(idle_timeout)}秒无弹幕连接帧或心跳') from exc
                if ws_op != 0x2 or len(frame) < 16:
                    continue
                now_mono = time.monotonic()
                if now_mono - last_report >= 15.0:
                    on_danmaku({'kind': 'transport', 'state': 'connected', 'frame_received': True})
                    last_report = now_mono
                for bop, body in iter_bili_envelopes(frame):
                    if bop == 8:
                        reply_raw = body.decode('utf-8', errors='replace')
                        reply = json.loads(reply_raw) if reply_raw.strip().startswith('{') else {'code': -1}
                        if reply.get('code') not in (None, 0):
                            raise RuntimeError(f'recording_refused: WebSocket Op8 异常状态 code={reply.get("code")}')
                dispatch_messages(frame, on_danmaku, include_raw=True)
        except BaseException as exc:
            if connected_reported and not isinstance(exc, asyncio.CancelledError):
                on_danmaku({'kind': 'transport', 'state': 'error'})
            raise
        finally:
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)
            if connected_reported:
                on_danmaku({'kind': 'transport', 'state': 'disconnected'})
            await ws.close()

    async def discover(self, limit: int = 20) -> list[dict[str, str]]:
        """免浏览器发现当前在播热门候选房间。"""
        guest = await self.ensure_guest()
        client = self._client()
        result: list[dict[str, str]] = []
        seen: set[str] = set()
        page = 1
        while len(result) < limit and page <= 10:
            params = sign_wbi(
                {'sort': 'online', 'page': page, 'page_size': 30, 'platform': 'web'},
                guest.img_key,
                guest.sub_key,
            )
            resp = await client.get(
                'https://api.live.bilibili.com/xlive/web-interface/v1/second/getListByArea',
                params=params,
                headers={'Cookie': f'buvid3={guest.buvid3}; buvid4={guest.buvid4}'},
            )
            if resp.status_code in (403, 412, 429):
                raise RuntimeError('discover_refused：来源发现接口拒绝访问，停止请求')
            resp.raise_for_status()
            payload = resp.json()
            if payload.get('code') in (-352, -412, -403):
                raise RuntimeError('discover_refused：来源发现接口拒绝访问，停止请求')
            rows = ((payload.get('data') or {}).get('list')) or []
            if not rows:
                break
            for item in rows:
                rid = item.get('roomid')
                if not rid:
                    continue
                try:
                    canonical = room_url(str(rid))
                except ValueError:
                    continue
                if canonical not in seen:
                    seen.add(canonical)
                    result.append({'url': canonical, 'title': str(item.get('title') or '')[:120]})
                    if len(result) >= limit:
                        break
            page += 1
        return result[:limit]

    async def close(self) -> None:
        if self._http is not None and not self._external_http:
            await self._http.aclose()
            self._http = None
