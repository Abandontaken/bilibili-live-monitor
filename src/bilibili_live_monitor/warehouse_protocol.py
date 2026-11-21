"""全局身份消解数仓的只读协议提取层：提取明文字典观测、原始哈希线索与标准化事件。"""
from __future__ import annotations

import base64
import hashlib
import json
import re
import zlib
from bisect import bisect_left, bisect_right
from collections import defaultdict
from typing import Any

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.message import DecodeError

from .gift_value import gift_value
from .time_utils import epoch

MAX_PROTO_BYTES = 4 * 1024 * 1024
_FACE_SHA1_RE = re.compile(r'/bfs/face/([0-9a-fA-F]{40})\.')
_HEX32_RE = re.compile(r'^[0-9a-fA-F]{32}$')
_HEX8_RE = re.compile(r'^[0-9a-fA-F]{8}$')
_COPY_NAME_RE = re.compile(r'<%([^%]+)%>')


def _build_proto_classes():
    pool = descriptor_pool.DescriptorPool()

    def add_file(file_name: str, package: str, messages: list[tuple[str, list[tuple[int, str, Any]]]]):
        schema = descriptor_pb2.FileDescriptorProto(name=file_name, package=package, syntax='proto3')
        for msg_name, fields in messages:
            msg = schema.message_type.add(name=msg_name)
            for number, field_name, kind in fields:
                field = msg.field.add(name=field_name, number=number, label=1)
                if isinstance(kind, str) and kind.endswith('?'):
                    kind = kind[:-1]
                    msg.oneof_decl.add(name='_' + field_name)
                    field.proto3_optional = True
                    field.oneof_index = len(msg.oneof_decl) - 1
                if kind == 'uint':
                    field.type = 4  # uint64
                elif kind == 'uint32':
                    field.type = 13
                elif kind == 'bool':
                    field.type = 8
                elif kind == 'str':
                    field.type = 9
                else:
                    field.type = 11
                    field.type_name = f'.{package}.' + str(kind).rstrip('[]')
                    if str(kind).endswith('[]'):
                        field.label = 3
        pool.Add(schema)

    add_file('warehouse_gift.proto', 'waregift', [
        ('Origin', [(1, 'name', 'str'), (2, 'face', 'str')]),
        ('Base', [(1, 'name', 'str'), (2, 'face', 'str'), (4, 'is_mystery', 'bool'), (6, 'origin', 'Origin')]),
        ('Medal', [(1, 'name', 'str'), (2, 'level', 'uint'), (10, 'ruid', 'uint'), (11, 'guard_level', 'uint')]),
        ('Wealth', [(1, 'level', 'uint')]),
        ('Guard', [(1, 'level', 'uint')]),
        ('User', [(1, 'uid', 'uint'), (2, 'base', 'Base'), (3, 'medal', 'Medal'),
                  (4, 'wealth', 'Wealth'), (6, 'guard', 'Guard')]),
        ('OuterMedal', [(1, 'target_id', 'uint'), (5, 'medal_level', 'uint'), (6, 'medal_name', 'str'),
                        (11, 'guard_level', 'uint'), (12, 'anchor_roomid', 'uint')]),
        ('BlindGift', [(1, 'blind_gift_config_id', 'uint'), (2, 'original_gift_id', 'uint'),
                       (3, 'original_gift_name', 'str'), (6, 'original_gift_price', 'uint?')]),
        ('Item', [(1, 'gift_id', 'uint'), (2, 'gift_name', 'str'), (3, 'num', 'uint'),
                  (5, 'price', 'uint?'), (7, 'total_coin', 'uint?'), (8, 'coin_type', 'str'),
                  (9, 'tid', 'str'), (10, 'timestamp', 'uint'), (12, 'batch_combo_id', 'str')]),
        ('Gift', [(1, 'uid', 'uint'), (2, 'uname', 'str'), (3, 'face', 'str'),
                  (4, 'batch_combo_id', 'str'), (8, 'medal_info', 'OuterMedal'),
                  (9, 'blind_gift', 'BlindGift'), (10, 'items', 'Item[]'),
                  (15, 'sender', 'User'), (19, 'is_mystery', 'bool')]),
    ])

    add_file('warehouse_rank.proto', 'warerank', [
        ('Base', [(1, 'name', 'str'), (2, 'face', 'str'), (4, 'is_mystery', 'bool')]),
        ('Wealth', [(1, 'level', 'uint')]),
        ('User', [(1, 'uid', 'uint'), (2, 'base', 'Base'), (4, 'wealth', 'Wealth')]),
        ('RankUser', [(1, 'uid', 'uint'), (2, 'face', 'str'), (3, 'score', 'str'),
                      (4, 'uname', 'str'), (5, 'rank', 'uint'), (6, 'guard_level', 'uint'),
                      (8, 'uinfo', 'User')]),
        ('OnlineRankV3', [(1, 'rank_type', 'str'), (2, 'list', 'RankUser[]'),
                          (3, 'online_list', 'RankUser[]')]),
    ])

    add_file('warehouse_interact.proto', 'wareinteract', [
        ('Base', [(1, 'name', 'str'), (2, 'face', 'str'), (4, 'is_mystery', 'bool')]),
        ('Medal', [(1, 'name', 'str'), (2, 'level', 'uint'), (10, 'ruid', 'uint')]),
        ('Wealth', [(1, 'level', 'uint')]),
        ('Guard', [(1, 'level', 'uint')]),
        ('User', [(1, 'uid', 'uint'), (2, 'base', 'Base'), (3, 'medal', 'Medal'),
                  (4, 'wealth', 'Wealth'), (6, 'guard', 'Guard')]),
        ('FansMedal', [(1, 'target_id', 'uint'), (2, 'medal_level', 'uint'), (3, 'medal_name', 'str'),
                       (11, 'guard_level', 'uint'), (12, 'anchor_roomid', 'uint')]),
        ('Interaction', [(1, 'uid', 'uint'), (2, 'username', 'str'), (5, 'msg_type', 'uint32'),
                         (7, 'timestamp', 'uint32'), (8, 'timestamp_ms', 'uint'),
                         (9, 'fans_medal', 'FansMedal'), (15, 'trigger_time', 'uint'),
                         (16, 'guard_type', 'uint32'), (22, 'uinfo', 'User')]),
    ])

    return (
        message_factory.GetMessageClass(pool.FindMessageTypeByName('waregift.Gift')),
        message_factory.GetMessageClass(pool.FindMessageTypeByName('warerank.OnlineRankV3')),
        message_factory.GetMessageClass(pool.FindMessageTypeByName('wareinteract.Interaction')),
    )


WareGift, WareRank, WareInteract = _build_proto_classes()


def uid_md5(uid: int) -> str:
    return hashlib.md5(str(int(uid)).encode()).hexdigest()


def uid_crc32(uid: int) -> str:
    return f'{zlib.crc32(str(int(uid)).encode()):08x}'


def pos_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return None
    text = str(value).strip()
    if not text.isdigit():
        return None
    num = int(text)
    return num if num > 0 else None


def nonneg_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return None
    text = str(value).strip()
    if not text.isdigit():
        return None
    return int(text)


def valid_guard_level(value: Any) -> int | None:
    num = nonneg_int(value)
    return num if num in (1, 2, 3) else None


def extract_face_sha1(url: Any) -> str | None:
    """仅提取用户自定义头像 /bfs/face/<40位hex>.，严格排除默认灰头 noface 与官方装扮 /bfs/garb/。"""
    if not isinstance(url, str) or not url:
        return None
    lower = url.lower()
    if 'noface' in lower or '/bfs/garb/' in lower:
        return None
    match = _FACE_SHA1_RE.search(lower)
    return match.group(1) if match else None


def is_masked_name(name: Any) -> bool:
    if not isinstance(name, str):
        return True
    cleaned = name.strip()
    if not cleaned or cleaned in ('神秘人', '未命名用户', '未提供昵称的送礼观众'):
        return True
    return '***' in cleaned or cleaned.startswith('*')


def clean_uname(name: Any) -> str | None:
    if not isinstance(name, str):
        return None
    cleaned = ''.join(ch for ch in name.strip() if ch.isprintable())[:200]
    return cleaned or None


def first_char(name: Any) -> str | None:
    cleaned = clean_uname(name)
    if not cleaned or cleaned in ('神秘人', '未命名用户', '未提供昵称的送礼观众') or cleaned[0] == '*':
        return None
    return cleaned[0]


def parse_batch_combo_clue(combo_id: Any) -> tuple[int | None, str | None]:
    """解析 batch:gift:combo_id:<32hex_or_uid>:...，实名数字UID段同步返回 raw_uid 与 clue_md5。"""
    if not isinstance(combo_id, str) or not combo_id.startswith('batch:gift:combo_id:'):
        return None, None
    parts = combo_id.split(':')
    if len(parts) < 4:
        return None, None
    seg = parts[3].strip()
    if _HEX32_RE.fullmatch(seg):
        return None, seg.lower()
    uid = pos_int(seg)
    if uid is not None:
        return uid, uid_md5(uid)
    return None, None


def _decode_pb(cls, data: dict[str, Any]):
    encoded = data.get('pb')
    if not isinstance(encoded, str) or not encoded or len(encoded) > MAX_PROTO_BYTES:
        return None
    return cls.FromString(base64.b64decode(encoded, validate=True))


def _ts_from_proto_or_json(raw_ts: Any, received_ts: float) -> tuple[float, str]:
    num = pos_int(raw_ts)
    if num is None and isinstance(raw_ts, float) and raw_ts > 0:
        num = raw_ts
    if num is None:
        return received_ts, 'received'
    ts = float(num)
    if ts > 10**11:
        ts /= 1000.0
    return ts, 'event'


def _extract_uinfo_dict(uinfo: Any) -> tuple[int | None, str | None, str | None, int, bool]:
    if not isinstance(uinfo, dict):
        return None, None, None, 0, False
    base = uinfo.get('base') if isinstance(uinfo.get('base'), dict) else {}
    origin = base.get('origin_info') if isinstance(base.get('origin_info'), dict) else {}
    mystery = bool(base.get('is_mystery') or uinfo.get('is_mystery'))
    uid = None if mystery else pos_int(uinfo.get('uid'))
    uname = None if mystery else (clean_uname(base.get('name')) or clean_uname(origin.get('name')))
    face = None if mystery else (extract_face_sha1(base.get('face')) or extract_face_sha1(origin.get('face')))
    wealth_obj = uinfo.get('wealth') if isinstance(uinfo.get('wealth'), dict) else {}
    wealth = nonneg_int(wealth_obj.get('level')) or 0
    return uid, uname, face, wealth, mystery


def _extract_danmaku_u15_user(info: Any) -> dict[str, Any] | None:
    """从 DANMU_MSG 的 info[0][15]（dict 或 JSON 字符串，含 extra 同层或嵌套 user）提取 user 对象。"""
    if not isinstance(info, list) or not info or not isinstance(info[0], list) or len(info[0]) <= 15:
        return None
    raw_meta = info[0][15]
    meta: dict[str, Any] | None = None
    if isinstance(raw_meta, dict):
        meta = raw_meta
    elif isinstance(raw_meta, str) and raw_meta.strip().startswith('{'):
        try:
            parsed = json.loads(raw_meta)
            if isinstance(parsed, dict):
                meta = parsed
        except Exception:
            meta = None
    if meta is None:
        return None
    user = meta.get('user')
    if isinstance(user, dict):
        return user
    extra = meta.get('extra')
    if isinstance(extra, str) and extra.strip().startswith('{'):
        try:
            extra_dict = json.loads(extra)
            if isinstance(extra_dict, dict) and isinstance(extra_dict.get('user'), dict):
                return extra_dict['user']
        except Exception:
            pass
    elif isinstance(extra, dict) and isinstance(extra.get('user'), dict):
        return extra['user']
    return None


def _extract_medal_from_uinfo_and_fans(uinfo: Any, fans_medal: Any = None) -> dict[str, Any]:
    u_medal = (uinfo.get('medal') if isinstance(uinfo, dict) and isinstance(uinfo.get('medal'), dict) else {})
    f_medal = fans_medal if isinstance(fans_medal, dict) else {}
    level = pos_int(u_medal.get('level')) or pos_int(f_medal.get('medal_level'))
    name = clean_uname(u_medal.get('name')) or clean_uname(f_medal.get('medal_name'))
    ruid = pos_int(u_medal.get('ruid')) or pos_int(f_medal.get('target_id'))
    room_val = pos_int(f_medal.get('anchor_roomid'))
    guard = valid_guard_level(u_medal.get('guard_level')) or valid_guard_level(f_medal.get('guard_level'))
    if not level and not name and not ruid:
        return {'medal_ruid': None, 'medal_room_id': None, 'medal_name': None, 'medal_level': None, 'medal_guard': guard}
    return {
        'medal_ruid': ruid,
        'medal_room_id': str(room_val) if room_val else None,
        'medal_name': name,
        'medal_level': level,
        'medal_guard': guard,
    }


def parse_online_rank_v3(data: Any) -> tuple[list[tuple[int, str | None, str | None, int]], dict[int, int]]:
    """解析 ONLINE_RANK_V3（Protobuf 或 JSON），返回 (明文身份观测列表, {uid: score})。"""
    if not isinstance(data, dict):
        return [], {}
    identities: list[tuple[int, str | None, str | None, int]] = []
    scores: dict[int, int] = {}
    if isinstance(data.get('pb'), str):
        msg = _decode_pb(WareRank, data)
        if msg is None:
            return [], {}
        seen_entries = list(msg.list) + list(msg.online_list)
        for entry in seen_entries:
            mystery = bool(entry.uinfo.base.is_mystery)
            if mystery:
                continue
            uid = pos_int(entry.uinfo.uid or entry.uid)
            if uid is None:
                continue
            uname = clean_uname(entry.uinfo.base.name or entry.uname)
            face = extract_face_sha1(entry.uinfo.base.face) or extract_face_sha1(entry.face)
            wealth = int(entry.uinfo.wealth.level or 0)
            identities.append((uid, uname, face, wealth))
            score_val = nonneg_int(entry.score)
            if score_val is not None:
                scores[uid] = max(scores.get(uid, 0), score_val)
        return identities, scores

    for key in ('list', 'online_list'):
        entries = data.get(key)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict) or entry.get('is_mystery'):
                continue
            u_uid, u_name, u_face, u_wealth, mystery = _extract_uinfo_dict(entry.get('uinfo'))
            if mystery:
                continue
            uid = u_uid or pos_int(entry.get('uid'))
            if uid is None:
                continue
            uname = u_name or clean_uname(entry.get('uname'))
            face = u_face or extract_face_sha1(entry.get('face'))
            wealth = u_wealth or (nonneg_int(entry.get('wealth_level')) or 0)
            identities.append((uid, uname, face, wealth))
            score_val = nonneg_int(entry.get('score'))
            if score_val is not None:
                scores[uid] = max(scores.get(uid, 0), score_val)
    return identities, scores


def extract_raw_identities(command: str, packet: dict[str, Any]) -> tuple[list[tuple[int, str | None, str | None, int]], int | None]:
    """从单条原始 WebSocket 报文提取 ([(uid, uname, face_sha1, wealth_level), ...], anchor_uid)。"""
    cmd = str(command or packet.get('cmd') or '')
    data = packet.get('data')
    out: list[tuple[int, str | None, str | None, int]] = []
    anchor_uid: int | None = None

    try:
        if cmd == 'ONLINE_RANK_V3':
            identities, _ = parse_online_rank_v3(data)
            return identities, None

        if cmd in ('ENTRY_EFFECT', 'ENTRY_EFFECT_MUST_RECEIVE') and isinstance(data, dict):
            u_uid, u_name, u_face, u_wealth, mystery = _extract_uinfo_dict(data.get('uinfo'))
            if mystery or data.get('is_mystery'):
                return [], None
            uid = u_uid or pos_int(data.get('uid'))
            if uid is not None:
                if not u_name:
                    cw = str(data.get('copy_writing') or data.get('copy_writing_v2') or '')
                    m = _COPY_NAME_RE.search(cw)
                    if m:
                        u_name = clean_uname(m.group(1))
                face = u_face or extract_face_sha1(data.get('face'))
                out.append((uid, u_name, face, u_wealth))
            anchor_uid = pos_int(data.get('target_id'))
            return out, anchor_uid

        if cmd == 'LIKE_INFO_V3_CLICK' and isinstance(data, dict):
            u_uid, u_name, u_face, u_wealth, mystery = _extract_uinfo_dict(data.get('uinfo'))
            if not mystery and not data.get('is_mystery'):
                uid = u_uid or pos_int(data.get('uid'))
                if uid is not None:
                    out.append((uid, u_name or clean_uname(data.get('uname')), u_face, u_wealth))
            return out, None

        if cmd == 'USER_TOAST_MSG' and isinstance(data, dict):
            uid = pos_int(data.get('uid'))
            if uid is not None:
                out.append((uid, clean_uname(data.get('username')), None, 0))
            return out, None

        if cmd == 'USER_TOAST_MSG_V2' and isinstance(data, dict):
            u_uid, u_name, u_face, u_wealth, mystery = _extract_uinfo_dict(data.get('sender_uinfo'))
            if not mystery and u_uid is not None:
                out.append((u_uid, u_name, u_face, u_wealth))
            r_uid, _, _, _, _ = _extract_uinfo_dict(data.get('receiver_uinfo'))
            return out, r_uid

        if cmd == 'GUARD_BUY' and isinstance(data, dict):
            uid = pos_int(data.get('uid'))
            if uid is not None:
                out.append((uid, clean_uname(data.get('username')), None, 0))
            return out, None

        if cmd == 'SUPER_CHAT_MESSAGE' and isinstance(data, dict):
            u_uid, u_name, u_face, u_wealth, mystery = _extract_uinfo_dict(data.get('uinfo'))
            if not mystery and not data.get('is_mystery'):
                user_info = data.get('user_info') if isinstance(data.get('user_info'), dict) else {}
                uid = u_uid or pos_int(data.get('uid'))
                if uid is not None:
                    uname = u_name or clean_uname(user_info.get('uname'))
                    face = u_face or extract_face_sha1(user_info.get('face'))
                    out.append((uid, uname, face, u_wealth))
            return out, None

        if cmd == 'PK_BATTLE_SETTLE_NEW' and isinstance(data, dict):
            for side_key in ('init_info', 'match_info'):
                side = data.get(side_key)
                if not isinstance(side, dict):
                    continue
                assists = side.get('assist_info')
                if not isinstance(assists, list):
                    continue
                for entry in assists:
                    if not isinstance(entry, dict) or entry.get('is_mystery'):
                        continue
                    u_uid, u_name, u_face, u_wealth, mystery = _extract_uinfo_dict(entry.get('uinfo'))
                    if mystery:
                        continue
                    uid = u_uid or pos_int(entry.get('uid'))
                    if uid is not None:
                        uname = u_name or clean_uname(entry.get('uname'))
                        face = u_face or extract_face_sha1(entry.get('face'))
                        out.append((uid, uname, face, u_wealth))
            return out, None

        if cmd == 'SEND_GIFT_V2' and isinstance(data, dict):
            msg = _decode_pb(WareGift, data)
            if msg is not None and not msg.is_mystery and not msg.sender.base.is_mystery:
                uid = pos_int(msg.sender.uid or msg.uid)
                if uid is None:
                    for item in msg.items:
                        c_uid, _ = parse_batch_combo_clue(item.batch_combo_id or msg.batch_combo_id)
                        if c_uid is not None:
                            uid = c_uid
                            break
                if uid is not None:
                    uname = clean_uname(msg.sender.base.name or msg.sender.base.origin.name or msg.uname)
                    face = (extract_face_sha1(msg.sender.base.face)
                            or extract_face_sha1(msg.sender.base.origin.face)
                            or extract_face_sha1(msg.face))
                    wealth = int(msg.sender.wealth.level or 0)
                    out.append((uid, uname, face, wealth))
            return out, None

        if cmd in ('SEND_GIFT', 'COMBO_SEND') and isinstance(data, dict):
            u_uid, u_name, u_face, u_wealth, mystery = _extract_uinfo_dict(data.get('sender_uinfo'))
            if not mystery and not data.get('is_mystery'):
                c_uid, _ = parse_batch_combo_clue(data.get('batch_combo_id'))
                uid = u_uid or pos_int(data.get('uid')) or c_uid
                if uid is not None:
                    uname = u_name or clean_uname(data.get('uname') or data.get('username'))
                    face = u_face or extract_face_sha1(data.get('face'))
                    out.append((uid, uname, face, u_wealth))
            ruid = pos_int(data.get('ruid'))
            return out, ruid

        if cmd in ('INTERACT_WORD', 'INTERACT_WORD_V2') and isinstance(data, dict):
            if cmd == 'INTERACT_WORD_V2' and isinstance(data.get('pb'), str):
                msg = _decode_pb(WareInteract, data)
                if msg is not None and not msg.uinfo.base.is_mystery:
                    uid = pos_int(msg.uinfo.uid or msg.uid)
                    if uid is not None:
                        uname = clean_uname(msg.uinfo.base.name or msg.username)
                        face = extract_face_sha1(msg.uinfo.base.face)
                        wealth = int(msg.uinfo.wealth.level or 0)
                        out.append((uid, uname, face, wealth))
            else:
                u_uid, u_name, u_face, u_wealth, mystery = _extract_uinfo_dict(data.get('uinfo'))
                if not mystery and not data.get('is_mystery'):
                    uid = u_uid or pos_int(data.get('uid'))
                    if uid is not None:
                        uname = u_name or clean_uname(data.get('uname') or data.get('username'))
                        out.append((uid, uname, u_face, u_wealth))
            return out, None

        if cmd.startswith('DANMU_MSG'):
            info = packet.get('info')
            if isinstance(info, list) and len(info) > 2 and isinstance(info[2], list) and len(info[2]) >= 2:
                u15_user = _extract_danmaku_u15_user(info)
                u_uid, u_name, u_face, u_wealth, mystery = _extract_uinfo_dict(u15_user)
                if not mystery:
                    uid = pos_int(info[2][0]) or u_uid
                    if uid is not None:
                        uname = clean_uname(info[2][1]) or u_name
                        w16 = (nonneg_int(info[16][0])
                               if len(info) > 16 and isinstance(info[16], list) and info[16] else None)
                        wealth = w16 if w16 is not None else u_wealth
                        out.append((uid, uname, u_face, wealth))
            return out, None
    except (ValueError, TypeError, KeyError, IndexError, DecodeError):
        return [], None

    return [], None


def extract_guard_snapshot_identities(snapshot: Any) -> tuple[list[tuple[int, str | None, str | None, int]], int | None]:
    """从 guard_snapshots.data 提取明文舰队成员身份及房间主播 ruid。"""
    if not isinstance(snapshot, dict):
        return [], None
    out: list[tuple[int, str | None, str | None, int]] = []
    anchor_uid: int | None = None
    candidates: list[dict[str, Any]] = []
    if isinstance(snapshot.get('members'), list):
        candidates.extend(m for m in snapshot['members'] if isinstance(m, dict))
    if isinstance(snapshot.get('raw_pages'), list):
        for page in snapshot['raw_pages']:
            pdata = page.get('data') if isinstance(page, dict) else None
            if isinstance(pdata, dict):
                for list_key in ('list', 'top3'):
                    if isinstance(pdata.get(list_key), list):
                        candidates.extend(m for m in pdata[list_key] if isinstance(m, dict))
    for member in candidates:
        uid = pos_int(member.get('uid'))
        if uid is not None:
            out.append((uid, clean_uname(member.get('username')), extract_face_sha1(member.get('face')), 0))
        if anchor_uid is None:
            anchor_uid = pos_int(member.get('ruid'))
    return out, anchor_uid


def extract_timeline_identity(data: Any) -> tuple[int, str | None] | None:
    """无 raw_events 的早期实名库回退：从 timeline.data 提取明文 sender_uid 与 author。"""
    if not isinstance(data, dict) or data.get('sender_hidden') or data.get('identity_status') == 'unresolved':
        return None
    gift = data.get('gift') if isinstance(data.get('gift'), dict) else {}
    uid = pos_int(data.get('sender_uid')) or pos_int(gift.get('sender_uid'))
    if uid is None:
        return None
    return uid, clean_uname(data.get('author'))


def _paid_milli_for_gift(coin_type: str | None, num: int, price: int | None, total_coin: int | None,
                         blind_gift: dict[str, Any] | None) -> int:
    if coin_type == 'silver':
        return 0
    if coin_type != 'gold':
        return 0
    if isinstance(blind_gift, dict):
        orig_price = nonneg_int(blind_gift.get('original_gift_price'))
        if orig_price is not None:
            return orig_price * num
        return total_coin if total_coin is not None else 0
    if total_coin is not None:
        return total_coin
    if price is not None:
        return price * num
    return 0


def _parse_entry_time(ts_sec: int | None, ts_ms: int | None, received_ts: float) -> tuple[float, str]:
    seconds = ts_sec or 0
    millis = ts_ms or 0
    matching_ms = bool(millis and (millis // 1000 == seconds if seconds else abs(millis / 1000.0 - received_ts) <= 60))
    chosen = millis if matching_ms else seconds
    if chosen:
        val = float(chosen)
        return (val / 1000.0 if val > 10**11 else val), 'event'
    return received_ts, 'received'


def extract_raw_gift_clues(
    raw_rows: list[tuple[Any, ...]], *, session_bounds: tuple[float, float] | None = None,
) -> dict[str, Any]:
    """从单场 raw_events 构建礼物线索索引，供数仓事件入库与本地网页回填共用。"""
    combo_map: dict[str, dict[str, Any]] = {}
    by_tid_batch: dict[tuple[str, int], dict[str, Any]] = {}
    by_fallback: list[dict[str, Any]] = []
    rank_frames: list[tuple[float, dict[int, int]]] = []
    named_ranks: list[tuple[float, dict[int, int], dict[int, str]]] = []
    all_gold_gift_times: list[tuple[int, float, int]] = []
    receipts: dict[int, float] = {}
    other_paid_receipts: list[float] = []

    for row in raw_rows:
        if len(row) == 4:
            _, received_utc, command, packet = row
        else:
            received_utc, command, packet = row[0], row[1], row[2]
        if not isinstance(packet, dict):
            continue
        cmd = str(command or packet.get('cmd') or '')
        data = packet.get('data')
        if cmd == 'COMBO_SEND' and isinstance(data, dict):
            combo_id = data.get('batch_combo_id')
            if isinstance(combo_id, str) and combo_id:
                u_uid, u_name, u_face, u_wealth, mystery = _extract_uinfo_dict(data.get('sender_uinfo'))
                c_uid, c_md5 = parse_batch_combo_clue(combo_id)
                uid = None if mystery or data.get('is_mystery') else (u_uid or pos_int(data.get('uid')) or c_uid)
                if uid is not None and c_md5 is None:
                    c_md5 = uid_md5(uid)
                medal = _extract_medal_from_uinfo_and_fans(data.get('sender_uinfo'), data.get('medal_info'))
                prev = combo_map.setdefault(combo_id, {})
                if uid is not None:
                    prev['raw_uid'] = uid
                if c_md5:
                    prev['clue_md5'] = c_md5
                face = None if mystery else (u_face or extract_face_sha1(data.get('face')))
                if face:
                    prev['clue_face_sha1'] = face
                if u_name and not is_masked_name(u_name):
                    prev['uname'] = u_name
                if u_wealth:
                    prev['wealth_level'] = max(prev.get('wealth_level', 0), u_wealth)
                for mk, mv in medal.items():
                    if mv is not None and prev.get(mk) is None:
                        prev[mk] = mv

    for row in raw_rows:
        if len(row) == 4:
            raw_id, received_utc, command, packet = row
        else:
            raw_id, received_utc, command, packet = None, row[0], row[1], row[2]
        if not isinstance(packet, dict):
            continue
        cmd = str(command or packet.get('cmd') or '')
        data = packet.get('data')
        rec_ts = epoch(received_utc)
        if raw_id is not None and cmd in ('SEND_GIFT_V2', 'SEND_GIFT'):
            receipts[raw_id] = rec_ts

        if cmd == 'ONLINE_RANK_V3' and isinstance(data, dict):
            try:
                identities, scores = parse_online_rank_v3(data)
                if scores:
                    rank_frames.append((rec_ts, scores))
                    if session_bounds and session_bounds[0] <= rec_ts <= session_bounds[1]:
                        names = {uid: name for uid, name, _, _ in identities if name and not is_masked_name(name)}
                        named_ranks.append((rec_ts, scores, names))
            except Exception:
                continue

        elif cmd == 'SEND_GIFT_V2' and isinstance(data, dict):
            try:
                msg = _decode_pb(WareGift, data)
            except Exception:
                continue
            if msg is None or not msg.items:
                continue
            mystery = bool(msg.is_mystery or msg.sender.base.is_mystery)
            base_uid = None if mystery else pos_int(msg.sender.uid or msg.uid)
            base_uname = '神秘人' if mystery else clean_uname(
                msg.sender.base.name or msg.sender.base.origin.name or msg.uname
            )
            base_face = None if mystery else (
                extract_face_sha1(msg.sender.base.face)
                or extract_face_sha1(msg.sender.base.origin.face)
                or extract_face_sha1(msg.face)
            )
            wealth = int(msg.sender.wealth.level or 0) if not mystery else None
            guard = (valid_guard_level(msg.sender.guard.level)
                     or valid_guard_level(msg.sender.medal.guard_level)
                     or valid_guard_level(msg.medal_info.guard_level))
            medal_ruid = pos_int(msg.sender.medal.ruid or msg.medal_info.target_id)
            medal_room_val = pos_int(msg.medal_info.anchor_roomid)
            medal_room_id = str(medal_room_val) if medal_room_val else None
            medal_name = clean_uname(msg.sender.medal.name or msg.medal_info.medal_name)
            medal_level = pos_int(msg.sender.medal.level or msg.medal_info.medal_level)

            blind_dict = None
            if msg.HasField('blind_gift'):
                blind_dict = {
                    'original_gift_id': int(msg.blind_gift.original_gift_id),
                    'original_gift_name': msg.blind_gift.original_gift_name,
                    'original_gift_price': (int(msg.blind_gift.original_gift_price)
                                            if msg.blind_gift.HasField('original_gift_price') else None),
                }

            for idx, item in enumerate(msg.items):
                combo_str = item.batch_combo_id or msg.batch_combo_id
                c_uid, c_md5 = (None, None) if mystery else parse_batch_combo_clue(combo_str)
                combo_extra = {} if mystery else combo_map.get(combo_str, {})
                eff_uid = None if mystery else (base_uid or c_uid or combo_extra.get('raw_uid'))
                eff_md5 = None if mystery else (c_md5 or combo_extra.get('clue_md5') or (uid_md5(eff_uid) if eff_uid else None))
                eff_face = None if mystery else (base_face or combo_extra.get('clue_face_sha1'))
                num = int(item.num or 1)
                price = int(item.price) if item.HasField('price') else None
                total_coin = int(item.total_coin) if item.HasField('total_coin') else None
                coin_type = item.coin_type or None
                ev_ts, t_src = _ts_from_proto_or_json(item.timestamp, rec_ts)
                paid = _paid_milli_for_gift(coin_type, num, price, total_coin, blind_dict)
                g_for_val = {
                    'event_type': 'SEND_GIFT_V2',
                    'coin_type': coin_type,
                    'num': num,
                    'price': price,
                    'total_coin': total_coin,
                }
                if blind_dict is not None:
                    g_for_val['blind_gift'] = blind_dict
                out_val = gift_value(g_for_val)
                clue = {
                    'raw_id': raw_id,
                    'event_ts': ev_ts,
                    'time_source': t_src,
                    'is_mystery': int(mystery),
                    'raw_uid': eff_uid,
                    'raw_uname': combo_extra.get('uname') or base_uname,
                    'clue_md5': eff_md5,
                    'clue_face_sha1': eff_face,
                    'wealth_level': wealth or combo_extra.get('wealth_level'),
                    'guard_level': guard or combo_extra.get('medal_guard'),
                    'medal_ruid': medal_ruid or combo_extra.get('medal_ruid'),
                    'medal_room_id': medal_room_id or combo_extra.get('medal_room_id'),
                    'medal_name': medal_name or combo_extra.get('medal_name'),
                    'medal_level': medal_level or combo_extra.get('medal_level'),
                    'gift_id': str(item.gift_id),
                    'gift_name': clean_uname(item.gift_name),
                    'blind_orig_name': clean_uname(blind_dict['original_gift_name']) if blind_dict else None,
                    'quantity': num,
                    'coin_type': coin_type,
                    'paid_milli_yuan': paid,
                    'output_milli_yuan': out_val if out_val is not None else 0,
                    'tid': str(item.tid) if item.tid else None,
                    'batch_index': idx if len(msg.items) > 1 else 0,
                    'inferred_uid': None,
                    'inference_method': None,
                }
                if paid > 0:
                    all_gold_gift_times.append((len(by_fallback), ev_ts, paid))
                if clue['tid']:
                    by_tid_batch.setdefault((clue['tid'], clue['batch_index']), clue)
                by_fallback.append(clue)

        elif cmd == 'SEND_GIFT' and isinstance(data, dict):
            mystery = bool(data.get('is_mystery'))
            u_uid, u_name, u_face, u_wealth, u_mys = _extract_uinfo_dict(data.get('sender_uinfo'))
            mystery = mystery or u_mys
            combo_str = data.get('batch_combo_id')
            c_uid, c_md5 = (None, None) if mystery else parse_batch_combo_clue(combo_str)
            combo_extra = {} if mystery else combo_map.get(combo_str, {})
            eff_uid = None if mystery else (pos_int(data.get('uid')) or u_uid or c_uid or combo_extra.get('raw_uid'))
            eff_md5 = None if mystery else (c_md5 or combo_extra.get('clue_md5') or (uid_md5(eff_uid) if eff_uid else None))
            eff_face = None if mystery else (u_face or extract_face_sha1(data.get('face')) or combo_extra.get('clue_face_sha1'))
            num = pos_int(data.get('num')) or 1
            price = nonneg_int(data.get('price'))
            total_coin = nonneg_int(data.get('total_coin'))
            coin_type = data.get('coin_type') if isinstance(data.get('coin_type'), str) else None
            blind_dict = data.get('blind_gift') if isinstance(data.get('blind_gift'), dict) else None
            ev_ts, t_src = _ts_from_proto_or_json(data.get('timestamp') or data.get('start_time'), rec_ts)
            paid = _paid_milli_for_gift(coin_type, num, price, total_coin, blind_dict)
            g_for_val = {
                'event_type': 'SEND_GIFT',
                'coin_type': coin_type,
                'num': num,
                'price': price,
                'total_coin': total_coin,
            }
            if blind_dict is not None:
                g_for_val['blind_gift'] = blind_dict
            out_val = gift_value(g_for_val)
            medal = _extract_medal_from_uinfo_and_fans(data.get('sender_uinfo'), data.get('medal_info'))
            tid_raw = data.get('tid')
            tid = str(tid_raw) if isinstance(tid_raw, (int, str)) and str(tid_raw) not in ('', '0') else None
            clue = {
                'raw_id': raw_id,
                'event_ts': ev_ts,
                'time_source': t_src,
                'is_mystery': int(mystery),
                'raw_uid': eff_uid,
                'raw_uname': ('神秘人' if mystery else clean_uname(data.get('uname') or data.get('username')) or u_name),
                'clue_md5': eff_md5,
                'clue_face_sha1': eff_face,
                'wealth_level': None if mystery else (u_wealth or combo_extra.get('wealth_level') or None),
                'guard_level': valid_guard_level(data.get('guard_level')) or medal['medal_guard'],
                'medal_ruid': medal['medal_ruid'] or combo_extra.get('medal_ruid'),
                'medal_room_id': medal['medal_room_id'] or combo_extra.get('medal_room_id'),
                'medal_name': medal['medal_name'] or combo_extra.get('medal_name'),
                'medal_level': medal['medal_level'] or combo_extra.get('medal_level'),
                'gift_id': str(data.get('giftId', '')),
                'gift_name': clean_uname(data.get('giftName')),
                'blind_orig_name': clean_uname(blind_dict.get('original_gift_name')) if blind_dict else None,
                'quantity': num,
                'coin_type': coin_type,
                'paid_milli_yuan': paid,
                'output_milli_yuan': out_val if out_val is not None else 0,
                'tid': tid,
                'batch_index': 0,
                'inferred_uid': None,
                'inference_method': None,
            }
            if paid > 0:
                all_gold_gift_times.append((len(by_fallback), ev_ts, paid))
            if tid:
                by_tid_batch.setdefault((tid, 0), clue)
            by_fallback.append(clue)

        elif cmd in ('GUARD_BUY', 'SUPER_CHAT_MESSAGE', 'USER_TOAST_MSG_V2') and isinstance(data, dict):
            other_paid_receipts.append(rec_ts)
            if cmd == 'USER_TOAST_MSG_V2':
                continue
            paid = nonneg_int(data.get('price')) or 0
            if cmd == 'SUPER_CHAT_MESSAGE':
                paid *= 1000
            ev_ts, _ = _ts_from_proto_or_json(data.get('start_time') or data.get('timestamp'), rec_ts)
            if paid > 0:
                all_gold_gift_times.append((-1, ev_ts, paid))

    _apply_rank_delta_inference(by_fallback, rank_frames, all_gold_gift_times)
    if session_bounds:
        _apply_blind_box_rank_windows(by_fallback, named_ranks, receipts, other_paid_receipts, session_bounds)
    return {'by_tid_batch': by_tid_batch, 'all_gifts': by_fallback, 'combo_map': combo_map}


def _apply_rank_delta_inference(
    gift_clues: list[dict[str, Any]],
    rank_frames: list[tuple[float, dict[int, int]]],
    all_gold_gift_times: list[tuple[int, float, int]],
) -> None:
    """L5 保守场内推断：无L1-L3线索的单笔金瓜子礼物，随后15秒内高能榜唯一1人贡献值增量精确吻合则推断。"""
    if not rank_frames or not gift_clues:
        return
    rank_frames = sorted(rank_frames, key=lambda item: item[0])
    for self_idx, clue in enumerate(gift_clues):
        if (clue['blind_orig_name'] or clue['is_mystery'] or clue['raw_uid'] is not None
                or clue['clue_md5'] is not None or clue['clue_face_sha1'] is not None):
            continue
        paid = clue['paid_milli_yuan']
        if clue['coin_type'] != 'gold' or paid < 100 or paid % 100 != 0:
            continue
        expected_delta = paid // 100
        t_gift = clue['event_ts']
        prev_frames = [f for f in rank_frames if t_gift - 120.0 <= f[0] <= t_gift]
        next_frames = [f for f in rank_frames if t_gift < f[0] <= t_gift + 15.0]
        if not prev_frames or not next_frames:
            continue
        prev_ts, prev_scores = prev_frames[-1]
        board_was_full = len(prev_scores) >= 50
        has_prior_gold_gifts = any(
            idx != self_idx and gt <= prev_ts + 1.0
            for idx, gt, _ in all_gold_gift_times
        )
        for next_ts, next_scores in next_frames:
            other_gold = any(
                idx != self_idx and prev_ts - 1.0 <= gt <= next_ts + 1.0
                for idx, gt, _ in all_gold_gift_times
            )
            if other_gold:
                continue
            matched_uids: list[int] = []
            other_increases = False
            for uid, new_score in next_scores.items():
                if uid in prev_scores:
                    delta = new_score - prev_scores[uid]
                    if delta <= 0:
                        continue
                    if delta == expected_delta:
                        matched_uids.append(uid)
                    else:
                        other_increases = True
                elif new_score > 0:
                    # 基线帧不存在的观众：仅当榜单未满50人且此前全场零金瓜子礼（确证旧积分为0）时允许首刷匹配
                    if not board_was_full and not has_prior_gold_gifts and new_score == expected_delta:
                        matched_uids.append(uid)
                    else:
                        other_increases = True
            if len(matched_uids) == 1 and not other_increases:
                clue['inferred_uid'] = matched_uids[0]
                clue['inference_method'] = 'rank_delta'
                break


def _apply_blind_box_rank_windows(
    gifts: list[dict[str, Any]],
    ranks: list[tuple[float, dict[int, int], dict[int, str]]],
    receipts: dict[int, float],
    other_paid: list[float],
    bounds: tuple[float, float],
) -> None:
    """只认领同房同场、上下榜单帧能闭合的整段盲盒，金额字段保持原值。"""
    if not ranks or not receipts:
        return
    packets: dict[int, list[dict[str, Any]]] = defaultdict(list)
    tid_counts: dict[str, int] = defaultdict(int)
    for gift in gifts:
        if gift['raw_id'] in receipts:
            packets[gift['raw_id']].append(gift)
        if gift['tid']:
            tid_counts[gift['tid']] += 1

    def mask_prefix(items: list[dict[str, Any]]) -> str | None:
        mask = items[0]['raw_uname'] or ''
        if len(mask) != 4 or mask[1:] != '***' or not mask[0].isalnum() or any(
            g['raw_uname'] != mask or g['is_mystery'] for g in items
        ):
            return None
        return mask[0].casefold()

    def packet_mask(items: list[dict[str, Any]]) -> str | None:
        prefix = mask_prefix(items)
        if prefix is None:
            return None
        if any(
            not g['blind_orig_name']
            or any(g[key] is not None for key in ('raw_uid', 'clue_md5', 'clue_face_sha1', 'inferred_uid'))
            or g['coin_type'] != 'gold' or g['paid_milli_yuan'] <= 0
            or g['output_milli_yuan'] <= 0 or g['output_milli_yuan'] % 100
            or not g['tid'] or tid_counts[g['tid']] != 1
            or not bounds[0] <= g['event_ts'] <= bounds[1]
            for g in items
        ):
            return None
        return prefix

    ids = sorted(packets, key=lambda rid: (receipts[rid], rid))
    masks = {rid: packet_mask(packets[rid]) for rid in ids}
    prefixes = {rid: mask_prefix(packets[rid]) for rid in ids}
    gift_times = [receipts[rid] for rid in ids]
    others = sorted(other_paid)
    ranks.sort(key=lambda frame: frame[0])
    rank_times = [frame[0] for frame in ranks]
    by_mask: dict[str, list[list[int]]] = defaultdict(list)
    for rid in ids:
        mask = masks[rid]
        if mask is None:
            continue
        groups = by_mask[mask]
        if not groups or receipts[rid] - receipts[groups[-1][-1]] > 40:
            groups.append([])
        groups[-1].append(rid)

    for mask, groups in by_mask.items():
        for group in groups:
            first, last = receipts[group[0]], receipts[group[-1]]
            if last - first > 180:
                continue
            lo = bisect_left(rank_times, first - 3) - 1
            if lo < 0 or first - rank_times[lo] > 60:
                continue
            before, old, old_names = ranks[lo]
            group_ids = set(group)
            points = sum(g['output_milli_yuan'] for rid in group for g in packets[rid]) // 100
            after_lo = bisect_left(rank_times, last - 1)
            after_hi = bisect_right(rank_times, last + 35)
            possible: set[int] = set()
            for after, scores, names in ranks[after_lo:after_hi]:
                if bisect_left(others, before - 1) != bisect_right(others, after + 1):
                    break
                nearby = ids[bisect_left(gift_times, before - 1):bisect_right(gift_times, after + 1)]
                competitors = [rid for rid in nearby if rid not in group_ids]
                # 不把不同首字的未识别礼物并入；若其金额本身可解释目标跳变，就拒绝整个窗口。
                if any(prefixes[rid] in (None, mask) for rid in competitors) or sum(
                    g['output_milli_yuan'] for rid in competitors for g in packets[rid]
                ) >= points * 100:
                    break
                matches = {
                    uid for uid, score in scores.items() if uid in old and score - old[uid] == points
                }
                if len(matches) > 1:
                    possible.clear()
                    break
                for uid in matches:
                    if not names.get(uid, '').casefold().startswith(mask) or not old_names.get(
                        uid, ''
                    ).casefold().startswith(mask):
                        continue
                    if any(
                        other != uid and value > old.get(other, 0)
                        and names.get(other, '').casefold().startswith(mask)
                        for other, value in scores.items()
                    ):
                        continue
                    possible.add(uid)
            if len(possible) == 1:
                uid = next(iter(possible))
                for rid in group:
                    for gift in packets[rid]:
                        gift['inferred_uid'] = uid
                        gift['inference_method'] = 'blind_box_rank_window'


def extract_session_events(
    room_id: str,
    start_ts: float,
    cutoff_ts: float,
    raw_rows: list[tuple[int, str, str, dict[str, Any]]],
    timeline_rows: list[tuple[int, str, str, dict[str, Any]]] | None = None,
) -> list[dict[str, Any]]:
    """提取单场 [start_ts, cutoff_ts] 内的标准化事件明细列表（按时间与序号稳定排序）。"""
    if not raw_rows and timeline_rows:
        return _extract_from_timeline_fallback(room_id, start_ts, cutoff_ts, timeline_rows)

    clues_bundle = extract_raw_gift_clues(raw_rows, session_bounds=(start_ts, cutoff_ts))
    seen_keys: set[str] = set()
    events: list[dict[str, Any]] = []

    toast_v2_map: dict[tuple[int, int, int], dict[str, Any]] = {}
    for _, _, command, packet in raw_rows:
        cmd = str(command or packet.get('cmd') or '')
        data = packet.get('data')
        if cmd == 'USER_TOAST_MSG_V2' and isinstance(data, dict):
            u_uid, u_name, u_face, u_wealth, mystery = _extract_uinfo_dict(data.get('sender_uinfo'))
            ginfo = data.get('guard_info') if isinstance(data.get('guard_info'), dict) else {}
            pinfo = data.get('pay_info') if isinstance(data.get('pay_info'), dict) else {}
            st = pos_int(ginfo.get('start_time'))
            lvl = valid_guard_level(ginfo.get('guard_level'))
            if not mystery and u_uid and st and lvl:
                toast_v2_map[(u_uid, st, lvl)] = {
                    'uname': u_name,
                    'face': u_face,
                    'wealth': u_wealth or None,
                    'paid_price': nonneg_int(pinfo.get('price')),
                    'payflow_id': pinfo.get('payflow_id'),
                    'num': pos_int(pinfo.get('num')) or 1,
                    'role_name': clean_uname(ginfo.get('role_name')),
                }

    matched_guard_keys: set[tuple[int, int, int]] = set()
    gift_cursor = 0
    all_gifts = clues_bundle['all_gifts']

    for raw_id, received_utc, command, packet in raw_rows:
        if not isinstance(packet, dict):
            continue
        cmd = str(command or packet.get('cmd') or '')
        data = packet.get('data')
        rec_ts = epoch(received_utc)

        if cmd in ('SEND_GIFT_V2', 'SEND_GIFT'):
            while gift_cursor < len(all_gifts) and all_gifts[gift_cursor]['raw_id'] == raw_id:
                g = all_gifts[gift_cursor]
                gift_cursor += 1
                if not (start_ts <= g['event_ts'] <= cutoff_ts):
                    continue
                dedup = f"gift:{g['tid']}:{g['batch_index']}" if g['tid'] else f"gift:raw:{raw_id}:{g['batch_index']}"
                if dedup in seen_keys:
                    continue
                seen_keys.add(dedup)
                events.append({
                    'room_id': str(room_id),
                    'event_ts': g['event_ts'],
                    'time_source': g['time_source'],
                    'kind': 'gift',
                    'is_mystery': g['is_mystery'],
                    'raw_uid': g['raw_uid'],
                    'raw_uname': g['raw_uname'],
                    'clue_first_char': None if g['is_mystery'] else first_char(g['raw_uname']),
                    'clue_md5': g['clue_md5'],
                    'clue_face_sha1': g['clue_face_sha1'],
                    'clue_crc32': None,
                    'inferred_uid': g['inferred_uid'],
                    'inference_method': g['inference_method'],
                    'wealth_level': g['wealth_level'],
                    'guard_level': g['guard_level'],
                    'medal_ruid': g['medal_ruid'],
                    'medal_room_id': g['medal_room_id'],
                    'medal_name': g['medal_name'],
                    'medal_level': g['medal_level'],
                    'gift_id': g['gift_id'],
                    'gift_name': g['gift_name'],
                    'blind_orig_name': g['blind_orig_name'],
                    'quantity': g['quantity'],
                    'coin_type': g['coin_type'],
                    'paid_milli_yuan': g['paid_milli_yuan'],
                    'output_milli_yuan': g['output_milli_yuan'],
                    'content_text': None,
                    'source_ref': f'raw:{raw_id}:{g["batch_index"]}',
                })

        elif cmd == 'GUARD_BUY' and isinstance(data, dict):
            ev_ts, t_src = _ts_from_proto_or_json(data.get('start_time') or data.get('timestamp'), rec_ts)
            if not (start_ts <= ev_ts <= cutoff_ts):
                continue
            uid = pos_int(data.get('uid'))
            lvl = valid_guard_level(data.get('guard_level')) or 3
            st = pos_int(data.get('start_time')) or int(ev_ts)
            num = pos_int(data.get('num')) or 1
            guard_price = nonneg_int(data.get('price')) or 0
            toast_extra = toast_v2_map.get((uid, st, lvl), {}) if uid else {}
            if uid:
                matched_guard_keys.add((uid, st, lvl))
            order_id = data.get('order_id') or toast_extra.get('payflow_id')
            dedup = f'guard:{order_id}' if order_id else f'guard:{uid}:{st}:{lvl}:{num}'
            if dedup in seen_keys:
                continue
            seen_keys.add(dedup)
            g_name = clean_uname(data.get('gift_name')) or {1: '总督', 2: '提督', 3: '舰长'}.get(lvl, '大航海')
            paid = toast_extra.get('paid_price') if toast_extra.get('paid_price') is not None else guard_price
            uname = toast_extra.get('uname') or clean_uname(data.get('username'))
            events.append({
                'room_id': str(room_id),
                'event_ts': ev_ts,
                'time_source': t_src,
                'kind': 'guard',
                'is_mystery': 0,
                'raw_uid': uid,
                'raw_uname': uname,
                'clue_first_char': first_char(uname),
                'clue_md5': uid_md5(uid) if uid else None,
                'clue_face_sha1': toast_extra.get('face'),
                'clue_crc32': None,
                'inferred_uid': None,
                'inference_method': None,
                'wealth_level': toast_extra.get('wealth'),
                'guard_level': lvl,
                'medal_ruid': None,
                'medal_room_id': None,
                'medal_name': None,
                'medal_level': None,
                'gift_id': str(data['gift_id']) if pos_int(data.get('gift_id')) else None,
                'gift_name': g_name,
                'blind_orig_name': None,
                'quantity': num,
                'coin_type': 'gold',
                'paid_milli_yuan': paid,
                'output_milli_yuan': guard_price,
                'content_text': None,
                'source_ref': f'raw:{raw_id}',
            })

        elif cmd == 'SUPER_CHAT_MESSAGE' and isinstance(data, dict):
            ev_ts, t_src = _ts_from_proto_or_json(data.get('start_time') or data.get('timestamp'), rec_ts)
            if not (start_ts <= ev_ts <= cutoff_ts):
                continue
            sc_id = data.get('id')
            dedup = f'sc:{sc_id}' if sc_id else f'sc:raw:{raw_id}'
            if dedup in seen_keys:
                continue
            seen_keys.add(dedup)
            u_uid, u_name, u_face, u_wealth, u_mys = _extract_uinfo_dict(data.get('uinfo'))
            mystery = bool(data.get('is_mystery') or u_mys)
            user_info = data.get('user_info') if isinstance(data.get('user_info'), dict) else {}
            uid = None if mystery else (u_uid or pos_int(data.get('uid')))
            uname = '神秘人' if mystery else (u_name or clean_uname(user_info.get('uname')))
            face = None if mystery else (u_face or extract_face_sha1(user_info.get('face')))
            medal = _extract_medal_from_uinfo_and_fans(data.get('uinfo'), data.get('medal_info'))
            price_yuan = nonneg_int(data.get('price')) or 0
            milli = price_yuan * 1000
            guard = valid_guard_level(user_info.get('guard_level')) or medal['medal_guard']
            events.append({
                'room_id': str(room_id),
                'event_ts': ev_ts,
                'time_source': t_src,
                'kind': 'sc',
                'is_mystery': int(mystery),
                'raw_uid': uid,
                'raw_uname': uname,
                'clue_first_char': None if mystery else first_char(uname),
                'clue_md5': uid_md5(uid) if uid else None,
                'clue_face_sha1': face,
                'clue_crc32': None,
                'inferred_uid': None,
                'inference_method': None,
                'wealth_level': None if mystery else (u_wealth or None),
                'guard_level': guard,
                'medal_ruid': medal['medal_ruid'],
                'medal_room_id': medal['medal_room_id'],
                'medal_name': medal['medal_name'],
                'medal_level': medal['medal_level'],
                'gift_id': None,
                'gift_name': '醒目留言',
                'blind_orig_name': None,
                'quantity': 1,
                'coin_type': 'gold',
                'paid_milli_yuan': milli,
                'output_milli_yuan': milli,
                'content_text': str(data.get('message') or '')[:10000],
                'source_ref': f'raw:{raw_id}',
            })

        elif cmd.startswith('DANMU_MSG'):
            info = packet.get('info')
            if not isinstance(info, list) or len(info) < 3 or not isinstance(info[0], list) or len(info[0]) < 5:
                continue
            text = info[1]
            if not isinstance(text, str) or not text.strip():
                continue
            ev_ts, t_src = _ts_from_proto_or_json(info[0][4], rec_ts)
            if not (start_ts <= ev_ts <= cutoff_ts):
                continue
            fp_key = 'dm:' + hashlib.sha256(json.dumps(info, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
            if fp_key in seen_keys:
                continue
            seen_keys.add(fp_key)
            user_arr = info[2] if isinstance(info[2], list) else []
            u15_user = _extract_danmaku_u15_user(info)
            u_uid, u_name, u_face, u_wealth, u_mys = _extract_uinfo_dict(u15_user)
            uid = None if u_mys else (pos_int(user_arr[0] if user_arr else 0) or u_uid)
            uname = '神秘人' if u_mys else (clean_uname(user_arr[1] if len(user_arr) > 1 else None) or u_name)
            raw_crc = str(info[0][7]).strip().lower() if len(info[0]) > 7 and info[0][7] is not None else ''
            crc32_val = raw_crc if _HEX8_RE.fullmatch(raw_crc) and raw_crc != '00000000' else None
            if u_mys:
                crc32_val = None
            w16 = (pos_int(info[16][0])
                   if len(info) > 16 and isinstance(info[16], list) and info[16] else None)
            wealth = w16 or (u_wealth or None)
            guard = valid_guard_level(info[7]) if len(info) > 7 else None
            m_arr = info[3] if len(info) > 3 and isinstance(info[3], list) else []
            u_medal = _extract_medal_from_uinfo_and_fans(u15_user)
            m_level = pos_int(m_arr[0]) if len(m_arr) >= 1 else u_medal['medal_level']
            m_name = clean_uname(m_arr[1]) if len(m_arr) >= 2 else u_medal['medal_name']
            m_room_int = pos_int(m_arr[3]) if len(m_arr) >= 4 else None
            m_room = str(m_room_int) if m_room_int else u_medal['medal_room_id']
            m_ruid = (pos_int(m_arr[12]) if len(m_arr) > 12 else None) or u_medal['medal_ruid']
            events.append({
                'room_id': str(room_id),
                'event_ts': ev_ts,
                'time_source': t_src,
                'kind': 'danmaku',
                'is_mystery': int(u_mys),
                'raw_uid': uid,
                'raw_uname': uname,
                'clue_first_char': None if u_mys else first_char(uname),
                'clue_md5': None,
                'clue_face_sha1': u_face if not u_mys else None,
                'clue_crc32': crc32_val,
                'inferred_uid': None,
                'inference_method': None,
                'wealth_level': wealth,
                'guard_level': guard,
                'medal_ruid': m_ruid,
                'medal_room_id': m_room,
                'medal_name': m_name,
                'medal_level': m_level,
                'gift_id': None,
                'gift_name': None,
                'blind_orig_name': None,
                'quantity': None,
                'coin_type': None,
                'paid_milli_yuan': 0,
                'output_milli_yuan': 0,
                'content_text': text[:10000],
                'source_ref': f'raw:{raw_id}',
            })

        elif cmd in ('INTERACT_WORD', 'INTERACT_WORD_V2') and isinstance(data, dict):
            if cmd == 'INTERACT_WORD_V2' and isinstance(data.get('pb'), str):
                try:
                    msg = _decode_pb(WareInteract, data)
                except Exception:
                    continue
                if msg is None or int(msg.msg_type) != 1:
                    continue
                mystery = bool(msg.uinfo.base.is_mystery)
                ev_ts, t_src = _parse_entry_time(int(msg.timestamp), int(msg.timestamp_ms), rec_ts)
                if not (start_ts <= ev_ts <= cutoff_ts):
                    continue
                uid = None if mystery else pos_int(msg.uinfo.uid or msg.uid)
                uname = '神秘人' if mystery else clean_uname(msg.uinfo.base.name or msg.username)
                face = None if mystery else extract_face_sha1(msg.uinfo.base.face)
                wealth = None if mystery else (pos_int(msg.uinfo.wealth.level) or None)
                guard = (valid_guard_level(msg.guard_type)
                         or valid_guard_level(msg.uinfo.guard.level)
                         or valid_guard_level(msg.fans_medal.guard_level))
                m_ruid = pos_int(msg.fans_medal.target_id or msg.uinfo.medal.ruid)
                m_room_int = pos_int(msg.fans_medal.anchor_roomid)
                m_room = str(m_room_int) if m_room_int else None
                m_name = clean_uname(msg.fans_medal.medal_name or msg.uinfo.medal.name)
                m_level = pos_int(msg.fans_medal.medal_level or msg.uinfo.medal.level)
                trig = int(msg.trigger_time or 0)
            else:
                if nonneg_int(data.get('msg_type')) != 1:
                    continue
                u_uid, u_name, u_face, u_wealth, u_mys = _extract_uinfo_dict(data.get('uinfo'))
                mystery = bool(data.get('is_mystery') or u_mys)
                ev_ts, t_src = _parse_entry_time(
                    nonneg_int(data.get('timestamp')), nonneg_int(data.get('timestamp_ms')), rec_ts
                )
                if not (start_ts <= ev_ts <= cutoff_ts):
                    continue
                uid = None if mystery else (u_uid or pos_int(data.get('uid')))
                uname = '神秘人' if mystery else (u_name or clean_uname(data.get('uname') or data.get('username')))
                face = None if mystery else u_face
                wealth = None if mystery else (u_wealth or None)
                medal = _extract_medal_from_uinfo_and_fans(data.get('uinfo'), data.get('fans_medal'))
                guard = valid_guard_level(data.get('guard_level') or data.get('guard_type')) or medal['medal_guard']
                m_ruid, m_room, m_name, m_level = (
                    medal['medal_ruid'], medal['medal_room_id'], medal['medal_name'], medal['medal_level']
                )
                trig = nonneg_int(data.get('trigger_time')) or 0

            dedup = f'entry:{uid}:{int(ev_ts)}:{trig}:{uname}' if not mystery else f'entry:raw:{raw_id}'
            if dedup in seen_keys:
                continue
            seen_keys.add(dedup)
            events.append({
                'room_id': str(room_id),
                'event_ts': ev_ts,
                'time_source': t_src,
                'kind': 'entry',
                'is_mystery': int(mystery),
                'raw_uid': uid,
                'raw_uname': uname,
                'clue_first_char': None if mystery else first_char(uname),
                'clue_md5': None,
                'clue_face_sha1': face,
                'clue_crc32': None,
                'inferred_uid': None,
                'inference_method': None,
                'wealth_level': wealth,
                'guard_level': guard,
                'medal_ruid': m_ruid,
                'medal_room_id': m_room,
                'medal_name': m_name,
                'medal_level': m_level,
                'gift_id': None,
                'gift_name': None,
                'blind_orig_name': None,
                'quantity': None,
                'coin_type': None,
                'paid_milli_yuan': 0,
                'output_milli_yuan': 0,
                'content_text': None,
                'source_ref': f'raw:{raw_id}',
            })

        elif cmd in ('ENTRY_EFFECT', 'ENTRY_EFFECT_MUST_RECEIVE') and isinstance(data, dict):
            if not (start_ts <= rec_ts <= cutoff_ts):
                continue
            u_uid, u_name, u_face, u_wealth, u_mys = _extract_uinfo_dict(data.get('uinfo'))
            mystery = bool(data.get('is_mystery') or u_mys)
            uid = None if mystery else (u_uid or pos_int(data.get('uid')))
            if uid is None and not mystery:
                continue
            if not mystery and not u_name:
                cw = str(data.get('copy_writing') or data.get('copy_writing_v2') or '')
                m = _COPY_NAME_RE.search(cw)
                if m:
                    u_name = clean_uname(m.group(1))
            medal = _extract_medal_from_uinfo_and_fans(data.get('uinfo'))
            guard = valid_guard_level(data.get('privilege_type')) or medal['medal_guard']
            dedup = f'entry_effect:{uid}:{int(rec_ts // 2)}'
            if dedup in seen_keys:
                continue
            seen_keys.add(dedup)
            events.append({
                'room_id': str(room_id),
                'event_ts': rec_ts,
                'time_source': 'received',
                'kind': 'entry',
                'is_mystery': int(mystery),
                'raw_uid': uid,
                'raw_uname': '神秘人' if mystery else u_name,
                'clue_first_char': None if mystery else first_char(u_name),
                'clue_md5': None,
                'clue_face_sha1': None if mystery else (u_face or extract_face_sha1(data.get('face'))),
                'clue_crc32': None,
                'inferred_uid': None,
                'inference_method': None,
                'wealth_level': None if mystery else (u_wealth or None),
                'guard_level': guard,
                'medal_ruid': medal['medal_ruid'],
                'medal_room_id': medal['medal_room_id'],
                'medal_name': medal['medal_name'],
                'medal_level': medal['medal_level'],
                'gift_id': None,
                'gift_name': None,
                'blind_orig_name': None,
                'quantity': None,
                'coin_type': None,
                'paid_milli_yuan': 0,
                'output_milli_yuan': 0,
                'content_text': None,
                'source_ref': f'raw:{raw_id}',
            })

        elif cmd == 'LIKE_INFO_V3_CLICK' and isinstance(data, dict):
            if not (start_ts <= rec_ts <= cutoff_ts):
                continue
            u_uid, u_name, u_face, u_wealth, u_mys = _extract_uinfo_dict(data.get('uinfo'))
            mystery = bool(data.get('is_mystery') or u_mys)
            uid = None if mystery else (u_uid or pos_int(data.get('uid')))
            uname = '神秘人' if mystery else (u_name or clean_uname(data.get('uname')))
            medal = _extract_medal_from_uinfo_and_fans(data.get('uinfo'), data.get('fans_medal'))
            u_guard_obj = (data.get('uinfo') or {}).get('guard') if isinstance(data.get('uinfo'), dict) else {}
            guard = valid_guard_level((u_guard_obj or {}).get('level')) or medal['medal_guard']
            events.append({
                'room_id': str(room_id),
                'event_ts': rec_ts,
                'time_source': 'received',
                'kind': 'like',
                'is_mystery': int(mystery),
                'raw_uid': uid,
                'raw_uname': uname,
                'clue_first_char': None if mystery else first_char(uname),
                'clue_md5': None,
                'clue_face_sha1': None if mystery else u_face,
                'clue_crc32': None,
                'inferred_uid': None,
                'inference_method': None,
                'wealth_level': None if mystery else (u_wealth or None),
                'guard_level': guard,
                'medal_ruid': medal['medal_ruid'],
                'medal_room_id': medal['medal_room_id'],
                'medal_name': medal['medal_name'],
                'medal_level': medal['medal_level'],
                'gift_id': None,
                'gift_name': None,
                'blind_orig_name': None,
                'quantity': None,
                'coin_type': None,
                'paid_milli_yuan': 0,
                'output_milli_yuan': 0,
                'content_text': None,
                'source_ref': f'raw:{raw_id}',
            })

    # 若存在孤立的 USER_TOAST_MSG_V2（无对应 GUARD_BUY），补入单条 guard 事件防止漏记
    for (uid, st, lvl), extra in toast_v2_map.items():
        if (uid, st, lvl) in matched_guard_keys:
            continue
        ev_ts = float(st)
        if not (start_ts <= ev_ts <= cutoff_ts):
            continue
        dedup = f"guard:{extra['payflow_id']}" if extra.get('payflow_id') else f'guard:{uid}:{st}:{lvl}:{extra["num"]}'
        if dedup in seen_keys:
            continue
        seen_keys.add(dedup)
        paid = extra['paid_price'] or 0
        events.append({
            'room_id': str(room_id),
            'event_ts': ev_ts,
            'time_source': 'event',
            'kind': 'guard',
            'is_mystery': 0,
            'raw_uid': uid,
            'raw_uname': extra['uname'],
            'clue_first_char': first_char(extra['uname']),
            'clue_md5': uid_md5(uid),
            'clue_face_sha1': extra['face'],
            'clue_crc32': None,
            'inferred_uid': None,
            'inference_method': None,
            'wealth_level': extra['wealth'],
            'guard_level': lvl,
            'medal_ruid': None,
            'medal_room_id': None,
            'medal_name': None,
            'medal_level': None,
            'gift_id': None,
            'gift_name': extra['role_name'] or {1: '总督', 2: '提督', 3: '舰长'}.get(lvl, '大航海'),
            'blind_orig_name': None,
            'quantity': extra['num'],
            'coin_type': 'gold',
            'paid_milli_yuan': paid,
            'output_milli_yuan': paid,
            'content_text': None,
            'source_ref': f'toast_v2:{uid}:{st}',
        })

    events.sort(key=lambda row: (row['event_ts'], row['source_ref']))
    return events


def _extract_from_timeline_fallback(
    room_id: str,
    start_ts: float,
    cutoff_ts: float,
    timeline_rows: list[tuple[int, str, str, dict[str, Any]]],
) -> list[dict[str, Any]]:
    """无 raw_events 早期库的场次事件回退提取。"""
    events: list[dict[str, Any]] = []
    for tl_id, kind, at_utc, data in timeline_rows:
        if kind not in ('gift', 'danmaku', 'entry', 'like') or not isinstance(data, dict):
            continue
        ev_ts = epoch(at_utc)
        if not (start_ts <= ev_ts <= cutoff_ts):
            continue
        mystery = bool(data.get('sender_hidden'))
        gift = data.get('gift') if isinstance(data.get('gift'), dict) else {}
        uid = None if mystery or data.get('identity_status') == 'unresolved' else (
            pos_int(data.get('sender_uid')) or pos_int(gift.get('sender_uid'))
        )
        uname = clean_uname(data.get('author'))
        medal = data.get('medal') if isinstance(data.get('medal'), dict) else {}
        norm_kind = kind
        paid = 0
        out_val = 0
        t_src = 'event'
        if kind == 'gift':
            ev_type = gift.get('event_type')
            if ev_type == 'GUARD_BUY':
                norm_kind = 'guard'
                paid = out_val = nonneg_int(gift.get('guard_price_gold')) or 0
            elif ev_type == 'SUPER_CHAT_MESSAGE':
                norm_kind = 'sc'
                paid = out_val = (nonneg_int(gift.get('price_yuan')) or 0) * 1000
            else:
                num = pos_int(gift.get('num')) or 1
                paid = _paid_milli_for_gift(
                    gift.get('coin_type'), num, nonneg_int(gift.get('price')),
                    nonneg_int(gift.get('total_coin')),
                    gift.get('blind_gift') if isinstance(gift.get('blind_gift'), dict) else None,
                )
                val = gift_value(gift)
                out_val = val if val is not None else 0
            t_src = 'received' if gift.get('time_source') == 'received' else 'event'
        elif kind == 'like':
            t_src = 'received'
        events.append({
            'room_id': str(room_id),
            'event_ts': ev_ts,
            'time_source': t_src,
            'kind': norm_kind,
            'is_mystery': int(mystery),
            'raw_uid': uid,
            'raw_uname': uname,
            'clue_first_char': None if mystery else first_char(uname),
            'clue_md5': uid_md5(uid) if uid and kind == 'gift' else None,
            'clue_face_sha1': None,
            'clue_crc32': None,
            'inferred_uid': None,
            'inference_method': None,
            'wealth_level': None,
            'guard_level': valid_guard_level(data.get('guard_level')),
            'medal_ruid': None,
            'medal_room_id': str(medal['room_id']) if pos_int(medal.get('room_id')) else None,
            'medal_name': clean_uname(medal.get('name')),
            'medal_level': pos_int(medal.get('level')),
            'gift_id': str(gift['gift_id']) if gift.get('gift_id') else None,
            'gift_name': clean_uname(gift.get('gift_name')),
            'blind_orig_name': clean_uname((gift.get('blind_gift') or {}).get('original_gift_name'))
            if isinstance(gift.get('blind_gift'), dict) else None,
            'quantity': pos_int(gift.get('num')),
            'coin_type': gift.get('coin_type'),
            'paid_milli_yuan': paid,
            'output_milli_yuan': out_val,
            'content_text': str(data.get('text') or '')[:10000] if norm_kind in ('danmaku', 'sc') else None,
            'source_ref': f'timeline:{tl_id}',
        })
    return events


_PK_INTERVAL_COMMANDS = frozenset({
    'PK_BATTLE_PRE_NEW',
    'PK_BATTLE_PRE',
    'PK_BATTLE_START_NEW',
    'PK_BATTLE_START',
    'PK_BATTLE_PROCESS_NEW',
    'PK_BATTLE_PROCESS',
    'PK_BATTLE_END',
    'PK_BATTLE_SETTLE_NEW',
    'PK_BATTLE_VIDEO_PUNISH_BEGIN',
    'PK_BATTLE_PUNISH_END',
    'PK_BATTLE_ABNORMAL',
    'PK_INFO',
})

_VOICE_JOIN_COMMANDS = frozenset({
    'VOICE_JOIN_STATUS',
    'VOICE_JOIN_LIST',
    'VOICE_JOIN_ROOM_COUNT_INFO',
})


def extract_room_metrics(
    room_id: str,
    start_ts: float,
    cutoff_ts: float,
    raw_rows: list[tuple[int, str, str, dict[str, Any]]],
) -> list[dict[str, Any]]:
    """提取场次 [start_ts, cutoff_ts] 按分钟桶聚合的房间状态时间序列（桶内取最新观测）。"""
    buckets: dict[int, dict[str, tuple[float, int, int]]] = {}
    rid = str(room_id)

    for raw_id, received_utc, command, packet in raw_rows:
        if not isinstance(packet, dict):
            continue
        cmd = str(command or packet.get('cmd') or '')
        if cmd not in ('ONLINE_RANK_COUNT', 'WATCHED_CHANGE', 'ROOM_REAL_TIME_MESSAGE_UPDATE'):
            continue
        data = packet.get('data')
        if not isinstance(data, dict):
            continue
        try:
            rec_ts = epoch(received_utc)
        except Exception:
            continue
        if not (start_ts <= rec_ts <= cutoff_ts):
            continue
        minute_ts = int(rec_ts // 60) * 60
        bucket = buckets.setdefault(minute_ts, {})
        order_key = (rec_ts, int(raw_id or 0))

        def _put(col: str, val: int | None) -> None:
            if val is None:
                return
            prev = bucket.get(col)
            if prev is None or order_key >= (prev[0], prev[1]):
                bucket[col] = (order_key[0], order_key[1], val)

        if cmd == 'ONLINE_RANK_COUNT':
            _put('online_count', nonneg_int(data.get('online_count')))
        elif cmd == 'WATCHED_CHANGE':
            _put('watched_count', nonneg_int(data.get('num')))
        elif cmd == 'ROOM_REAL_TIME_MESSAGE_UPDATE':
            _put('fans_count', nonneg_int(data.get('fans')))
            _put('fans_club_count', nonneg_int(data.get('fans_club')))

    out: list[dict[str, Any]] = []
    for minute_ts in sorted(buckets):
        b = buckets[minute_ts]
        if not b:
            continue
        out.append({
            'minute_ts': minute_ts,
            'room_id': rid,
            'online_count': b['online_count'][2] if 'online_count' in b else None,
            'watched_count': b['watched_count'][2] if 'watched_count' in b else None,
            'fans_count': b['fans_count'][2] if 'fans_count' in b else None,
            'fans_club_count': b['fans_club_count'][2] if 'fans_club_count' in b else None,
        })
    return out


def _clamp_session_interval(iv_start: float, iv_end: float, start_ts: float, cutoff_ts: float) -> tuple[float, float] | None:
    if iv_end < start_ts or iv_start > cutoff_ts:
        return None
    cs = max(start_ts, iv_start)
    ce = min(cutoff_ts, max(cs, iv_end))
    return cs, ce


def extract_cohost_intervals(
    room_id: str,
    start_ts: float,
    cutoff_ts: float,
    raw_rows: list[tuple[int, str, str, dict[str, Any]]],
    *,
    platform_room_id: str | None = None,
    anchor_uid: int | None = None,
) -> list[dict[str, Any]]:
    """从单场 raw_events 归并 PK 对战、PK 惩罚与语音连麦状态区间。"""
    rid = str(room_id)
    self_rooms = {rid}
    if platform_room_id:
        self_rooms.add(str(platform_room_id))

    def _is_self(cand_room: str | None, cand_uid: int | None) -> bool:
        if cand_room and cand_room in self_rooms:
            return True
        if anchor_uid and cand_uid and cand_uid == anchor_uid:
            return True
        return False

    ordered_rows: list[tuple[float, int, str, dict[str, Any]]] = []
    for raw_id, received_utc, command, packet in raw_rows:
        if not isinstance(packet, dict):
            continue
        cmd = str(command or packet.get('cmd') or '')
        data_top = packet.get('data') if isinstance(packet.get('data'), dict) else {}
        if cmd == 'ROOM_REAL_TIME_MESSAGE_UPDATE':
            canon_rid = pos_int(data_top.get('roomid'))
            if canon_rid:
                self_rooms.add(str(canon_rid))
            continue
        if cmd == 'PK_INFO' and anchor_uid and isinstance(data_top.get('members'), list):
            for mem in data_top['members']:
                if isinstance(mem, dict) and pos_int(mem.get('uid')) == anchor_uid:
                    m_rid = pos_int(mem.get('room_id'))
                    if m_rid:
                        self_rooms.add(str(m_rid))
        if cmd not in _PK_INTERVAL_COMMANDS and cmd not in _VOICE_JOIN_COMMANDS:
            continue
        try:
            rec_ts = epoch(received_utc)
        except Exception:
            continue
        ordered_rows.append((rec_ts, int(raw_id or 0), cmd, packet))
    ordered_rows.sort(key=lambda item: (item[0], item[1]))

    battles: dict[str, dict[str, Any]] = {}
    intervals: list[dict[str, Any]] = []

    def _purge_self_from_opponents(b: dict[str, Any], newly_self_room: str) -> None:
        self_rooms.add(newly_self_room)
        b['opponents'].pop(newly_self_room, None)
        if b['opponent_room_id'] == newly_self_room:
            b['opponent_room_id'] = next(
                (v['room_id'] for v in b['opponents'].values() if v.get('room_id') and v['room_id'] not in self_rooms),
                None,
            )

    def _record_opponent(b: dict[str, Any], opp_uid: int | None, opp_room: str | None, opp_uname: str | None,
                         votes: int | None = None) -> None:
        if _is_self(opp_room, opp_uid):
            if opp_room and opp_room not in self_rooms:
                _purge_self_from_opponents(b, opp_room)
            return
        if not opp_uid and not opp_room and not opp_uname:
            return
        if opp_uid and b['opponent_uid'] is None:
            b['opponent_uid'] = opp_uid
        if opp_room and b['opponent_room_id'] is None:
            b['opponent_room_id'] = opp_room
        if opp_uname and not is_masked_name(opp_uname):
            b['opponent_uname'] = opp_uname
        elif opp_uname and b['opponent_uname'] is None:
            b['opponent_uname'] = opp_uname
        if votes is not None:
            b['opponent_votes'] = votes
        key = opp_room or (f'uid:{opp_uid}' if opp_uid else f'name:{opp_uname}')
        entry = b['opponents'].setdefault(key, {'uid': None, 'room_id': None, 'uname': None, 'votes': None})
        if opp_uid:
            entry['uid'] = opp_uid
        if opp_room:
            entry['room_id'] = opp_room
        if opp_uname:
            entry['uname'] = opp_uname
        if votes is not None:
            entry['votes'] = votes

    def _update_side_pair(b: dict[str, Any], data: dict[str, Any]) -> None:
        sides: list[tuple[str | None, int | None, Any]] = []
        for side_key in ('init_info', 'match_info'):
            side = data.get(side_key)
            if not isinstance(side, dict):
                continue
            s_room_int = pos_int(side.get('room_id'))
            s_room = str(s_room_int) if s_room_int else None
            s_votes = nonneg_int(side.get('votes'))
            w_type = side.get('winner_type') if side.get('winner_type') is not None else side.get('result_type')
            sides.append((s_room, s_votes, w_type))

        if len(sides) == 2 and b['opponent_room_id']:
            r0, r1 = sides[0][0], sides[1][0]
            if r0 == b['opponent_room_id'] and r1 and r1 != r0:
                _purge_self_from_opponents(b, r1)
            elif r1 == b['opponent_room_id'] and r0 and r0 != r1:
                _purge_self_from_opponents(b, r0)

        has_confirmed_self = any(_is_self(sr, None) for sr, _, _ in sides)
        for s_room, s_votes, w_type in sides:
            if _is_self(s_room, None):
                if s_votes is not None:
                    b['self_votes'] = s_votes
                if w_type in (2, -1):
                    b['self_winner'] = 1 if w_type == 2 else 0
                elif w_type in (1, 3) and b['self_winner'] is None:
                    b['self_winner'] = 0
            elif s_room and (has_confirmed_self or s_room == b['opponent_room_id']):
                _record_opponent(b, None, s_room, None, votes=s_votes)
                if w_type in (2, -1) and b['self_winner'] is None:
                    b['self_winner'] = 0 if w_type == 2 else 1

    active_voice: dict[str, Any] | None = None
    active_queue: dict[str, Any] | None = None
    recent_queue_end_ts: float | None = None

    def _flush_voice(close_ts: float, closed_by: str) -> None:
        nonlocal active_voice
        if active_voice is None:
            return
        v = active_voice
        active_voice = None
        clamped = _clamp_session_interval(v['start_ts'], max(v['start_ts'], close_ts), start_ts, cutoff_ts)
        if clamped is None:
            return
        summary = {
            'channel': v['channel'],
            'channel_type': v['channel_type'],
            'event_count': v['event_count'],
            'had_queue_before': v['had_queue_before'],
            'closed_by': closed_by,
            'commands_seen': v['commands_seen'],
        }
        intervals.append({
            'room_id': rid,
            'kind': 'voice_join',
            'start_ts': clamped[0],
            'end_ts': clamped[1],
            'battle_id': v['channel'],
            'opponent_uid': v['opponent_uid'],
            'opponent_room_id': v['opponent_room_id'],
            'opponent_uname': v['opponent_uname'],
            'raw_summary_json': json.dumps(summary, ensure_ascii=False, sort_keys=True),
        })

    def _flush_queue(close_ts: float, closed_by: str) -> None:
        nonlocal active_queue, recent_queue_end_ts
        if active_queue is None:
            return
        q = active_queue
        active_queue = None
        recent_queue_end_ts = close_ts
        clamped = _clamp_session_interval(q['start_ts'], max(q['start_ts'], close_ts), start_ts, cutoff_ts)
        if clamped is None:
            return
        summary = {
            'max_apply_count': q['max_apply_count'],
            'max_notify_count': q['max_notify_count'],
            'event_count': q['event_count'],
            'closed_by': closed_by,
            'commands_seen': q['commands_seen'],
        }
        intervals.append({
            'room_id': rid,
            'kind': 'voice_join_queue',
            'start_ts': clamped[0],
            'end_ts': clamped[1],
            'battle_id': None,
            'opponent_uid': None,
            'opponent_room_id': None,
            'opponent_uname': None,
            'raw_summary_json': json.dumps(summary, ensure_ascii=False, sort_keys=True),
        })

    for rec_ts, _, cmd, packet in ordered_rows:
        data = packet.get('data') if isinstance(packet.get('data'), dict) else {}

        if cmd in _PK_INTERVAL_COMMANDS:
            basic = data.get('pk_basic') if isinstance(data.get('pk_basic'), dict) else {}
            pk_id_int = pos_int(packet.get('pk_id')) or pos_int(data.get('pk_id')) or pos_int(basic.get('pk_id'))
            if pk_id_int is None:
                continue
            bid = str(pk_id_int)
            b = battles.get(bid)
            if b is None:
                b = {
                    'battle_id': bid,
                    'first_obs_ts': rec_ts,
                    'last_obs_ts': rec_ts,
                    'pre_ts': None,
                    'estimated_battle_start': None,
                    'battle_start_ts': None,
                    'planned_end_ts': None,
                    'explicit_battle_end_ts': None,
                    'info_settle_ts': None,
                    'sprint_duration': 10,
                    'saw_active_battle': False,
                    'punish_seen': False,
                    'punish_start_ts': None,
                    'planned_punish_end_ts': None,
                    'actual_punish_end_ts': None,
                    'punish_text': None,
                    'muti_pk_type': None,
                    'opponent_uid': None,
                    'opponent_room_id': None,
                    'opponent_uname': None,
                    'opponents': {},
                    'self_votes': None,
                    'opponent_votes': None,
                    'self_winner': None,
                    'commands_seen': [],
                }
                battles[bid] = b
            b['last_obs_ts'] = max(b['last_obs_ts'], rec_ts)
            if cmd not in b['commands_seen']:
                b['commands_seen'].append(cmd)

            pkt_ts = _ts_from_proto_or_json(packet.get('timestamp') or data.get('timestamp'), rec_ts)[0]

            if cmd in ('PK_BATTLE_PRE_NEW', 'PK_BATTLE_PRE'):
                b['saw_active_battle'] = True
                b['pre_ts'] = pkt_ts if b['pre_ts'] is None else min(b['pre_ts'], pkt_ts)
                pre_timer = pos_int(data.get('pre_timer'))
                if pre_timer and b['estimated_battle_start'] is None:
                    b['estimated_battle_start'] = pkt_ts + pre_timer
                opp_room_int = pos_int(data.get('room_id'))
                _record_opponent(
                    b,
                    pos_int(data.get('uid')),
                    str(opp_room_int) if opp_room_int else None,
                    clean_uname(data.get('uname')),
                )

            elif cmd in ('PK_BATTLE_START_NEW', 'PK_BATTLE_START'):
                b['saw_active_battle'] = True
                s_ts = _ts_from_proto_or_json(data.get('pk_start_time'), pkt_ts)[0]
                b['battle_start_ts'] = s_ts if b['battle_start_ts'] is None else min(b['battle_start_ts'], s_ts)
                e_raw = pos_int(data.get('pk_frozen_time')) or pos_int(data.get('pk_end_time'))
                if e_raw:
                    b['planned_end_ts'] = float(e_raw)
                _update_side_pair(b, data)

            elif cmd in ('PK_BATTLE_PROCESS_NEW', 'PK_BATTLE_PROCESS'):
                b['saw_active_battle'] = True
                _update_side_pair(b, data)

            elif cmd in ('PK_BATTLE_END', 'PK_BATTLE_SETTLE_NEW'):
                b['saw_active_battle'] = True
                if b['explicit_battle_end_ts'] is None or pkt_ts < b['explicit_battle_end_ts']:
                    b['explicit_battle_end_ts'] = pkt_ts
                _update_side_pair(b, data)
                if cmd == 'PK_BATTLE_SETTLE_NEW':
                    p_end_plan = pos_int(data.get('punish_end_time'))
                    if p_end_plan:
                        b['planned_punish_end_ts'] = float(p_end_plan)
                    pname = clean_uname(data.get('punish_name'))
                    if pname:
                        b['punish_text'] = pname

            elif cmd == 'PK_BATTLE_ABNORMAL':
                b['saw_active_battle'] = True
                if b['explicit_battle_end_ts'] is None or pkt_ts < b['explicit_battle_end_ts']:
                    b['explicit_battle_end_ts'] = pkt_ts

            elif cmd == 'PK_BATTLE_VIDEO_PUNISH_BEGIN':
                b['punish_seen'] = True
                b['punish_start_ts'] = pkt_ts if b['punish_start_ts'] is None else min(b['punish_start_ts'], pkt_ts)
                if b['explicit_battle_end_ts'] is None:
                    b['explicit_battle_end_ts'] = pkt_ts
                vp = data.get('video_punish') if isinstance(data.get('video_punish'), dict) else {}
                dur = pos_int(vp.get('duration'))
                if dur:
                    b['planned_punish_end_ts'] = pkt_ts + dur
                pname = clean_uname(vp.get('punish_name'))
                if pname:
                    b['punish_text'] = pname
                _update_side_pair(b, data)

            elif cmd == 'PK_BATTLE_PUNISH_END':
                b['actual_punish_end_ts'] = pkt_ts

            elif cmd == 'PK_INFO':
                st_code = nonneg_int(basic.get('status'))
                b_start = pos_int(basic.get('start_time'))
                b_end = pos_int(basic.get('end_time'))
                b_pend = pos_int(basic.get('punish_end_time'))
                sprint = pos_int(basic.get('sprint_duration'))
                if sprint:
                    b['sprint_duration'] = sprint
                mtype = nonneg_int(basic.get('muti_pk_type'))
                if mtype is not None:
                    b['muti_pk_type'] = mtype
                ptext = clean_uname(basic.get('punish_text'))
                if ptext and b['punish_text'] is None:
                    b['punish_text'] = ptext
                pk_play = data.get('pk_play') if isinstance(data.get('pk_play'), dict) else {}
                pre_dur = nonneg_int(pk_play.get('pre_duration'))
                if b_start:
                    if b['pre_ts'] is None:
                        b['pre_ts'] = float(b_start)
                    if pre_dur is not None and b['estimated_battle_start'] is None:
                        b['estimated_battle_start'] = float(b_start + pre_dur)
                if b_end:
                    b['planned_end_ts'] = float(b_end)
                if b_pend:
                    b['planned_punish_end_ts'] = float(b_pend)

                members = data.get('members')
                if isinstance(members, list):
                    for mem in members:
                        if not isinstance(mem, dict):
                            continue
                        m_uid = pos_int(mem.get('uid'))
                        m_room_int = pos_int(mem.get('room_id'))
                        m_room = str(m_room_int) if m_room_int else None
                        m_uname = clean_uname(mem.get('uname'))
                        m_votes = nonneg_int(mem.get('votes'))
                        m_win = nonneg_int(mem.get('is_winner'))
                        if _is_self(m_room, m_uid):
                            if m_room and m_room not in self_rooms:
                                _purge_self_from_opponents(b, m_room)
                            if m_votes is not None:
                                b['self_votes'] = m_votes
                            if m_win in (0, 1) and st_code is not None and st_code >= 304:
                                b['self_winner'] = m_win
                        else:
                            _record_opponent(b, m_uid, m_room, m_uname, votes=m_votes)

                if st_code in (101, 201, 301, 304, 401, 404, 501):
                    b['saw_active_battle'] = True
                if st_code in (304, 401, 404, 501) and b['info_settle_ts'] is None:
                    b['info_settle_ts'] = rec_ts
                elif st_code in (601, 602, 610):
                    b['punish_seen'] = True
                    if b['punish_start_ts'] is None:
                        b['punish_start_ts'] = rec_ts
                    if b['info_settle_ts'] is None:
                        b['info_settle_ts'] = rec_ts
                elif st_code == 1001:
                    if b['punish_seen'] and b['actual_punish_end_ts'] is None:
                        b['actual_punish_end_ts'] = rec_ts
                    if b['info_settle_ts'] is None:
                        b['info_settle_ts'] = rec_ts

        elif cmd == 'VOICE_JOIN_STATUS':
            st = nonneg_int(data.get('status'))
            cur_ts = _ts_from_proto_or_json(data.get('current_time'), rec_ts)[0]
            if st == 1:
                if active_queue is not None:
                    _flush_queue(cur_ts, 'voice_connected')
                s_at = pos_int(data.get('start_at'))
                v_start = float(s_at) if s_at else cur_ts
                mystery = bool(data.get('is_mystery'))
                opp_uid = None if mystery else pos_int(data.get('uid'))
                opp_uname = '神秘人' if mystery else clean_uname(data.get('user_name') or data.get('uname'))
                opp_room_int = pos_int(data.get('room_id'))
                opp_room = (
                    str(opp_room_int)
                    if opp_room_int and not _is_self(str(opp_room_int), opp_uid)
                    else None
                )
                channel = clean_uname(data.get('channel'))
                ctype = clean_uname(data.get('channel_type')) or 'voice'
                if active_voice is not None:
                    same_peer = (
                        (channel and active_voice['channel'] == channel)
                        or (opp_uid and active_voice['opponent_uid'] == opp_uid)
                    )
                    if same_peer:
                        active_voice['event_count'] += 1
                        if opp_uid and active_voice['opponent_uid'] is None:
                            active_voice['opponent_uid'] = opp_uid
                        if opp_uname and active_voice['opponent_uname'] is None:
                            active_voice['opponent_uname'] = opp_uname
                        continue
                    _flush_voice(cur_ts, 'switched_speaker')
                had_q = bool(recent_queue_end_ts is not None and abs(cur_ts - recent_queue_end_ts) <= 60.0)
                active_voice = {
                    'start_ts': v_start,
                    'channel': channel,
                    'channel_type': ctype,
                    'opponent_uid': opp_uid,
                    'opponent_room_id': opp_room,
                    'opponent_uname': opp_uname,
                    'event_count': 1,
                    'had_queue_before': had_q,
                    'commands_seen': [cmd],
                }
            elif st == 0:
                _flush_voice(cur_ts, 'status_off')

        elif cmd in ('VOICE_JOIN_LIST', 'VOICE_JOIN_ROOM_COUNT_INFO'):
            joined_list = data.get('list')
            if isinstance(joined_list, list) and joined_list and active_voice is None:
                first_j = next((item for item in joined_list if isinstance(item, dict)), None)
                if first_j is not None:
                    mystery = bool(first_j.get('is_mystery'))
                    j_uid = None if mystery else pos_int(first_j.get('uid'))
                    j_name = '神秘人' if mystery else clean_uname(first_j.get('user_name') or first_j.get('uname'))
                    active_voice = {
                        'start_ts': rec_ts,
                        'channel': clean_uname(first_j.get('channel')),
                        'channel_type': 'voice',
                        'opponent_uid': j_uid,
                        'opponent_room_id': None,
                        'opponent_uname': j_name,
                        'event_count': 1,
                        'had_queue_before': active_queue is not None,
                        'commands_seen': [cmd],
                    }
            apply_cnt = nonneg_int(data.get('apply_count')) or 0
            notify_cnt = nonneg_int(data.get('notify_count')) or 0
            if apply_cnt > 0 or notify_cnt > 0:
                if active_voice is None:
                    if active_queue is None:
                        active_queue = {
                            'start_ts': rec_ts,
                            'max_apply_count': apply_cnt,
                            'max_notify_count': notify_cnt,
                            'event_count': 1,
                            'commands_seen': [cmd],
                        }
                    else:
                        active_queue['max_apply_count'] = max(active_queue['max_apply_count'], apply_cnt)
                        active_queue['max_notify_count'] = max(active_queue['max_notify_count'], notify_cnt)
                        active_queue['event_count'] += 1
                        if cmd not in active_queue['commands_seen']:
                            active_queue['commands_seen'].append(cmd)
            elif active_queue is not None:
                _flush_queue(rec_ts, 'queue_cleared')

    if active_voice is not None:
        _flush_voice(cutoff_ts, 'session_cutoff')
    if active_queue is not None:
        _flush_queue(cutoff_ts, 'session_cutoff')

    for b in battles.values():
        eff_b_start = b['battle_start_ts'] or b['estimated_battle_start']
        pk_candidates = [x for x in (b['pre_ts'], eff_b_start) if x is not None]
        pk_start = min(pk_candidates) if pk_candidates else b['first_obs_ts']

        if b['explicit_battle_end_ts'] is not None:
            pk_end = b['explicit_battle_end_ts']
        elif b['info_settle_ts'] is not None:
            if b['planned_end_ts'] is not None and b['info_settle_ts'] > b['planned_end_ts'] + 15.0:
                pk_end = b['planned_end_ts']
            else:
                pk_end = b['info_settle_ts']
        elif b['planned_end_ts'] is not None:
            pk_end = b['planned_end_ts']
        else:
            pk_end = b['last_obs_ts']

        emit_pk = b['saw_active_battle'] or (
            b['planned_end_ts'] is not None and b['planned_end_ts'] > start_ts and pk_start < pk_end
        )
        if emit_pk:
            clamped_pk = _clamp_session_interval(pk_start, max(pk_start, pk_end), start_ts, cutoff_ts)
            if clamped_pk is not None:
                ref_end = b['planned_end_ts'] or clamped_pk[1]
                ref_start = eff_b_start or clamped_pk[0]
                sprint_start = (
                    max(ref_start, ref_end - b['sprint_duration'])
                    if ref_end > ref_start
                    else None
                )
                pk_summary = {
                    'pre_start_ts': b['pre_ts'],
                    'battle_start_ts': eff_b_start,
                    'planned_end_ts': b['planned_end_ts'],
                    'sprint_duration': b['sprint_duration'],
                    'sprint_start_ts': sprint_start,
                    'self_votes': b['self_votes'],
                    'opponent_votes': b['opponent_votes'],
                    'self_winner': b['self_winner'],
                    'muti_pk_type': b['muti_pk_type'],
                    'opponents': list(b['opponents'].values()),
                    'commands_seen': b['commands_seen'],
                }
                intervals.append({
                    'room_id': rid,
                    'kind': 'pk',
                    'start_ts': clamped_pk[0],
                    'end_ts': clamped_pk[1],
                    'battle_id': b['battle_id'],
                    'opponent_uid': b['opponent_uid'],
                    'opponent_room_id': b['opponent_room_id'],
                    'opponent_uname': b['opponent_uname'],
                    'raw_summary_json': json.dumps(pk_summary, ensure_ascii=False, sort_keys=True),
                })

        if b['punish_seen']:
            p_start = b['punish_start_ts'] or pk_end
            if b['actual_punish_end_ts'] is not None and b['actual_punish_end_ts'] >= p_start:
                p_end = b['actual_punish_end_ts']
            elif b['planned_punish_end_ts'] is not None and b['planned_punish_end_ts'] >= p_start:
                p_end = b['planned_punish_end_ts']
            else:
                p_end = max(p_start, b['last_obs_ts'])
            clamped_punish = _clamp_session_interval(p_start, p_end, start_ts, cutoff_ts)
            if clamped_punish is not None:
                punish_summary = {
                    'punish_text': b['punish_text'],
                    'planned_punish_end_ts': b['planned_punish_end_ts'],
                    'self_votes': b['self_votes'],
                    'opponent_votes': b['opponent_votes'],
                    'self_winner': b['self_winner'],
                    'opponents': list(b['opponents'].values()),
                    'commands_seen': b['commands_seen'],
                }
                intervals.append({
                    'room_id': rid,
                    'kind': 'pk_punish',
                    'start_ts': clamped_punish[0],
                    'end_ts': clamped_punish[1],
                    'battle_id': b['battle_id'],
                    'opponent_uid': b['opponent_uid'],
                    'opponent_room_id': b['opponent_room_id'],
                    'opponent_uname': b['opponent_uname'],
                    'raw_summary_json': json.dumps(punish_summary, ensure_ascii=False, sort_keys=True),
                })

    kind_rank = {'pk': 0, 'pk_punish': 1, 'voice_join_queue': 2, 'voice_join': 3}
    intervals.sort(key=lambda item: (item['start_ts'], item['end_ts'], kind_rank.get(item['kind'], 9), item['battle_id'] or ''))
    return intervals
