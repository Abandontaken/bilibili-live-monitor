"""跨房全局身份消解数仓与本地匿名看板差量回填的离线单元与集成测试（全部使用虚构房间号与虚构身份）。"""
from __future__ import annotations

import base64
import fcntl
import hashlib
import json
import sqlite3
import zlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from bilibili_live_monitor.__main__ import main as cli_main
from bilibili_live_monitor.live_share import atomic_json, fingerprint, iso
from bilibili_live_monitor.live_share_control import (
    export_snapshot,
    initialize,
    load_config,
    verify_snapshot,
)
from bilibili_live_monitor.warehouse import sync_warehouse, warehouse_stats
from bilibili_live_monitor.warehouse_backfill import backfill_local_pages
from bilibili_live_monitor.warehouse_protocol import (
    WareGift,
    WareInteract,
    WareRank,
    extract_face_sha1,
    uid_crc32,
    uid_md5,
)


def _find_colliding_crc32_pair() -> tuple[int, int]:
    """用生日攻击确定性找出一对真实 zlib.crc32 相等的正整数虚构 UID（约数万次迭代，<50ms）。"""
    seen: dict[int, int] = {}
    uid = 10_000_001
    while True:
        c = zlib.crc32(str(uid).encode())
        if c in seen:
            return seen[c], uid
        seen[c] = uid
        uid += 1


COLLIDING_UID_A, COLLIDING_UID_B = _find_colliding_crc32_pair()


def _pb_gift_packet(
    *,
    uid: int = 0,
    uname: str = '星***',
    face: str = 'https://i0.hdslb.com/bfs/face/member/noface.jpg',
    combo_seg: str | None = None,
    gift_id: int = 31039,
    gift_name: str = '牛哇牛哇',
    num: int = 1,
    price: int = 100,
    total_coin: int = 100,
    coin_type: str = 'gold',
    tid: str = '1000000000000000001',
    timestamp: int = 1790000020,
    blind_orig_name: str | None = None,
    blind_orig_price: int | None = None,
    is_mystery: bool = False,
) -> dict:
    msg = WareGift()
    msg.uid = uid
    msg.uname = uname
    msg.face = face
    msg.is_mystery = is_mystery
    msg.sender.uid = uid
    msg.sender.base.name = uname
    msg.sender.base.face = face
    msg.sender.base.is_mystery = is_mystery
    if blind_orig_name is not None:
        msg.blind_gift.original_gift_id = 32000
        msg.blind_gift.original_gift_name = blind_orig_name
        if blind_orig_price is not None:
            msg.blind_gift.original_gift_price = blind_orig_price
    item = msg.items.add()
    item.gift_id = gift_id
    item.gift_name = gift_name
    item.num = num
    item.price = price
    item.total_coin = total_coin
    item.coin_type = coin_type
    item.tid = tid
    item.timestamp = timestamp
    if combo_seg is not None:
        item.batch_combo_id = f'batch:gift:combo_id:{combo_seg}:999999:{gift_id}:{timestamp}'
    encoded = base64.b64encode(msg.SerializeToString()).decode()
    return {'cmd': 'SEND_GIFT_V2', 'data': {'pb': encoded}}


def _pb_rank_packet(entries: list[tuple[int, str, str, int, int]]) -> dict:
    """entries: [(uid, uname, face, score, wealth_level), ...]"""
    msg = WareRank()
    msg.rank_type = 'gold-rank'
    for idx, (uid, uname, face, score, wealth) in enumerate(entries, start=1):
        row = msg.online_list.add()
        row.uid = uid
        row.uname = uname
        row.face = face
        row.score = str(score)
        row.rank = idx
        row.uinfo.uid = uid
        row.uinfo.base.name = uname
        row.uinfo.base.face = face
        row.uinfo.wealth.level = wealth
    encoded = base64.b64encode(msg.SerializeToString()).decode()
    return {'cmd': 'ONLINE_RANK_V3', 'data': {'pb': encoded}}


def _pb_interact_packet(
    uid: int,
    uname: str,
    ts: int,
    *,
    wealth: int = 0,
    medal_ruid: int = 0,
    medal_room: int = 0,
    medal_name: str = '',
    medal_level: int = 0,
) -> dict:
    msg = WareInteract()
    msg.uid = uid
    msg.username = uname
    msg.msg_type = 1
    msg.timestamp = ts
    msg.uinfo.uid = uid
    msg.uinfo.base.name = uname
    msg.uinfo.wealth.level = wealth
    if medal_level > 0:
        msg.fans_medal.target_id = medal_ruid
        msg.fans_medal.anchor_roomid = medal_room
        msg.fans_medal.medal_name = medal_name
        msg.fans_medal.medal_level = medal_level
        msg.uinfo.medal.ruid = medal_ruid
        msg.uinfo.medal.name = medal_name
        msg.uinfo.medal.level = medal_level
    return {'cmd': 'INTERACT_WORD_V2', 'data': {'pb': base64.b64encode(msg.SerializeToString()).decode()}}


def _danmaku_packet(
    text: str,
    ts_sec: int,
    *,
    uid: int = 0,
    uname: str = '星***',
    crc32_hex: str = '00000000',
    wealth_level: int = 0,
    guard_level: int = 0,
    medal: list | None = None,
    face_url: str | None = None,
    u15_mode: str = 'dict',
    is_mystery: bool = False,
) -> dict:
    info = [None] * 17
    info[0] = [0, 1, 25, 16777215, ts_sec * 1000, 0, 0, crc32_hex]
    if face_url is not None or is_mystery:
        info[0].extend([None] * 7)
        user_obj = {
            'uid': uid,
            'base': {'name': uname, 'face': face_url or '', 'is_mystery': is_mystery},
            'wealth': {'level': wealth_level},
        }
        if u15_mode == 'json_str':
            info[0].append(json.dumps({'user': user_obj}, ensure_ascii=False))
        elif u15_mode == 'extra_json':
            info[0].append({'extra': json.dumps({'user': user_obj}, ensure_ascii=False)})
        else:
            info[0].append({'user': user_obj, 'extra': '{}'})
    info[1] = text
    info[2] = [uid, uname, 0, 0, 0, 10000, 1, '']
    info[3] = medal or []
    info[4] = [20, 0, 0, '', 0]
    info[5] = ['', '']
    info[7] = guard_level
    info[16] = [wealth_level] if wealth_level > 0 else []
    return {'cmd': 'DANMU_MSG', 'info': info}


def _create_source_db(path: Path) -> None:
    with sqlite3.connect(path) as db:
        db.executescript(
            """
            CREATE TABLE rooms(room_id TEXT PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE raw_events(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                room_id TEXT NOT NULL,
                session_id TEXT,
                received_utc TEXT NOT NULL,
                command TEXT,
                data TEXT NOT NULL
            );
            CREATE TABLE guard_snapshots(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                room_id TEXT NOT NULL,
                observed_utc TEXT NOT NULL,
                data TEXT NOT NULL
            );
            CREATE TABLE segments(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                room_id TEXT NOT NULL,
                seq INTEGER NOT NULL,
                data TEXT NOT NULL,
                UNIQUE(room_id, seq)
            );
            CREATE TABLE timeline(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                room_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                source_key TEXT NOT NULL,
                at_utc TEXT NOT NULL,
                data TEXT NOT NULL,
                UNIQUE(room_id, kind, source_key)
            );
            """
        )


def _insert_raw(db: sqlite3.Connection, room_id: str, ts: float, packet: dict) -> int:
    cur = db.execute(
        'INSERT INTO raw_events(room_id, session_id, received_utc, command, data) VALUES (?, ?, ?, ?, ?)',
        (room_id, 'sess1', iso(ts), packet['cmd'], json.dumps(packet, ensure_ascii=False)),
    )
    return cur.lastrowid


def _insert_timeline_gift(
    db: sqlite3.Connection,
    room_id: str,
    ts: float,
    *,
    author: str,
    event_type: str = 'SEND_GIFT_V2',
    event_id: str | None = None,
    gift_id: str = '31039',
    gift_name: str = '牛哇牛哇',
    num: int = 1,
    price: int = 100,
    total_coin: int = 100,
    coin_type: str = 'gold',
    sender_uid: str | None = None,
    blind_gift: dict | None = None,
    guard_level: int | None = None,
    guard_price_gold: int | None = None,
    anonymous_mode: bool = True,
) -> None:
    gift_obj: dict = {
        'event_type': event_type,
        'sender_uid': sender_uid,
        'time_source': 'event',
        'event_at_utc': iso(ts),
        'event_id': event_id,
        'gift_id': gift_id,
        'gift_name': gift_name,
        'num': num,
        'coin_type': coin_type,
        'price': price,
        'total_coin': total_coin,
    }
    if blind_gift is not None:
        gift_obj['blind_gift'] = blind_gift
    if guard_level is not None:
        gift_obj['guard_level'] = guard_level
    if guard_price_gold is not None:
        gift_obj['guard_price_gold'] = guard_price_gold
    payload: dict = {
        'room_id': room_id,
        'kind': 'gift',
        'author': author,
        'text': f'赠送 {gift_name}',
        'at_utc': iso(ts),
        'received_utc': iso(ts),
        'source_key': f'{event_type}:{event_id or ts}',
        'sender_uid': sender_uid,
        'identity_status': 'uid' if sender_uid else 'unresolved',
        'sender_hidden': False,
        'gift': gift_obj,
    }
    if anonymous_mode:
        payload['analysis_identity_basis'] = 'nickname'
    db.execute(
        'INSERT INTO timeline(room_id, kind, source_key, at_utc, data) VALUES (?, ?, ?, ?, ?)',
        (room_id, 'gift', payload['source_key'], iso(ts), json.dumps(payload, ensure_ascii=False)),
    )


def _create_completed_session(
    session_root: Path,
    key: str,
    db_path: Path,
    room_id: str,
    start_ts: float,
    cutoff_ts: float,
    *,
    local_only: bool = True,
    stage: str = 'completed',
    song_title: str = '测试晴天',
    segments: list[tuple[int, float, float, str]] | None = None,
) -> Path:
    workspace = session_root / key
    args = SimpleNamespace(
        workspace=workspace,
        database=db_path,
        room=room_id,
        label=f'虚构主播{room_id}',
        start=iso(start_ts),
        hours=4,
        api='http://127.0.0.1:8765',
        launcher='http://127.0.0.1:7997',
        ai_session=None,
        model=None,
        opencode_cwd=None,
        opencode_python=None,
        remote='host:/srv/example/' if not local_only else None,
        url='https://example.com/live/' if not local_only else None,
        blind_gift_name=[],
        no_blind_gifts=False,
        include_guard_lamps=False,
    )
    initialize(args)
    cfg = json.loads((workspace / 'config.json').read_text())
    cfg['local_only'] = local_only
    cfg['workflow_key'] = key
    cfg['approved_songs'] = [song_title]
    atomic_json(workspace / 'config.json', cfg)

    reviews = []
    if segments:
        with sqlite3.connect(db_path) as db:
            for seq, s_ts, e_ts, text in segments:
                seg_obj = {
                    'room_id': room_id,
                    'seq': seq,
                    'start_utc': iso(s_ts),
                    'end_utc': iso(e_ts),
                    'text': text,
                    'is_final': True,
                    'status': 'done',
                }
                db.execute(
                    'INSERT OR REPLACE INTO segments(room_id, seq, data) VALUES (?, ?, ?)',
                    (room_id, seq, json.dumps(seg_obj, ensure_ascii=False)),
                )
                reviews.append({
                    'seq': seq,
                    'fingerprint': fingerprint(seg_obj),
                    'label': 'singing',
                    'music_source': 'probable_live',
                    'song': song_title,
                    'confidence': 'high',
                    'investigation_version': 2,
                })
    atomic_json(
        workspace / 'annotations.json',
        {'reviews': reviews, 'completed_followups': [], 'song_requests': []},
    )
    atomic_json(workspace / 'state.json', {'phase': 'completed', 'stop_intent': True, 'cutoff': cutoff_ts})
    export_snapshot(load_config(workspace / 'config.json'))
    artifacts = {
        name: hashlib.sha256((workspace / name).read_bytes()).hexdigest()
        for name in ('public/index.html', 'public/snapshot.json', 'annotations.json', 'verified_songs.json')
        if (workspace / name).is_file()
    }
    atomic_json(workspace / 'workflow.json', {'stage': stage, 'cutoff': cutoff_ts, 'final_artifacts': artifacts})
    return workspace


def test_face_sha1_strict_filtering():
    valid_hash = 'a' * 40
    assert extract_face_sha1(f'https://i1.hdslb.com/bfs/face/{valid_hash}.jpg') == valid_hash
    assert extract_face_sha1(f'https://i2.hdslb.com/bfs/face/{valid_hash.upper()}.webp') == valid_hash
    assert extract_face_sha1('https://i0.hdslb.com/bfs/face/member/noface.jpg') is None
    assert extract_face_sha1(f'https://i0.hdslb.com/bfs/face/noface_{valid_hash}.jpg') is None
    assert extract_face_sha1(f'https://i0.hdslb.com/bfs/garb/{valid_hash}.png') is None
    assert extract_face_sha1(f'https://i0.hdslb.com/bfs/garb/face/{valid_hash}.jpg') is None


def test_cross_room_delayed_reveal_unlocks_historical_gifts_and_danmaku(tmp_path: Path):
    """周一在房间123456匿名送礼+发弹幕，周四在房间654321点赞露真身，自动双向认领旧场礼物与弹幕。"""
    db_path = tmp_path / 'anon_monitor.sqlite3'
    wh_path = tmp_path / 'warehouse.sqlite3'
    sessions_dir = tmp_path / 'sessions'
    sessions_dir.mkdir()
    _create_source_db(db_path)

    monday_start = 1790000000.0
    monday_cutoff = monday_start + 600.0
    target_uid = 880001
    unique_face = '1' * 40
    shared_face = '2' * 40
    garb_face = '3' * 40

    with sqlite3.connect(db_path) as db:
        # 周一在 123456：匿名发2条弹幕 + 1笔连击礼物(MD5) + 1笔单抽盲盒(唯一头像) + COMBO_SEND(不重复计费)
        _insert_raw(
            db, '123456', monday_start + 10,
            _danmaku_packet('点一首晴天', int(monday_start + 10), uname='星***',
                            crc32_hex=uid_crc32(target_uid), wealth_level=42,
                            medal=[25, '外房牌', '虚构外房主播', 999999, 0, '', 0, 0, 0, 0, 3, 1, 777777]),
        )
        _insert_raw(
            db, '123456', monday_start + 15,
            _danmaku_packet('唱得真好', int(monday_start + 15), uname='星***',
                            crc32_hex=uid_crc32(target_uid), wealth_level=42),
        )
        _insert_raw(
            db, '123456', monday_start + 30,
            _pb_gift_packet(
                uid=0, uname='星***', combo_seg=uid_md5(target_uid),
                gift_id=31001, gift_name='小花花', num=2, price=1000, total_coin=2000,
                tid='tid_combo_1', timestamp=int(monday_start + 30),
            ),
        )
        # 同批次 COMBO_SEND 绝不能重复计入礼物件数或金额
        _insert_raw(
            db, '123456', monday_start + 31,
            {
                'cmd': 'COMBO_SEND',
                'data': {
                    'batch_combo_id': f'batch:gift:combo_id:{uid_md5(target_uid)}:999999:31001:{int(monday_start + 30)}',
                    'combo_num': 2,
                    'combo_total_coin': 2000,
                    'coin_type': 'gold',
                    'gift_id': 31001,
                    'gift_name': '小花花',
                },
            },
        )
        # 单发盲盒：无 batch_combo_id，仅靠自定义头像认领；实付 15000 (15元)，开出面值 52000 (52元)
        _insert_raw(
            db, '123456', monday_start + 50,
            _pb_gift_packet(
                uid=0, uname='星***',
                face=f'https://i1.hdslb.com/bfs/face/{unique_face}.jpg',
                combo_seg=None, gift_id=32001, gift_name='浪漫城堡', num=1,
                price=52000, total_coin=15000,
                blind_orig_name='心动盲盒', blind_orig_price=15000,
                tid='tid_blind_1', timestamp=int(monday_start + 50),
            ),
        )
        # 共享头像测试：两个无关观众 880010 与 880011 共用 shared_face，外加 /bfs/garb/ 装扮图
        _insert_raw(
            db, '123456', monday_start + 60,
            _pb_rank_packet([
                (880010, '甲观众', f'https://i0.hdslb.com/bfs/face/{shared_face}.jpg', 10, 5),
                (880011, '乙观众', f'https://i0.hdslb.com/bfs/face/{shared_face}.jpg', 10, 8),
                (880012, '装扮观众', f'https://i0.hdslb.com/bfs/garb/{garb_face}.png', 10, 12),
            ]),
        )
        # 一笔无 MD5、仅带共享头像的单发盲盒，必须熔断不认领
        _insert_raw(
            db, '123456', monday_start + 70,
            _pb_gift_packet(
                uid=0, uname='甲***',
                face=f'https://i0.hdslb.com/bfs/face/{shared_face}.jpg',
                combo_seg=None, gift_id=32002, gift_name='共享图礼', num=1,
                price=3000, total_coin=3000, tid='tid_shared_face', timestamp=int(monday_start + 70),
            ),
        )

    _create_completed_session(
        sessions_dir, 'sess_monday_123456', db_path, '123456', monday_start, monday_cutoff,
        segments=[(1, monday_start + 5, monday_start + 100, '原转写词句')],
    )

    # 第一次同步（周一晚）：880001 尚未露真身
    src_sha_before = hashlib.sha256(db_path.read_bytes()).hexdigest()
    res1 = sync_warehouse(wh_path, sources=[('anon', db_path)], session_roots=[sessions_dir])
    assert hashlib.sha256(db_path.read_bytes()).hexdigest() == src_sha_before
    assert res1['sessions']['ingested'] == 1

    with sqlite3.connect(wh_path) as wh:
        # COMBO_SEND 不重复计费：总共只有 3 笔原子礼物（小花花 + 浪漫城堡盲盒 + 共享图礼）
        gifts_mon = wh.execute(
            "SELECT gift_name, paid_milli_yuan, output_milli_yuan, resolved_uid, actor_key, resolution_basis "
            "FROM v_resolved_events WHERE kind = 'gift' ORDER BY event_seq"
        ).fetchall()
        assert len(gifts_mon) == 3
        assert gifts_mon[0][:3] == ('小花花', 2000, 2000)
        assert gifts_mon[0][3] is None and gifts_mon[0][4] == f'md5:{uid_md5(target_uid)}'
        assert gifts_mon[1][:3] == ('浪漫城堡', 15000, 52000)
        assert gifts_mon[1][3] is None
        assert gifts_mon[2][0] == '共享图礼' and gifts_mon[2][3] is None  # 共享头像被熔断
        dm_resolved = wh.execute(
            "SELECT resolved_uid FROM v_resolved_events WHERE kind = 'danmaku'"
        ).fetchall()
        assert all(r[0] is None for r in dm_resolved)
        # 观测到外房粉丝牌 allows_ext_medal == 1
        ext_flag = wh.execute("SELECT allows_ext_medal FROM dim_rooms WHERE room_id = '123456'").fetchone()[0]
        assert ext_flag == 1

    # 周四：观众 880001 在另一房间 654321 点赞露真身
    thursday_ts = monday_start + 3 * 86400
    with sqlite3.connect(db_path) as db:
        _insert_raw(
            db, '654321', thursday_ts,
            {
                'cmd': 'LIKE_INFO_V3_CLICK',
                'data': {
                    'uid': target_uid,
                    'uname': '星海听歌人',
                    'is_mystery': False,
                    'uinfo': {
                        'uid': target_uid,
                        'base': {
                            'name': '星海听歌人',
                            'face': f'https://i1.hdslb.com/bfs/face/{unique_face}.jpg',
                            'is_mystery': False,
                        },
                        'wealth': {'level': 42},
                    },
                },
            },
        )

    # 第二次增量同步：不重扫周一已入库场次，仅增量扫新增 raw_events
    res2 = sync_warehouse(wh_path, sources=[('anon', db_path)], session_roots=[sessions_dir])
    assert res2['sessions']['ingested'] == 0
    assert res2['sessions']['skipped_already_ingested'] == 1

    with sqlite3.connect(wh_path) as wh:
        gifts_after = wh.execute(
            "SELECT gift_name, resolved_uid, resolution_basis FROM v_resolved_events "
            "WHERE kind = 'gift' ORDER BY event_seq"
        ).fetchall()
        assert gifts_after[0] == ('小花花', target_uid, 'global_md5')
        assert gifts_after[1] == ('浪漫城堡', target_uid, 'unique_face')
        assert gifts_after[2] == ('共享图礼', None, 'unresolved')  # 共享头像依然严格隔离

        dms_after = wh.execute(
            "SELECT resolved_uid, resolution_basis FROM v_resolved_events WHERE kind = 'danmaku'"
        ).fetchall()
        assert dms_after == [(target_uid, 'crc32_first_char'), (target_uid, 'crc32_first_char')]

        # fact_session_presence 自动将周一 880001 的弹幕与两笔礼物归并为唯一一行
        pres = wh.execute(
            "SELECT actor_key, danmaku_count, gift_event_count, paid_milli_yuan, output_milli_yuan, max_wealth_level "
            "FROM fact_session_presence WHERE resolved_uid = ?",
            (target_uid,),
        ).fetchone()
        assert pres == (f'uid:{target_uid}', 2, 2, 17000, 54000, 42)


def test_crc32_collision_protection_and_same_room_disambiguation(tmp_path: Path):
    """真实 CRC32 碰撞双用户保护：无同房共现标 ambiguous_crc32，唯一同房共现精准消歧。"""
    db_path = tmp_path / 'crc_monitor.sqlite3'
    wh_path = tmp_path / 'crc_wh.sqlite3'
    sessions_dir = tmp_path / 'sessions'
    sessions_dir.mkdir()
    _create_source_db(db_path)

    assert uid_crc32(COLLIDING_UID_A) == uid_crc32(COLLIDING_UID_B)
    shared_crc = uid_crc32(COLLIDING_UID_A)
    start_ts = 1791000000.0
    cutoff_ts = start_ts + 300.0

    with sqlite3.connect(db_path) as db:
        # 两个碰撞 UID 在其他房间 999888 均拥有相同首字“林”
        _insert_raw(
            db, '999888', start_ts - 100,
            _pb_rank_packet([
                (COLLIDING_UID_A, '林间清风', 'https://i0.hdslb.com/bfs/face/member/noface.jpg', 10, 20),
                (COLLIDING_UID_B, '林下听雨', 'https://i0.hdslb.com/bfs/face/member/noface.jpg', 20, 25),
            ]),
        )
        # 在目标房间 234567：一条“林***”弹幕（与两人同时匹配），一条“陈***”弹幕（首字不匹配）
        _insert_raw(
            db, '234567', start_ts + 20,
            _danmaku_packet('林字碰撞弹幕', int(start_ts + 20), uname='林***', crc32_hex=shared_crc, wealth_level=20),
        )
        _insert_raw(
            db, '234567', start_ts + 25,
            _danmaku_packet('首字不符弹幕', int(start_ts + 25), uname='陈***', crc32_hex=shared_crc, wealth_level=20),
        )

    _create_completed_session(sessions_dir, 'sess_crc_1', db_path, '234567', start_ts, cutoff_ts)
    sync_warehouse(wh_path, sources=[('s1', db_path)], session_roots=[sessions_dir])

    with sqlite3.connect(wh_path) as wh:
        rows = wh.execute(
            "SELECT content_text, resolved_uid, crc32_status, resolution_basis "
            "FROM v_resolved_events WHERE kind = 'danmaku' ORDER BY event_seq"
        ).fetchall()
        # 两人均未在 234567 露面 -> ambiguous_crc32 置空保护；首字不符 -> unresolved
        assert rows[0] == ('林字碰撞弹幕', None, 'ambiguous_crc32', 'ambiguous_crc32')
        assert rows[1] == ('首字不符弹幕', None, None, 'unresolved')

    # 随后 COLLIDING_UID_B 在另一场 234567 房间进场露面 -> 同房唯一共现解锁
    s2_start = start_ts + 1000.0
    s2_cutoff = s2_start + 200.0
    with sqlite3.connect(db_path) as db:
        _insert_raw(db, '234567', s2_start + 10, _pb_interact_packet(COLLIDING_UID_B, '林下听雨', int(s2_start + 10)))
    _create_completed_session(sessions_dir, 'sess_crc_2', db_path, '234567', s2_start, s2_cutoff)

    sync_warehouse(wh_path, sources=[('s1', db_path)], session_roots=[sessions_dir])
    with sqlite3.connect(wh_path) as wh:
        row_after = wh.execute(
            "SELECT resolved_uid, crc32_status, resolution_basis FROM v_resolved_events "
            "WHERE content_text = '林字碰撞弹幕'"
        ).fetchone()
        assert row_after == (COLLIDING_UID_B, 'resolved', 'crc32_first_char')


def test_rank_delta_inference_and_legacy_timeline_fallback_and_stats(tmp_path: Path):
    """覆盖 L5 rank_delta 精确跳变推断、实名 combo 数字段解析、无 raw_events 旧库回退与 stats 分档输出。"""
    legacy_db = tmp_path / 'legacy_monitor.sqlite3'
    modern_db = tmp_path / 'modern_monitor.sqlite3'
    wh_path = tmp_path / 'wh.sqlite3'
    sessions_dir = tmp_path / 'sessions'
    sessions_dir.mkdir()

    # 1. 无 raw_events 的旧版实名库（同时含顶层 sender_uid 与嵌套 gift.sender_uid）
    with sqlite3.connect(legacy_db) as db:
        db.executescript(
            """
            CREATE TABLE rooms(room_id TEXT PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE timeline(
                id INTEGER PRIMARY KEY AUTOINCREMENT, room_id TEXT NOT NULL,
                kind TEXT NOT NULL, source_key TEXT NOT NULL, at_utc TEXT NOT NULL, data TEXT NOT NULL
            );
            """
        )
        db.execute(
            'INSERT INTO timeline(room_id, kind, source_key, at_utc, data) VALUES (?, ?, ?, ?, ?)',
            ('111111', 'danmaku', 'k1', '2026-10-25T10:00:00+00:00',
             json.dumps({'sender_uid': '770001', 'author': '旧库弹幕观众', 'text': '你好'})),
        )
        db.execute(
            'INSERT INTO timeline(room_id, kind, source_key, at_utc, data) VALUES (?, ?, ?, ?, ?)',
            ('111111', 'gift', 'k2', '2026-10-25T10:01:00+00:00',
             json.dumps({'author': '旧库送礼观众', 'gift': {'sender_uid': '770002', 'gift_name': '辣条'}})),
        )

    # 2. 含 rank_delta、GUARD_BUY+USER_TOAST_MSG_V2 去重与大额 SC 的现代库
    _create_source_db(modern_db)
    start_ts = 1792000000.0
    cutoff_ts = start_ts + 300.0
    rank_uid = 770003
    realname_combo_uid = 770004
    guard_uid = 770005

    with sqlite3.connect(modern_db) as db:
        _insert_raw(db, '123456', start_ts + 5, _pb_rank_packet([(rank_uid, '灰头榜一大哥', '', 100, 35)]))
        # 无 MD5、无自定义头像的单笔金瓜子礼（5200 千分元 = 52 贡献值）
        _insert_raw(
            db, '123456', start_ts + 10,
            _pb_gift_packet(
                uid=0, uname='灰***', combo_seg=None, gift_id=31099, gift_name='比心',
                num=1, price=5200, total_coin=5200, tid='tid_rank_delta', timestamp=int(start_ts + 10),
            ),
        )
        # 8 秒后榜单唯一 1 人从 100 -> 152 (+52)
        _insert_raw(db, '123456', start_ts + 18, _pb_rank_packet([(rank_uid, '灰头榜一大哥', '', 152, 35)]))
        # 实名态 combo_id 第4段为纯数字 UID
        _insert_raw(
            db, '123456', start_ts + 30,
            _pb_gift_packet(
                uid=0, uname='实名连击哥', combo_seg=str(realname_combo_uid),
                gift_id=31088, gift_name='摩天大楼', num=1, price=250000, total_coin=250000,
                tid='tid_real_combo', timestamp=int(start_ts + 30),
            ),
        )
        # 同笔舰长同时下发 GUARD_BUY (面值198000) 与 USER_TOAST_MSG_V2 (续费实付138000)，不重复计费
        _insert_raw(
            db, '123456', start_ts + 50,
            {
                'cmd': 'GUARD_BUY',
                'data': {
                    'uid': guard_uid, 'username': '续舰老粉', 'guard_level': 3,
                    'num': 1, 'price': 198000, 'gift_id': 10003, 'gift_name': '舰长',
                    'start_time': int(start_ts + 50),
                },
            },
        )
        _insert_raw(
            db, '123456', start_ts + 50,
            {
                'cmd': 'USER_TOAST_MSG_V2',
                'data': {
                    'sender_uinfo': {'uid': guard_uid, 'base': {'name': '续舰老粉', 'face': ''}},
                    'guard_info': {'guard_level': 3, 'start_time': int(start_ts + 50), 'role_name': '舰长'},
                    'pay_info': {'payflow_id': 'pf_1', 'price': 138000, 'num': 1},
                },
            },
        )

    _create_completed_session(sessions_dir, 'sess_modern', modern_db, '123456', start_ts, cutoff_ts)

    rc = cli_main_helper([
        'warehouse', 'sync',
        '--warehouse', str(wh_path),
        '--source', f'legacy={legacy_db}',
        '--source', f'modern={modern_db}',
        '--session-root', str(sessions_dir),
    ])
    assert rc == 0

    stats = warehouse_stats(wh_path)
    assert stats['known_uids'] == 5  # 770001..770005
    assert stats['ingested_sessions'] == 1
    # <10元: 5.2元(rank_delta); 10-200元: 138元(续舰); >=200元: 250元(摩天大楼) -> 全部100%解出
    assert stats['paid_gifts']['<10元']['paid_yuan'] == 5.2
    assert stats['paid_gifts']['<10元']['event_resolution_rate'] == 1.0
    assert stats['paid_gifts']['10-200元']['paid_yuan'] == 138.0
    assert stats['paid_gifts']['>=200元']['paid_yuan'] == 250.0
    assert stats['paid_gifts']['total']['events'] == 3
    assert stats['paid_gift_resolution_basis']['rank_delta'] == 1

    # 测试 --reingest 单场幂等重入
    re_res = sync_warehouse(wh_path, sources=[('modern', modern_db)],
                            session_roots=[sessions_dir], reingest={'sess_modern'})
    assert re_res['sessions']['ingested'] == 1
    assert warehouse_stats(wh_path)['paid_gifts']['total']['events'] == 3


def test_rank_delta_rejects_concurrent_gifts_and_full_board_newcomers(tmp_path: Path):
    """L5 保守护栏：同时间窗存在其他金瓜子礼物或满50人榜新挤入第50名时绝不强猜。"""
    db_path = tmp_path / 'compete_monitor.sqlite3'
    wh_path = tmp_path / 'compete_wh.sqlite3'
    sessions_dir = tmp_path / 'sessions'
    sessions_dir.mkdir()
    _create_source_db(db_path)

    start_ts = 1794000000.0
    cutoff_ts = start_ts + 300.0

    with sqlite3.connect(db_path) as db:
        # 场景1：同秒两笔相同金额的无MD5灰头礼物 -> 必须拒绝推断
        _insert_raw(db, '123456', start_ts + 5, _pb_rank_packet([(660001, '观众一', '', 10, 10)]))
        _insert_raw(
            db, '123456', start_ts + 10,
            _pb_gift_packet(uid=0, uname='甲***', combo_seg=None, price=2000, total_coin=2000,
                            tid='tid_comp_1', timestamp=int(start_ts + 10)),
        )
        _insert_raw(
            db, '123456', start_ts + 10,
            _pb_gift_packet(uid=0, uname='乙***', combo_seg=None, price=2000, total_coin=2000,
                            tid='tid_comp_2', timestamp=int(start_ts + 10)),
        )
        _insert_raw(db, '123456', start_ts + 15, _pb_rank_packet([(660001, '观众一', '', 30, 10)]))

        # 场景2：满50人榜（底分10），新挤入第50名 score=15 可能是旧底分9+增量6，不能认领 1500 金瓜子礼
        full_50 = [(650000 + i, f'榜众{i}', '', 100 - i, 10) for i in range(50)]
        _insert_raw(db, '123456', start_ts + 100, _pb_rank_packet(full_50))
        _insert_raw(
            db, '123456', start_ts + 105,
            _pb_gift_packet(uid=0, uname='丙***', combo_seg=None, price=6000, total_coin=6000,
                            tid='tid_full_board', timestamp=int(start_ts + 105)),
        )
        next_50 = [*full_50[:-1], (669999, '新上榜者', '', 60, 15)]
        _insert_raw(db, '123456', start_ts + 110, _pb_rank_packet(next_50))

    _create_completed_session(sessions_dir, 'sess_compete', db_path, '123456', start_ts, cutoff_ts)
    sync_warehouse(wh_path, sources=[('m', db_path)], session_roots=[sessions_dir])

    with sqlite3.connect(wh_path) as wh:
        inferred = wh.execute(
            "SELECT inferred_uid FROM fact_events WHERE kind = 'gift'"
        ).fetchall()
        assert all(row[0] is None for row in inferred)


def test_backfill_pages_five_hard_boundaries_and_rollback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """完整验证 backfill-pages 的五条硬边界、不同小***按MD5拆行、同人舰长+盲盒合并、followups冻结与失败回滚。"""
    db_path = tmp_path / 'backfill_monitor.sqlite3'
    wh_path = tmp_path / 'backfill_wh.sqlite3'
    sessions_dir = tmp_path / 'sessions'
    sessions_dir.mkdir()
    _create_source_db(db_path)

    start_ts = 1793000000.0
    cutoff_ts = start_ts + 300.0

    # 人物设定（全部虚构）：
    # 1) 同人合并：uid=990001 (最新昵称 "小王今天不熬夜") 买了 198 元舰长(明文) + 抽了盲盒(打码 "小***", md5=md5(990001))
    # 2) 不同小***拆行：未露真身的老板甲 (md5="a1b2" + "0"*28) 与老板乙 (md5="c3d4" + "0"*28) 都叫 "小***"
    boss_uid = 990001
    md5_boss = uid_md5(boss_uid)
    md5_unknown_a = 'a1b2' + '0' * 28
    md5_unknown_b = 'c3d4' + '0' * 28

    with sqlite3.connect(db_path) as db:
        # 舰长 (明文)
        _insert_raw(
            db, '123456', start_ts + 20,
            {
                'cmd': 'GUARD_BUY',
                'data': {
                    'uid': boss_uid, 'username': '小王旧名', 'guard_level': 3,
                    'num': 1, 'price': 198000, 'gift_id': 10003, 'gift_name': '舰长',
                    'start_time': int(start_ts + 20),
                },
            },
        )
        _insert_timeline_gift(
            db, '123456', start_ts + 20,
            author='小王旧名', event_type='GUARD_BUY', event_id='guard_order_1',
            gift_id='10003', gift_name='舰长', num=1, price=198000, total_coin=198000,
            sender_uid=str(boss_uid), guard_level=3, guard_price_gold=198000,
        )
        # 同一位 990001 抽了打码盲盒（面值 100 元 = 100000）
        _insert_raw(
            db, '123456', start_ts + 30,
            _pb_gift_packet(
                uid=0, uname='小***', combo_seg=md5_boss,
                gift_id=32010, gift_name='星愿盲盒礼', num=1, price=100000, total_coin=15000,
                blind_orig_name='心动盲盒', blind_orig_price=15000,
                tid='tid_boss_blind', timestamp=int(start_ts + 30),
            ),
        )
        _insert_timeline_gift(
            db, '123456', start_ts + 30,
            author='小***', event_type='SEND_GIFT_V2', event_id='tid_boss_blind',
            gift_id='32010', gift_name='星愿盲盒礼', num=1, price=100000, total_coin=15000,
            blind_gift={'original_gift_price': 15000, 'original_gift_name': '心动盲盒'},
        )
        # 未解出真身的老板甲 "小***"（面值 50 元）
        _insert_raw(
            db, '123456', start_ts + 40,
            _pb_gift_packet(
                uid=0, uname='小***', combo_seg=md5_unknown_a,
                gift_id=31050, gift_name='礼花', num=1, price=50000, total_coin=50000,
                tid='tid_unk_a', timestamp=int(start_ts + 40),
            ),
        )
        _insert_timeline_gift(
            db, '123456', start_ts + 40,
            author='小***', event_type='SEND_GIFT_V2', event_id='tid_unk_a',
            gift_id='31050', gift_name='礼花', num=1, price=50000, total_coin=50000,
        )
        # 未解出真身的老板乙 "小***"（面值 30 元）
        _insert_raw(
            db, '123456', start_ts + 45,
            _pb_gift_packet(
                uid=0, uname='小***', combo_seg=md5_unknown_b,
                gift_id=31030, gift_name='粉丝票', num=1, price=30000, total_coin=30000,
                tid='tid_unk_b', timestamp=int(start_ts + 45),
            ),
        )
        _insert_timeline_gift(
            db, '123456', start_ts + 45,
            author='小***', event_type='SEND_GIFT_V2', event_id='tid_unk_b',
            gift_id='31030', gift_name='粉丝票', num=1, price=30000, total_coin=30000,
        )
        # 后来 990001 在别的房间更新了最新昵称 "小王今天不熬夜"
        _insert_raw(
            db, '654321', cutoff_ts + 100,
            _pb_interact_packet(boss_uid, '小王今天不熬夜', int(cutoff_ts + 100), wealth=30),
        )

    # 准备 4 个场次验证硬边界：
    # 1. 目标匿名本地已完成场次 (local_only=True, stage='completed')
    ws_target = _create_completed_session(
        sessions_dir, '01_target_local_completed', db_path, '123456', start_ts, cutoff_ts,
        local_only=True, stage='completed',
        segments=[(1, start_ts + 10, start_ts + 60, '唱一首测试晴天')],
    )
    # 2. 远端发布场次 (local_only=False) -> 必须跳过
    ws_remote = _create_completed_session(
        sessions_dir, '02_remote_session', db_path, '123456', start_ts, cutoff_ts,
        local_only=False, stage='completed',
    )
    # 3. 正在直播场次 (stage='live') -> 必须跳过
    ws_live = _create_completed_session(
        sessions_dir, '03_live_session', db_path, '123456', start_ts, cutoff_ts,
        local_only=True, stage='live',
    )
    # 4. 零变化场次 (无任何礼物的已完成本地场) -> 必须 unchanged 零写入
    ws_unchanged = _create_completed_session(
        sessions_dir, '04_empty_unchanged', db_path, '999000', start_ts, cutoff_ts,
        local_only=True, stage='completed',
        segments=[(1, start_ts + 10, start_ts + 60, '纯清唱无礼物')],
    )

    # 回填前：小*** 3笔（100+50+30=180元）被混成1行，小王旧名 198元单独1行，单曲去重人数只有 2 人，followups 为空 []
    snap_before = json.loads((ws_target / 'public/snapshot.json').read_text())
    assert snap_before['followups'] == []
    song_before = next(r for r in snap_before['activity_summary'] if r['kind'] == 'song')
    assert song_before['gift_users'] == 2
    target_snap_mtime_before = (ws_target / 'public/snapshot.json').stat().st_mtime_ns
    unchanged_mtime_before = (ws_unchanged / 'public/snapshot.json').stat().st_mtime_ns
    remote_bytes_before = (ws_remote / 'public/snapshot.json').read_bytes()
    live_bytes_before = (ws_live / 'public/snapshot.json').read_bytes()

    sync_warehouse(wh_path, sources=[('anon', db_path)], session_roots=[sessions_dir])

    # 硬边界验证：默认 --dry-run 不写任何文件，连 .lock 都不创建
    dry = backfill_local_pages(wh_path, [sessions_dir], execute=False)
    assert dry['mode'] == 'dry_run'
    assert dry['counts']['would_update'] == 1
    assert dry['counts']['unchanged'] == 1
    assert dry['counts']['skipped_not_local_only'] == 1
    assert dry['counts']['skipped_not_completed'] == 1
    assert not (ws_target / 'workflow.lock').exists()
    assert (ws_target / 'public/snapshot.json').stat().st_mtime_ns == target_snap_mtime_before

    # 硬边界验证：当场次锁被占用时非阻塞跳过 (skipped_locked)
    with (ws_target / 'workflow.lock').open('a') as held_lock:
        fcntl.flock(held_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        locked_res = backfill_local_pages(wh_path, [sessions_dir], execute=True)
        assert locked_res['counts']['skipped_locked'] == 1

    # 硬边界验证：校验失败自动回滚保护
    old_target_snap_bytes = (ws_target / 'public/snapshot.json').read_bytes()
    old_target_html_bytes = (ws_target / 'public/index.html').read_bytes()
    monkeypatch.setattr(
        'bilibili_live_monitor.warehouse_backfill.verify_snapshot',
        lambda cfg: {'valid': False, 'errors': ['synthetic_failure']},
    )
    with pytest.raises(ValueError, match='已自动回滚'):
        backfill_local_pages(wh_path, [sessions_dir], execute=True)
    assert (ws_target / 'public/snapshot.json').read_bytes() == old_target_snap_bytes
    assert (ws_target / 'public/index.html').read_bytes() == old_target_html_bytes
    assert not (ws_target / 'identity_backfill_history.jsonl').exists()
    monkeypatch.undo()

    # 正式 --execute 执行回填
    exe = backfill_local_pages(wh_path, [sessions_dir], execute=True)
    assert exe['mode'] == 'execute'
    assert exe['counts']['updated'] == 1
    assert exe['counts']['unchanged'] == 1
    assert exe['counts']['skipped_not_local_only'] == 1
    assert exe['counts']['skipped_not_completed'] == 1

    # 验证未触碰 skipped 与 unchanged 场次
    assert (ws_unchanged / 'public/snapshot.json').stat().st_mtime_ns == unchanged_mtime_before
    assert (ws_remote / 'public/snapshot.json').read_bytes() == remote_bytes_before
    assert (ws_live / 'public/snapshot.json').read_bytes() == live_bytes_before

    # 验证目标场次回填内容：
    # 1) 990001 的 198舰长 + 100盲盒合并为 "小王今天不熬夜" (298.0元)
    # 2) 两个未解名小*** 拆为 "小*** #a1b2" (50.0元) 与 "小*** #c3d4" (30.0元)
    # 3) 单曲去重送礼人数 gift_users 修正为 3 人
    # 4) followups 严格保持原封口的 [] 不变，绝不制造假 pending 待办！
    snap_after = json.loads((ws_target / 'public/snapshot.json').read_text())
    assert snap_after['followups'] == []
    assert [(r['name'], r['value_yuan']) for r in snap_after['gift_summary']] == [
        ('小王今天不熬夜', 298.0),
        ('小*** #a1b2', 50.0),
        ('小*** #c3d4', 30.0),
    ]
    song_after = next(r for r in snap_after['activity_summary'] if r['kind'] == 'song')
    assert song_after['gift_users'] == 3
    assert str(boss_uid) not in json.dumps(snap_after, ensure_ascii=False)

    # 原生 verify_snapshot 必须通过，且 workflow.json.final_artifacts 与审计日志同步更新
    v_report = verify_snapshot(load_config(ws_target / 'config.json'))
    assert v_report['valid'] is True, v_report
    wf_after = json.loads((ws_target / 'workflow.json').read_text())
    assert wf_after['final_artifacts']['public/snapshot.json'] == hashlib.sha256(
        (ws_target / 'public/snapshot.json').read_bytes()
    ).hexdigest()
    assert wf_after['final_artifacts']['public/index.html'] == hashlib.sha256(
        (ws_target / 'public/index.html').read_bytes()
    ).hexdigest()
    history_lines = (ws_target / 'identity_backfill_history.jsonl').read_text().strip().splitlines()
    assert len(history_lines) == 1
    audit = json.loads(history_lines[0])
    assert audit['before_sha256']['public/snapshot.json'] != audit['after_sha256']['public/snapshot.json']

    # 再次运行 --execute 必须 100% 命中 unchanged 幂等零写入
    snap_mtime_post = (ws_target / 'public/snapshot.json').stat().st_mtime_ns
    exe_again = backfill_local_pages(wh_path, [sessions_dir], execute=True)
    assert exe_again['counts']['updated'] == 0
    assert exe_again['counts']['unchanged'] == 2
    assert (ws_target / 'public/snapshot.json').stat().st_mtime_ns == snap_mtime_post
    assert len((ws_target / 'identity_backfill_history.jsonl').read_text().strip().splitlines()) == 1


def test_source_db_protection_and_exception_rollback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """验证源库同路径/软硬链接拒绝写入，以及校验器抛异常时完整回滚页面与审计文件。"""
    import os

    src_db = tmp_path / 'monitor.sqlite3'
    _create_source_db(src_db)
    src_sha = hashlib.sha256(src_db.read_bytes()).hexdigest()

    sym_alias = tmp_path / 'sym_warehouse.sqlite3'
    sym_alias.symlink_to(src_db)
    hard_alias = tmp_path / 'hard_warehouse.sqlite3'
    os.link(src_db, hard_alias)

    for bad_wh in (src_db, sym_alias, hard_alias):
        with pytest.raises(ValueError):
            sync_warehouse(bad_wh, sources=[('src', src_db)])
    # 即使未显式传同一 --source，直接把已有源库当数仓打开也必须拒绝且零改动
    other_src = tmp_path / 'other.sqlite3'
    _create_source_db(other_src)
    with pytest.raises(ValueError):
        sync_warehouse(src_db, sources=[('other', other_src)])
    assert hashlib.sha256(src_db.read_bytes()).hexdigest() == src_sha

    # 验证 verify_snapshot 抛异常（而非仅返回 valid=False）时同样完整回滚
    sessions_dir = tmp_path / 'sessions'
    sessions_dir.mkdir()
    start_ts = 1795000000.0
    cutoff_ts = start_ts + 120.0
    with sqlite3.connect(src_db) as db:
        _insert_raw(
            db, '123456', start_ts + 20,
            _pb_gift_packet(uid=0, uname='甲***', combo_seg='f1f2' + '0' * 28,
                            price=1000, total_coin=1000, tid='t_rb_1', timestamp=int(start_ts + 20)),
        )
        _insert_timeline_gift(
            db, '123456', start_ts + 20, author='甲***', event_id='t_rb_1', price=1000, total_coin=1000,
        )
    ws = _create_completed_session(sessions_dir, 'sess_rb', src_db, '123456', start_ts, cutoff_ts)
    wh_path = tmp_path / 'real_wh.sqlite3'
    sync_warehouse(wh_path, sources=[('s', src_db)])

    old_snap = (ws / 'public/snapshot.json').read_bytes()
    old_html = (ws / 'public/index.html').read_bytes()

    def boom(_cfg):
        raise RuntimeError('simulated verifier crash')

    monkeypatch.setattr('bilibili_live_monitor.warehouse_backfill.verify_snapshot', boom)
    with pytest.raises(ValueError, match='已自动回滚'):
        backfill_local_pages(wh_path, [sessions_dir], execute=True)
    assert (ws / 'public/snapshot.json').read_bytes() == old_snap
    assert (ws / 'public/index.html').read_bytes() == old_html
    assert not (ws / 'identity_backfill_history.jsonl').exists()


def test_legacy_session_ingest_uname_ts_order_and_missing_prev_rank(tmp_path: Path):
    """覆盖无 raw_events 旧库场次入库、latest_uname_ts 乱序保护、无前置榜单帧拒绝及 talk_finalize 文本。"""
    legacy_db = tmp_path / 'legacy_only_timeline.sqlite3'
    with sqlite3.connect(legacy_db) as db:
        db.executescript(
            """
            CREATE TABLE timeline(
                id INTEGER PRIMARY KEY AUTOINCREMENT, room_id TEXT NOT NULL,
                kind TEXT NOT NULL, source_key TEXT NOT NULL, at_utc TEXT NOT NULL, data TEXT NOT NULL
            );
            """
        )
        start_ts = 1796000000.0
        cutoff_ts = start_ts + 120.0
        db.execute(
            'INSERT INTO timeline(room_id, kind, source_key, at_utc, data) VALUES (?, ?, ?, ?, ?)',
            ('123456', 'gift', 'g1', iso(start_ts + 10), json.dumps({
                'room_id': '123456', 'kind': 'gift', 'author': '早期送礼人',
                'at_utc': iso(start_ts + 10), 'source_key': 'g1',
                'sender_uid': '550001', 'identity_status': 'uid', 'sender_hidden': False,
                'gift': {'event_type': 'SEND_GIFT', 'sender_uid': '550001', 'gift_id': '1',
                         'gift_name': '花束', 'num': 1, 'coin_type': 'gold', 'price': 2000, 'total_coin': 2000,
                         'time_source': 'event', 'event_at_utc': iso(start_ts + 10)},
            })),
        )

    sessions_dir = tmp_path / 'legacy_sessions'
    sessions_dir.mkdir()
    _create_completed_session(sessions_dir, 'sess_legacy_1', legacy_db, '123456', start_ts, cutoff_ts)

    wh_path = tmp_path / 'wh.sqlite3'
    res = sync_warehouse(wh_path, sources=[('leg', legacy_db)], session_roots=[sessions_dir])
    assert res['sessions']['ingested'] == 1
    assert warehouse_stats(wh_path)['paid_gifts']['total']['paid_yuan'] == 2.0

    # 验证 latest_uname_ts 独立于无昵称高时间戳观测，且无前置榜单帧时绝不误认 rank_delta
    mod_db = tmp_path / 'mod.sqlite3'
    _create_source_db(mod_db)
    s2 = 1797000000.0
    with sqlite3.connect(mod_db) as db:
        # 先在 t=s2+200 观测到新昵称“新改昵称”
        _insert_raw(db, '123456', s2 + 200, _pb_interact_packet(550099, '新改昵称', int(s2 + 200)))
    sync_warehouse(wh_path, sources=[('mod', mod_db)])

    other_db = tmp_path / 'other_src.sqlite3'
    _create_source_db(other_db)
    with sqlite3.connect(other_db) as db:
        # 另一源同一批次同时含 t=s2+50 旧昵称“早期旧名”与 t=s2+500 无昵称观测 -> 不得覆盖 s2+200 的“新改昵称”
        _insert_raw(db, '654321', s2 + 50, _pb_interact_packet(550099, '早期旧名', int(s2 + 50)))
        _insert_raw(
            db, '654321', s2 + 500,
            {'cmd': 'ENTRY_EFFECT', 'data': {'uid': 550099, 'copy_writing': '欢迎进房'}},
        )
        # 无前置 ONLINE_RANK_V3 基线帧，仅有后帧累计 score=10 -> 绝不能误判 rank_delta
        _insert_raw(
            db, '654321', s2 + 10,
            _pb_gift_packet(uid=0, uname='无***', combo_seg=None, price=1000, total_coin=1000,
                            tid='t_no_prev', timestamp=int(s2 + 10)),
        )
        _insert_raw(db, '654321', s2 + 15, _pb_rank_packet([(550088, '历史积分观众', '', 10, 15)]))

    mod_sessions = tmp_path / 'mod_sessions'
    mod_sessions.mkdir()
    _create_completed_session(mod_sessions, 'sess_no_prev', other_db, '654321', s2, s2 + 60)
    sync_warehouse(wh_path, sources=[('other', other_db)], session_roots=[mod_sessions])

    with sqlite3.connect(wh_path) as wh:
        uname = wh.execute('SELECT latest_uname FROM dim_users WHERE uid = 550099').fetchone()[0]
        assert uname == '新改昵称'
        inf = wh.execute(
            "SELECT inferred_uid FROM fact_events WHERE session_key = 'sess_no_prev' AND kind = 'gift'"
        ).fetchone()[0]
        assert inf is None

    # 验证 talk_finalize 已完成场次采用精修后的 corrected_text 且标为 finalized_talk
    from bilibili_live_monitor.talk_finalize import digest as talk_digest
    from bilibili_live_monitor.talk_finalize import frozen as talk_frozen

    ws_talk = _create_completed_session(
        mod_sessions, 'sess_talk_fin', mod_db, '123456', s2 + 100, s2 + 250,
        segments=[(1, s2 + 110, s2 + 140, '原语音转写有错字')],
    )
    talk_cfg = json.loads((ws_talk / 'config.json').read_text())
    talk_cfg.update(content_mode='talk', talk_finalize=True, ai_enabled=False)
    atomic_json(ws_talk / 'config.json', talk_cfg)
    plan_id, windows = talk_frozen(load_config(ws_talk / 'config.json'))
    rev_row = {
        **windows[0],
        'corrected_text': '原语音转写已修正',
        'reason': '同音修正',
        'confidence': 'high',
        'evidence': [],
    }
    with sqlite3.connect(mod_db) as db:
        db.execute(
            'CREATE TABLE IF NOT EXISTS transcript_revisions('
            'workflow_key TEXT NOT NULL, plan_id TEXT NOT NULL, seq INTEGER NOT NULL, '
            'position INTEGER NOT NULL, data TEXT NOT NULL, PRIMARY KEY(workflow_key, plan_id, seq))'
        )
        db.execute(
            'INSERT INTO transcript_revisions VALUES (?, ?, ?, ?, ?)',
            ('sess_talk_fin', plan_id, 1, 0, json.dumps(rev_row, ensure_ascii=False, sort_keys=True)),
        )
    (ws_talk / 'talk_finalize').mkdir(exist_ok=True)
    atomic_json(
        ws_talk / 'talk_finalize/completed.json',
        {'plan_id': plan_id, 'status': 'completed', 'rows': 1, 'rows_sha256': talk_digest([rev_row])},
    )
    sync_warehouse(wh_path, sources=[('mod', mod_db)], session_roots=[mod_sessions])
    with sqlite3.connect(wh_path) as wh:
        basis = wh.execute("SELECT transcript_basis FROM dim_sessions WHERE session_key = 'sess_talk_fin'").fetchone()[0]
        t_text = wh.execute("SELECT text FROM fact_transcripts WHERE session_key = 'sess_talk_fin' AND seq = 1").fetchone()[0]
        assert basis == 'finalized_talk'
        assert t_text == '原语音转写已修正'


def test_danmaku_u15_face_resolution_priority_and_guardrails(tmp_path: Path):
    """验证匿名/实名弹幕 info[0][15] 头像提取、CRC32->唯一头像优先级、首字/CRC32护栏及未解名盲盒关联。"""
    db_path = tmp_path / 'dm_face_monitor.sqlite3'
    wh_path = tmp_path / 'dm_face_wh.sqlite3'
    sessions_dir = tmp_path / 'sessions'
    sessions_dir.mkdir()
    _create_source_db(db_path)

    start_ts = 1798000000.0
    cutoff_ts = start_ts + 600.0

    uid_realname_dm = 890001
    face_realname = 'a1' * 20

    uid_noname_entry = 890002
    face_noname = 'b2' * 20

    face_collide_a = 'c3' * 20
    face_unseen_blind = 'd4' * 20
    face_garb = 'e5' * 20

    uid_stranger = 899999  # 未进入字典的同姓路人，撞了 uid_realname_dm 的网图头像

    with sqlite3.connect(db_path) as db:
        # 1. 外房 654321：uid_realname_dm 发了一条实名明文弹幕（info[0][15] 为 extra JSON 形态）-> 纳入字典
        _insert_raw(
            db, '654321', start_ts - 200,
            _danmaku_packet(
                '实名外房弹幕', int(start_ts - 200),
                uid=uid_realname_dm, uname='云端旅人', crc32_hex=uid_crc32(uid_realname_dm),
                face_url=f'https://i1.hdslb.com/bfs/face/{face_realname}.jpg',
                u15_mode='extra_json',
            ),
        )
        # 2. 外房 654321：uid_noname_entry 仅通过无昵称飘屏曝光 uid + face（first_char 为 NULL）
        _insert_raw(
            db, '654321', start_ts - 180,
            {
                'cmd': 'ENTRY_EFFECT',
                'data': {
                    'uid': uid_noname_entry,
                    'copy_writing': '欢迎贵宾进入直播间',
                    'face': f'https://i2.hdslb.com/bfs/face/{face_noname}.webp',
                },
            },
        )
        # 3. 外房 654321：CRC32 碰撞同姓双用户，其中 COLLIDING_UID_A 拥有唯一自定义头像
        _insert_raw(
            db, '654321', start_ts - 150,
            _pb_rank_packet([
                (COLLIDING_UID_A, '林海雪原', f'https://i0.hdslb.com/bfs/face/{face_collide_a}.jpg', 10, 20),
                (COLLIDING_UID_B, '林间牧歌', 'https://i0.hdslb.com/bfs/face/member/noface.jpg', 20, 20),
            ]),
        )

        # 本场 123456 测试弹幕集合：
        # (a) CRC32+首字 与 唯一头像同时命中 -> 必须优先走 crc32_first_char
        _insert_raw(
            db, '123456', start_ts + 10,
            _danmaku_packet(
                '优先级测试弹幕', int(start_ts + 10),
                uname='云***', crc32_hex=uid_crc32(uid_realname_dm),
                face_url=f'https://i1.hdslb.com/bfs/face/{face_realname}.jpg',
            ),
        )
        # (b) 字典 first_char 为 NULL 导致 CRC32 未命中 -> 通过唯一头像认领 (json_str 形态)
        _insert_raw(
            db, '123456', start_ts + 20,
            _danmaku_packet(
                '无昵称字典头像认领', int(start_ts + 20),
                uname='月***', crc32_hex=uid_crc32(uid_noname_entry),
                face_url=f'https://i2.hdslb.com/bfs/face/{face_noname}.webp',
                u15_mode='json_str',
            ),
        )
        # (c) CRC32 同姓碰撞 ambiguous_crc32 -> 带唯一头像那条消歧给 COLLIDING_UID_A，同场灰头那条保持 ambiguous_crc32
        _insert_raw(
            db, '123456', start_ts + 30,
            _danmaku_packet(
                '碰撞靠唯一头像消歧', int(start_ts + 30),
                uname='林***', crc32_hex=uid_crc32(COLLIDING_UID_A),
                face_url=f'https://i0.hdslb.com/bfs/face/{face_collide_a}.jpg',
            ),
        )
        _insert_raw(
            db, '123456', start_ts + 32,
            _danmaku_packet(
                '碰撞灰头保持未消解', int(start_ts + 32),
                uname='林***', crc32_hex=uid_crc32(COLLIDING_UID_B),
                face_url='https://i0.hdslb.com/bfs/face/member/noface.jpg',
            ),
        )
        # (d) 护栏1：首字不一致（字典为“云”，弹幕为“赵***”，crc32 缺失）-> 严禁误认
        _insert_raw(
            db, '123456', start_ts + 40,
            _danmaku_packet(
                '首字冲突拦截', int(start_ts + 40),
                uname='赵***', crc32_hex='00000000',
                face_url=f'https://i1.hdslb.com/bfs/face/{face_realname}.jpg',
            ),
        )
        # (e) 护栏2：同首字“云***”但 CRC32 属于未入库路人 uid_stranger -> 严禁因撞网图误认给 uid_realname_dm
        _insert_raw(
            db, '123456', start_ts + 50,
            _danmaku_packet(
                '同姓撞网图CRC32矛盾拦截', int(start_ts + 50),
                uname='云***', crc32_hex=uid_crc32(uid_stranger),
                face_url=f'https://i1.hdslb.com/bfs/face/{face_realname}.jpg',
            ),
        )
        # (e2) 改名老观众：首字从“海”改为“云”，但 CRC32 与唯一头像双哈希同时匹配 -> 安全认领
        _insert_raw(
            db, '123456', start_ts + 55,
            _danmaku_packet(
                '改名老粉双哈希认领', int(start_ts + 55),
                uname='海***', crc32_hex=uid_crc32(uid_realname_dm),
                face_url=f'https://i1.hdslb.com/bfs/face/{face_realname}.jpg',
            ),
        )
        # (f) 未露真身观众：发弹幕 + 抽盲盒携带同一非装扮头像 face_unseen_blind，外加装扮头像过滤
        _insert_raw(
            db, '123456', start_ts + 60,
            _danmaku_packet(
                '未解名盲盒前弹幕', int(start_ts + 60),
                uname='风***', crc32_hex='abcdef01',
                face_url=f'https://i0.hdslb.com/bfs/face/{face_unseen_blind}.jpg',
            ),
        )
        _insert_raw(
            db, '123456', start_ts + 70,
            _pb_gift_packet(
                uid=0, uname='风***',
                face=f'https://i0.hdslb.com/bfs/face/{face_unseen_blind}.jpg',
                combo_seg=None, gift_id=32099, gift_name='梦幻盲盒礼',
                price=50000, total_coin=15000, blind_orig_name='星光盲盒', blind_orig_price=15000,
                tid='tid_unseen_face_blind', timestamp=int(start_ts + 70),
            ),
        )
        _insert_raw(
            db, '123456', start_ts + 80,
            _danmaku_packet(
                '装扮头像过滤弹幕', int(start_ts + 80),
                uname='雨***', crc32_hex='12345678',
                face_url=f'https://i0.hdslb.com/bfs/garb/{face_garb}.png',
            ),
        )

    _create_completed_session(sessions_dir, 'sess_dm_face', db_path, '123456', start_ts, cutoff_ts)
    sync_warehouse(wh_path, sources=[('anon', db_path)], session_roots=[sessions_dir])

    with sqlite3.connect(wh_path) as wh:
        dm_rows = wh.execute(
            "SELECT content_text, resolved_uid, resolution_basis, clue_face_sha1 "
            "FROM v_resolved_events WHERE kind = 'danmaku' ORDER BY event_seq"
        ).fetchall()
        by_text = {r[0]: (r[1], r[2], r[3]) for r in dm_rows}

        assert by_text['优先级测试弹幕'] == (uid_realname_dm, 'crc32_first_char', face_realname)
        assert by_text['无昵称字典头像认领'] == (uid_noname_entry, 'unique_face', face_noname)
        assert by_text['碰撞靠唯一头像消歧'] == (COLLIDING_UID_A, 'unique_face', face_collide_a)
        assert by_text['碰撞灰头保持未消解'] == (None, 'ambiguous_crc32', None)
        assert by_text['首字冲突拦截'] == (None, 'unresolved', face_realname)
        assert by_text['同姓撞网图CRC32矛盾拦截'] == (None, 'unresolved', face_realname)
        assert by_text['改名老粉双哈希认领'] == (uid_realname_dm, 'unique_face', face_realname)
        assert by_text['未解名盲盒前弹幕'] == (None, 'unresolved', face_unseen_blind)
        assert by_text['装扮头像过滤弹幕'] == (None, 'unresolved', None)

        # 未解出时可通过保留的 clue_face_sha1 精确关联同场盲盒与本人弹幕
        linked_dm = wh.execute(
            """
            SELECT d.content_text
            FROM v_resolved_events g
            JOIN v_resolved_events d
              ON d.session_key = g.session_key
             AND d.kind = 'danmaku'
             AND d.clue_face_sha1 = g.clue_face_sha1
            WHERE g.kind = 'gift' AND g.gift_name = '梦幻盲盒礼'
            """
        ).fetchall()
        assert linked_dm == [('未解名盲盒前弹幕',)]

        # presence 同步包含通过 unique_face 解出的弹幕用户
        pres_noname = wh.execute(
            'SELECT danmaku_count FROM fact_session_presence WHERE resolved_uid = ?',
            (uid_noname_entry,),
        ).fetchone()
        assert pres_noname == (1,)


def test_room_metrics_minute_bucketing_and_cohost_intervals(tmp_path: Path):
    """验证 fact_room_metrics 分钟最新桶聚合与 fact_cohost_intervals PK/惩罚/连麦区间提取。"""
    db_path = tmp_path / 'ctx_monitor.sqlite3'
    wh_path = tmp_path / 'ctx_wh.sqlite3'
    sessions_dir = tmp_path / 'sessions'
    sessions_dir.mkdir()
    _create_source_db(db_path)

    # 起点对齐整分：1800000000 为 60 的整数倍
    start_ts = 1800000000.0
    cutoff_ts = start_ts + 900.0

    ws = _create_completed_session(sessions_dir, 'sess_room_ctx', db_path, '123456', start_ts, cutoff_ts)
    # 模拟短房号 123456 映射规范长房号 12345600
    cfg = json.loads((ws / 'config.json').read_text())
    cfg['platform_room_id'] = '12345600'
    atomic_json(ws / 'config.json', cfg)

    with sqlite3.connect(db_path) as db:
        # 场外噪声观测（不应进入 fact_room_metrics）
        _insert_raw(db, '123456', start_ts - 10, {'cmd': 'ONLINE_RANK_COUNT', 'data': {'online_count': 9999}})

        # 第 0 分钟桶 [start_ts, start_ts+60)：多条观测取桶内最新，fans_club 显式 0 保留 0
        _insert_raw(db, '123456', start_ts + 5, {'cmd': 'ONLINE_RANK_COUNT', 'data': {'online_count': 120}})
        _insert_raw(db, '123456', start_ts + 45, {'cmd': 'ONLINE_RANK_COUNT', 'data': {'online_count': 155}})
        _insert_raw(db, '123456', start_ts + 20, {'cmd': 'WATCHED_CHANGE', 'data': {'num': 1024}})
        _insert_raw(
            db, '123456', start_ts + 50,
            {'cmd': 'ROOM_REAL_TIME_MESSAGE_UPDATE', 'data': {'roomid': 12345600, 'fans': 5000, 'fans_club': 0}},
        )

        # 第 1 分钟桶 [start_ts+60, start_ts+120)：仅有 online_count，其余列缺测为 NULL
        _insert_raw(db, '123456', start_ts + 75, {'cmd': 'ONLINE_RANK_COUNT', 'data': {'online_count': 180}})

        # PK 第 1 局 (pk_id=5001)：含准备(t+60) -> 正式对战(t+70..t+370) -> 惩罚(t+372..t+432)
        _insert_raw(
            db, '123456', start_ts + 60,
            {
                'cmd': 'PK_BATTLE_PRE_NEW',
                'pk_id': 5001,
                'timestamp': int(start_ts + 60),
                'data': {'uid': 660101, 'room_id': 654321, 'uname': '虚构对手主播甲', 'pre_timer': 10},
            },
        )
        _insert_raw(
            db, '123456', start_ts + 70,
            {
                'cmd': 'PK_BATTLE_START_NEW',
                'pk_id': 5001,
                'timestamp': int(start_ts + 70),
                'data': {
                    'pk_start_time': int(start_ts + 70),
                    'pk_frozen_time': int(start_ts + 370),
                    'pk_end_time': int(start_ts + 370),
                    'init_info': {'room_id': 12345600},
                    'match_info': {'room_id': 654321},
                },
            },
        )
        _insert_raw(
            db, '123456', start_ts + 120,
            {
                'cmd': 'PK_INFO',
                'data': {
                    'pk_basic': {
                        'pk_id': 5001,
                        'status': 201,
                        'start_time': int(start_ts + 60),
                        'end_time': int(start_ts + 370),
                        'punish_end_time': int(start_ts + 430),
                        'sprint_duration': 15,
                    },
                    'members': [
                        {'uid': 550101, 'room_id': 12345600, 'uname': '虚构主播123456', 'votes': 800, 'is_winner': 0},
                        {'uid': 660101, 'room_id': 654321, 'uname': '虚构对手主播甲', 'votes': 1200, 'is_winner': 0},
                    ],
                },
            },
        )
        _insert_raw(
            db, '123456', start_ts + 370,
            {
                'cmd': 'PK_BATTLE_SETTLE_NEW',
                'pk_id': 5001,
                'timestamp': int(start_ts + 370),
                'data': {
                    'pk_id': 5001,
                    'punish_end_time': int(start_ts + 432),
                    'punish_name': '贴纸条',
                    'init_info': {'room_id': 12345600, 'votes': 900, 'result_type': -1},
                    'match_info': {'room_id': 654321, 'votes': 1500, 'result_type': 2},
                },
            },
        )
        _insert_raw(
            db, '123456', start_ts + 372,
            {
                'cmd': 'PK_BATTLE_VIDEO_PUNISH_BEGIN',
                'pk_id': 5001,
                'timestamp': int(start_ts + 372),
                'data': {
                    'video_punish': {'duration': 60, 'punish_name': '贴纸条'},
                    'init_info': {'room_id': 12345600, 'votes': 900, 'winner_type': -1},
                    'match_info': {'room_id': 654321, 'votes': 1500, 'winner_type': 2},
                },
            },
        )
        _insert_raw(
            db, '123456', start_ts + 425,
            {
                'cmd': 'PK_BATTLE_PUNISH_END',
                'pk_id': 5001,
                'timestamp': int(start_ts + 425),
                'data': {},
            },
        )

        # PK 第 2 局 (pk_id=5002)：平局无惩罚，结束同秒发 PUNISH_END -> 只出 pk，不出 pk_punish
        _insert_raw(
            db, '123456', start_ts + 450,
            {
                'cmd': 'PK_BATTLE_START',
                'pk_id': 5002,
                'timestamp': int(start_ts + 450),
                'data': {
                    'pk_start_time': int(start_ts + 450),
                    'pk_frozen_time': int(start_ts + 510),
                    'init_info': {'room_id': 12345600},
                    'match_info': {'room_id': 654322},
                },
            },
        )
        _insert_raw(
            db, '123456', start_ts + 510,
            {
                'cmd': 'PK_BATTLE_END',
                'pk_id': 5002,
                'timestamp': int(start_ts + 510),
                'data': {
                    'init_info': {'room_id': 12345600, 'votes': 0, 'winner_type': 3},
                    'match_info': {'room_id': 654322, 'votes': 0, 'winner_type': 3},
                },
            },
        )
        _insert_raw(
            db, '123456', start_ts + 510,
            {'cmd': 'PK_BATTLE_PUNISH_END', 'pk_id': 5002, 'timestamp': int(start_ts + 510), 'data': {}},
        )

        # 语音连麦：排麦队列 (t+600..t+610) -> 接通上麦连发2条重复 status=1 (t+610) -> 挂断连发2条 status=0 (t+720)
        _insert_raw(
            db, '123456', start_ts + 600,
            {'cmd': 'VOICE_JOIN_ROOM_COUNT_INFO', 'data': {'apply_count': 1, 'notify_count': 0, 'room_status': 1}},
        )
        _insert_raw(
            db, '123456', start_ts + 602,
            {'cmd': 'VOICE_JOIN_LIST', 'data': {'apply_count': 2, 'category': 1, 'refresh': 1}},
        )
        for dup_offset in (610, 611):
            _insert_raw(
                db, '123456', start_ts + dup_offset,
                {
                    'cmd': 'VOICE_JOIN_STATUS',
                    'data': {
                        'status': 1,
                        'channel': 'ch_voice_01',
                        'channel_type': 'voice',
                        'start_at': int(start_ts + 610),
                        'current_time': int(start_ts + dup_offset),
                        'uid': 770201,
                        'user_name': '虚构连麦舰长',
                        'room_id': 12345600,
                        'is_mystery': False,
                    },
                },
            )
        for dup_off_end in (720, 720.2):
            _insert_raw(
                db, '123456', start_ts + dup_off_end,
                {
                    'cmd': 'VOICE_JOIN_STATUS',
                    'data': {
                        'status': 0,
                        'channel': 'ch_voice_01',
                        'channel_type': 'voice',
                        'start_at': 0,
                        'current_time': int(start_ts + 720),
                        'uid': 0,
                        'user_name': '',
                        'is_mystery': False,
                    },
                },
            )

    sync_res = sync_warehouse(wh_path, sources=[('anon', db_path)], session_roots=[sessions_dir])
    assert sync_res['room_metric_rows'] == 2
    assert sync_res['cohost_intervals'] == 5

    with sqlite3.connect(wh_path) as wh:
        metrics = wh.execute(
            'SELECT minute_ts, online_count, watched_count, fans_count, fans_club_count '
            'FROM fact_room_metrics ORDER BY minute_ts'
        ).fetchall()
        assert metrics == [
            (int(start_ts), 155, 1024, 5000, 0),
            (int(start_ts + 60), 180, None, None, None),
        ]

        cohosts = wh.execute(
            'SELECT kind, start_ts, end_ts, battle_id, opponent_uid, opponent_room_id, opponent_uname, raw_summary_json '
            'FROM fact_cohost_intervals ORDER BY interval_seq'
        ).fetchall()
        assert [(r[0], r[1], r[2], r[3], r[4], r[5], r[6]) for r in cohosts] == [
            ('pk', start_ts + 60, start_ts + 370, '5001', 660101, '654321', '虚构对手主播甲'),
            ('pk_punish', start_ts + 372, start_ts + 425, '5001', 660101, '654321', '虚构对手主播甲'),
            ('pk', start_ts + 450, start_ts + 510, '5002', None, '654322', None),
            ('voice_join_queue', start_ts + 600, start_ts + 610, None, None, None, None),
            ('voice_join', start_ts + 610, start_ts + 720, 'ch_voice_01', 770201, None, '虚构连麦舰长'),
        ]
        pk1_meta = json.loads(cohosts[0][7])
        assert pk1_meta['sprint_start_ts'] == start_ts + 355
        assert pk1_meta['self_votes'] == 900
        assert pk1_meta['opponent_votes'] == 1500
        assert pk1_meta['self_winner'] == 0

    st = warehouse_stats(wh_path)
    assert st['room_metric_rows'] == 2
    assert st['cohost_intervals'] == 5
    assert st['cohost_intervals_by_kind'] == {
        'pk': 2,
        'pk_punish': 1,
        'voice_join_queue': 1,
        'voice_join': 1,
    }

    # 回归：旧配置无 platform_room_id（仅有短房号 123456 与 rooms 表 anchor_uid），先收到 START 后收到 PK_INFO 再收到本房获胜 END
    s2_start = start_ts + 2000.0
    s2_cutoff = s2_start + 300.0
    with sqlite3.connect(db_path) as db:
        db.execute(
            'INSERT OR REPLACE INTO rooms(room_id, data) VALUES (?, ?)',
            ('123456', json.dumps({'room_id': '123456', 'uid': 550101, 'updated_at': iso(s2_start)})),
        )
        _insert_raw(
            db, '123456', s2_start + 10,
            {
                'cmd': 'PK_BATTLE_START_NEW',
                'pk_id': 5009,
                'timestamp': int(s2_start + 10),
                'data': {
                    'pk_start_time': int(s2_start + 10),
                    'pk_frozen_time': int(s2_start + 190),
                    'init_info': {'room_id': 12345600},
                    'match_info': {'room_id': 654399},
                },
            },
        )
        _insert_raw(
            db, '123456', s2_start + 12,
            {
                'cmd': 'PK_INFO',
                'data': {
                    'pk_basic': {'pk_id': 5009, 'status': 201, 'start_time': int(s2_start + 10), 'end_time': int(s2_start + 190)},
                    'members': [
                        {'uid': 550101, 'room_id': 12345600, 'uname': '虚构主播123456', 'votes': 2000, 'is_winner': 0},
                        {'uid': 660999, 'room_id': 654399, 'uname': '虚构对手乙', 'votes': 500, 'is_winner': 0},
                    ],
                },
            },
        )
        _insert_raw(
            db, '123456', s2_start + 190,
            {
                'cmd': 'PK_BATTLE_END',
                'pk_id': 5009,
                'timestamp': int(s2_start + 190),
                'data': {
                    'init_info': {'room_id': 12345600, 'votes': 3000, 'winner_type': 2},
                    'match_info': {'room_id': 654399, 'votes': 600, 'winner_type': -1},
                },
            },
        )
    _create_completed_session(sessions_dir, 'sess_short_no_plat', db_path, '123456', s2_start, s2_cutoff)
    sync_warehouse(wh_path, sources=[('anon', db_path)], session_roots=[sessions_dir])
    with sqlite3.connect(wh_path) as wh:
        short_pk = wh.execute(
            "SELECT opponent_uid, opponent_room_id, opponent_uname, raw_summary_json "
            "FROM fact_cohost_intervals WHERE session_key = 'sess_short_no_plat' AND kind = 'pk'"
        ).fetchone()
        assert short_pk[:3] == (660999, '654399', '虚构对手乙')
        short_meta = json.loads(short_pk[3])
        assert short_meta['self_winner'] == 1
        assert short_meta['self_votes'] == 3000
        assert short_meta['opponent_votes'] == 600


def test_v1_to_v2_schema_migration_and_auto_reingest(tmp_path: Path):
    """验证旧版 v1 数仓（缺新表、旧游标、旧视图、extractor_version=1）在 sync 时自动平滑升级并与全新 v2 一致。"""
    db_path = tmp_path / 'v1_src.sqlite3'
    wh_path = tmp_path / 'v1_warehouse.sqlite3'
    fresh_wh_path = tmp_path / 'fresh_v2_warehouse.sqlite3'
    sessions_dir = tmp_path / 'sessions'
    sessions_dir.mkdir()
    _create_source_db(db_path)

    start_ts = 1801000000.0
    cutoff_ts = start_ts + 180.0
    face_hash = 'f6' * 20
    shared_extra_face = 'e7' * 20

    with sqlite3.connect(db_path) as db:
        _insert_raw(db, '123456', start_ts + 15, {'cmd': 'ONLINE_RANK_COUNT', 'data': {'online_count': 88}})
        _insert_raw(
            db, '123456', start_ts + 30,
            {
                'cmd': 'VOICE_JOIN_STATUS',
                'data': {
                    'status': 1, 'channel': 'ch_mig', 'channel_type': 'voice',
                    'start_at': int(start_ts + 30), 'current_time': int(start_ts + 30),
                    'uid': 810001, 'user_name': '虚构连麦观众', 'is_mystery': False,
                },
            },
        )
        # 故意测 v1 视图曾存在的缺陷：路人撞了同图但首字不同（字典“张”，弹幕“李***”）
        _insert_raw(
            db, '654321', start_ts - 50,
            _pb_rank_packet([
                (810002, '张山风', f'https://i0.hdslb.com/bfs/face/{face_hash}.jpg', 10, 15),
                (810010, '共享图用户甲', f'https://i0.hdslb.com/bfs/face/{shared_extra_face}.jpg', 10, 15),
            ]),
        )
        # 外房历史：用户乙仅通过 v2 新支持的 extra JSON 形态实名弹幕暴露同一头像 shared_extra_face
        _insert_raw(
            db, '654321', start_ts - 40,
            _danmaku_packet(
                '外房extra头像弹幕', int(start_ts - 40),
                uid=810011, uname='共享图用户乙', crc32_hex=uid_crc32(810011),
                face_url=f'https://i0.hdslb.com/bfs/face/{shared_extra_face}.jpg',
                u15_mode='extra_json',
            ),
        )
        _insert_raw(
            db, '123456', start_ts + 40,
            _danmaku_packet(
                '升级前误认测试弹幕', int(start_ts + 40),
                uname='李***', crc32_hex='00000000',
                face_url=f'https://i0.hdslb.com/bfs/face/{face_hash}.jpg',
            ),
        )
        _insert_raw(
            db, '123456', start_ts + 50,
            _pb_gift_packet(
                uid=0, uname='共***',
                face=f'https://i0.hdslb.com/bfs/face/{shared_extra_face}.jpg',
                combo_seg=None, gift_id=31001, gift_name='测试共享图盲盒',
                price=2000, total_coin=2000, tid='tid_v1_mig_shared', timestamp=int(start_ts + 50),
            ),
        )

    _create_completed_session(sessions_dir, 'sess_mig_v1', db_path, '123456', start_ts, cutoff_ts)

    # 先初始化再降级模拟真实 v1 状态（删掉两张 v2 新表、删掉游标新列与 extra 头像记录、还原 v1 旧视图、降 extractor_version=1）
    sync_warehouse(wh_path, sources=[('s', db_path)], session_roots=[sessions_dir])
    with sqlite3.connect(wh_path) as wh:
        wh.executescript(
            f"""
            DROP TABLE fact_room_metrics;
            DROP TABLE fact_cohost_intervals;
            DELETE FROM dim_user_faces WHERE uid = 810011 AND face_sha1 = '{shared_extra_face}';
            CREATE TABLE meta_source_cursors_v1 (
                source_name TEXT PRIMARY KEY,
                db_path TEXT NOT NULL,
                last_raw_event_id INTEGER NOT NULL DEFAULT 0,
                last_guard_snapshot_id INTEGER NOT NULL DEFAULT 0,
                last_timeline_id INTEGER NOT NULL DEFAULT 0,
                updated_at_utc TEXT NOT NULL
            );
            INSERT INTO meta_source_cursors_v1
            SELECT source_name, db_path, last_raw_event_id, last_guard_snapshot_id, last_timeline_id, updated_at_utc
            FROM meta_source_cursors;
            DROP TABLE meta_source_cursors;
            ALTER TABLE meta_source_cursors_v1 RENAME TO meta_source_cursors;
            DROP VIEW v_resolved_events;
            CREATE VIEW v_resolved_events AS
            SELECT
                e.*,
                COALESCE(e.raw_uid, e.resolved_crc32_uid, e.inferred_uid) AS resolved_uid,
                'legacy' AS actor_key,
                'legacy' AS resolution_basis
            FROM fact_events e;
            UPDATE dim_sessions SET extractor_version = 1;
            """
        )

    # 未执行 v2 sync 前，只读 stats 兼容旧 v1 库不报错
    pre_stats = warehouse_stats(wh_path)
    assert pre_stats['room_metric_rows'] == 0
    assert pre_stats['cohost_intervals'] == 0

    # 执行 v2 sync：自动升级游标补扫历史弹幕头像、建新表、替换视图、重入 extractor_version=1 的旧场次
    mig_res = sync_warehouse(wh_path, sources=[('s', db_path)], session_roots=[sessions_dir])
    assert mig_res['sessions']['ingested'] == 1
    assert mig_res['room_metric_rows'] == 1
    assert mig_res['cohost_intervals'] == 1

    # 全新建仓 v2 对照：升级后的 v2 数仓与全新建仓 v2 的事件消解结果完全一致
    sync_warehouse(fresh_wh_path, sources=[('s', db_path)], session_roots=[sessions_dir])
    with sqlite3.connect(wh_path) as wh, sqlite3.connect(fresh_wh_path) as fresh_wh:
        ver = wh.execute("SELECT extractor_version FROM dim_sessions WHERE session_key = 'sess_mig_v1'").fetchone()[0]
        assert ver == 2
        dm_check = wh.execute(
            "SELECT resolved_uid, resolution_basis FROM v_resolved_events WHERE content_text = '升级前误认测试弹幕'"
        ).fetchone()
        assert dm_check == (None, 'unresolved')
        gift_check = wh.execute(
            "SELECT resolved_uid, resolution_basis FROM v_resolved_events WHERE gift_name = '测试共享图盲盒'"
        ).fetchone()
        assert gift_check == (None, 'unresolved')
        migrated_resolved = wh.execute(
            'SELECT event_seq, kind, resolved_uid, actor_key, resolution_basis FROM v_resolved_events ORDER BY event_seq'
        ).fetchall()
        fresh_resolved = fresh_wh.execute(
            'SELECT event_seq, kind, resolved_uid, actor_key, resolution_basis FROM v_resolved_events ORDER BY event_seq'
        ).fetchall()
        assert migrated_resolved == fresh_resolved

    # 再次执行 sync：场次与游标均已是 v2，直接幂等跳过
    again = sync_warehouse(wh_path, sources=[('s', db_path)], session_roots=[sessions_dir])
    assert again['sessions']['ingested'] == 0
    assert again['sessions']['skipped_already_ingested'] == 1


def cli_main_helper(argv: list[str]) -> int:
    import sys
    old_argv = sys.argv
    try:
        sys.argv = ['bili-live', *argv]
        return cli_main()
    finally:
        sys.argv = old_argv
