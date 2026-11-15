"""程序累计保存已产生的分类，防止AI整文件写回时丢弃其他批次。"""
import fcntl
import json
from pathlib import Path

from .live_share import atomic_json


def read_annotations(workspace):
    root = Path(workspace)
    with (root / 'annotation_history.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        current_path = root / 'annotations.json'
        current = json.loads(current_path.read_text()) if current_path.exists() else {}
        history_path = root / 'annotation_history.json'
        history = json.loads(history_path.read_text()) if history_path.exists() else {'reviews': []}
        merged = {}
        for row in [*history.get('reviews', []), *current.get('reviews', [])]:
            if isinstance(row, dict) and type(row.get('seq')) is int and isinstance(row.get('fingerprint'), str):
                merged[row['seq']] = row
        reviews = sorted(merged.values(), key=lambda row: row['seq'])
        if history.get('reviews') != reviews:
            atomic_json(history_path, {'reviews': reviews})
        # 不覆盖AI正在写的文件；所有程序消费者读取累计结果。
        return {**current, 'reviews': reviews}
