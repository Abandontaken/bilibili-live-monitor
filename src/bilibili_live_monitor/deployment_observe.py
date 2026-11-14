"""只报告当前业务进程实际绑定，不把启动意图冒充运行验收。"""
import hashlib
import json
import os
import sys
from pathlib import Path


def observe():
    import bilibili_live_monitor
    source = Path(bilibili_live_monitor.__file__).resolve().parent
    files = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(source.glob('*.py'))}
    return {'pid': os.getpid(), 'python': sys.executable, 'package': str(source),
            'release_id': os.environ.get('BILI_RELEASE_ID'), 'module_sha256': files,
            'task_pin': os.environ.get('BILI_TASK_PIN'), 'status': 'current_process_observed'}


if __name__ == '__main__':
    print(json.dumps(observe(), ensure_ascii=False, indent=2))
