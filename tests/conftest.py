"""全局测试隔离夹具：确保测试期间离线 ASR 全局并发信号量使用临时目录，绝不与宿主机常驻进程争用锁槽位。"""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _isolate_global_asr_lock_dir(tmp_path, monkeypatch):
    monkeypatch.setenv('BILI_ASR_LOCK_DIR', str(tmp_path / '.pytest_asr_global_locks'))
