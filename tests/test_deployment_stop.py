"""真实合成父子进程停启验收；不连接生产Launcher或运行Cursor。"""
import json
import os
import subprocess
import sys
from pathlib import Path

from bilibili_live_monitor import cursor_finalize, deployment


def test_controlled_parent_child_exit_and_persistent_pause(tmp_path):
    receipt = tmp_path / 'exit.json'
    script = '''
import json, signal, subprocess, sys
from pathlib import Path
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
def stop(signum, frame):
    child.terminate()
    code = child.wait(timeout=5)
    Path(sys.argv[1]).write_text(json.dumps({'child_pid': child.pid, 'child_exit': code}))
    raise SystemExit(0)
signal.signal(signal.SIGTERM, stop)
print(json.dumps({'parent': __import__('os').getpid(), 'child': child.pid}), flush=True)
signal.pause()
'''
    process = subprocess.Popen([sys.executable, '-c', script, str(receipt)],
                               stdout=subprocess.PIPE, text=True, start_new_session=True)
    try:
        identity = json.loads(process.stdout.readline())
        assert identity['parent'] == process.pid
        cursor_finalize.stop_group(process)
        assert process.returncode == 0
        evidence = json.loads(receipt.read_text())
        assert evidence['child_pid'] == identity['child']
        assert evidence['child_exit'] is not None
    finally:
        if process.poll() is None:
            cursor_finalize.stop_group(process)
    guard = tmp_path / 'guard.json'
    deployment.atomic(guard, {'mode': 'paused'})
    env = {**os.environ, 'BILI_DEPLOYMENT_GUARD': str(guard),
           'PYTHONPATH': str(Path(deployment.__file__).resolve().parents[1])}
    probe = [sys.executable, '-P', '-c',
             'from bilibili_live_monitor.deployment import paused; print(paused("sessions"))']
    assert subprocess.check_output(probe, env=env, text=True).strip() == 'True'
    deployment.atomic(guard, {'mode': 'running'})
    assert subprocess.check_output(probe, env=env, text=True).strip() == 'False'
