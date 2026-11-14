import os
import sys

from bilibili_live_monitor.deployment_observe import observe


def test_observation_reports_actual_process_not_launch_intent(monkeypatch):
    monkeypatch.setenv('BILI_RELEASE_ID', 'synthetic')
    result = observe()
    assert result['pid'] == os.getpid()
    assert result['python'] == sys.executable
    assert result['release_id'] == 'synthetic'
    assert 'deployment.py' in result['module_sha256']
    assert result['status'] == 'current_process_observed'
