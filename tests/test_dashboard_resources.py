import time

import httpx
import pytest
from fastapi.testclient import TestClient

from bilibili_live_monitor.dashboard_gateway import create_gateway


def config(**extra):
    return {'instances': [{'id': 'guest', 'label': '测试', 'url': 'http://127.0.0.1:8766'}], **extra}


def metrics(**extra):
    return {'sampled_at': time.time(), 'cpu_percent': 25, 'memory_total': 64 * 1024 ** 3,
            'memory_used': 16 * 1024 ** 3, 'gpu_usage': 40, 'network_recv_rate': 2_000_000,
            'network_sent_rate': 125_000, 'private': 'SECRET', **extra}


def test_yamon_is_projected_once_as_system_metrics_not_per_instance_usage():
    calls = []

    def upstream(request):
        calls.append((str(request.url), request.method))
        return httpx.Response(200, json=metrics())

    with TestClient(create_gateway(config(resources_url='http://127.0.0.1:8000/'),
                                   transport=httpx.MockTransport(upstream))) as client:
        response = client.get('/api/dashboard/resources')
        row = response.json()
        assert response.status_code == 200 and response.headers['cache-control'] == 'no-store'
        assert row['source'] == 'yamon' and row['scope'] == 'system'
        assert row['cpu_percent'] == 25 and row['memory_used_gib'] == 16 and row['memory_total_gib'] == 64
        assert row['download_mbps'] == 16 and row['upload_mbps'] == 1 and row['gpu_percent'] == 40
        assert 'SECRET' not in response.text and 'cpu_cores' not in row and 'rss_gib' not in row
    assert calls == [('http://127.0.0.1:8000/api/metrics', 'GET')]


@pytest.mark.parametrize('url', ['https://example.org', 'http://example.org', 'http://user@localhost:8000',
                                 'http://localhost:8000/api', 'http://localhost:8000?url=secret', 8000])
def test_resource_upstream_is_explicit_local_http_only(url):
    with pytest.raises(ValueError):
        create_gateway(config(resources_url=url))


def test_unconfigured_yamon_does_not_probe_ports():
    def upstream(request):
        pytest.fail('未配置时不应调用资源上游')

    with TestClient(create_gateway(config(), transport=httpx.MockTransport(upstream))) as client:
        assert client.get('/api/dashboard/resources').status_code == 503


@pytest.mark.parametrize('failure', ['http', 'timeout', 'bad_json', 'bad_number', 'missing_time'])
def test_resource_failure_does_not_break_other_dashboard_endpoints(failure):
    def upstream(request):
        if failure == 'timeout':
            raise httpx.ReadTimeout('SECRET', request=request)
        if failure == 'http':
            return httpx.Response(500, text='SECRET')
        if failure == 'bad_json':
            return httpx.Response(200, text='SECRET')
        return httpx.Response(200, json=metrics(**({'cpu_percent': True} if failure == 'bad_number'
                                                  else {'sampled_at': None})))

    with TestClient(create_gateway(config(resources_url='http://localhost:8000'),
                                   transport=httpx.MockTransport(upstream))) as client:
        response = client.get('/api/dashboard/resources')
        assert response.status_code == 503 and 'SECRET' not in response.text
        assert client.get('/api/dashboard/instances').status_code == 200


def test_absent_gpu_and_network_are_unknown_not_zero():
    def upstream(request):
        return httpx.Response(200, json=metrics(gpu_usage=None, network_recv_rate=None, network_sent_rate=None))

    with TestClient(create_gateway(config(resources_url='http://localhost:8000'),
                                   transport=httpx.MockTransport(upstream))) as client:
        data = client.get('/api/dashboard/resources').json()
        assert data['gpu_percent'] is None and data['download_mbps'] is None and data['upload_mbps'] is None
