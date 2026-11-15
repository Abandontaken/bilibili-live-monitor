"""YAMON 整机采样的只读展示投影，不推算监控进程占用。"""
import math
from urllib.parse import urlsplit


def resource_url(value):
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError('资源上游须为本机 HTTP 服务地址')
    parsed = urlsplit(value)
    if (parsed.scheme != 'http' or parsed.hostname not in ('127.0.0.1', 'localhost', '::1')
            or parsed.username or parsed.password or parsed.path not in ('', '/')
            or parsed.query or parsed.fragment):
        raise ValueError('资源上游须为本机 HTTP 服务地址')
    _ = parsed.port
    return value.rstrip('/')


def yamon_metrics(data):
    if not isinstance(data, dict):
        raise ValueError('资源采样格式错误')

    def number(key, *, optional=False, maximum=None):
        value = data.get(key)
        if value is None and optional:
            return None
        if (isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
                or value < 0 or (maximum is not None and value > maximum)):
            raise ValueError('资源采样数值错误')
        return value

    total = number('memory_total')
    used = number('memory_used', maximum=total)
    if total == 0:
        raise ValueError('资源采样内存总量错误')
    down, up = number('network_recv_rate', optional=True), number('network_sent_rate', optional=True)
    return {'source': 'yamon', 'scope': 'system', 'epoch': number('sampled_at'),
            'cpu_percent': number('cpu_percent', maximum=100),
            'memory_used_gib': used / 1024 ** 3, 'memory_total_gib': total / 1024 ** 3,
            'gpu_percent': number('gpu_usage', optional=True, maximum=100),
            'download_mbps': down * 8 / 1e6 if down is not None else None,
            'upload_mbps': up * 8 / 1e6 if up is not None else None}
