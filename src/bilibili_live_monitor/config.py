import json
import math
import os
import re
from dataclasses import dataclass, field, replace
from pathlib import Path

from dotenv import load_dotenv


@dataclass
class Settings:
    automation_config: str = field(default_factory=lambda: os.getenv('AUTOMATION_CONFIG', ''))
    capture_backend: str = field(default_factory=lambda: os.getenv('CAPTURE_BACKEND', 'cdp').strip().lower() or 'cdp')
    record_mode: str = field(default_factory=lambda: os.getenv('RECORD_MODE', 'audio'))
    audio_format: str = field(default_factory=lambda: os.getenv('AUDIO_FORMAT', '').strip().lower())
    audio_bitrate: str = field(default_factory=lambda: os.getenv('AUDIO_BITRATE', '32k').strip().lower() or '32k')
    direct_audio_qn: int = field(default_factory=lambda: int(os.getenv('DIRECT_AUDIO_QN', '80')))
    direct_video_qn: int = field(default_factory=lambda: int(os.getenv('DIRECT_VIDEO_QN', '10000')))
    direct_handshake_interval: float = field(default_factory=lambda: float(os.getenv('DIRECT_HANDSHAKE_INTERVAL', '2.5')))
    asr_enabled: bool = field(default_factory=lambda: os.getenv('ASR_ENABLED', '0').lower() in ('1', 'true', 'yes'))
    preview_enabled: bool = field(default_factory=lambda: os.getenv('PREVIEW_ENABLED', '0').lower() in ('1', 'true', 'yes'))
    recording_segment_seconds: int = field(default_factory=lambda: int(os.getenv('RECORDING_SEGMENT_SECONDS', '300')))
    data_dir: Path = field(default_factory=lambda: Path(os.getenv('DATA_DIR', '.local')))
    cdp_url: str = field(default_factory=lambda: os.getenv('CDP_URL', 'http://127.0.0.1:9222'))
    asr_python: str = field(default_factory=lambda: os.getenv('ASR_PYTHON', '.venvs/asr/bin/python'))
    asr_model: str = field(default_factory=lambda: os.getenv('ASR_MODEL', 'Qwen/Qwen3-ASR-1.7B'))
    chunk_seconds: int = 5  # 实时流式固定5秒输入，30秒仅为模型上下文上限。
    max_rooms: int = field(default_factory=lambda: int(os.getenv('MAX_ROOMS', '10')))
    queue_size: int = field(default_factory=lambda: int(os.getenv('QUEUE_SIZE', '20')))
    silence_dbfs: float = field(default_factory=lambda: float(os.getenv('SILENCE_DBFS', '-55')))
    root_path: str = field(default_factory=lambda: os.getenv('APP_ROOT_PATH', '').rstrip('/'))
    max_reconnects: int = field(default_factory=lambda: int(os.getenv('MAX_RECONNECTS', '20')))
    retry_delay: float = field(default_factory=lambda: float(os.getenv('RETRY_DELAY', '2')))
    retry_max_delay: float = field(default_factory=lambda: float(os.getenv('RETRY_MAX_DELAY', '300')))
    asr_start_timeout: float = 30
    asr_infer_timeout: float = 30
    platform_tick_seconds: float = 2
    platform_start_concurrency: int = 5
    platform_min_free_bytes: int = 5 * 1024**3
    watch_poll_seconds: float = field(default_factory=lambda: float(os.getenv('WATCH_POLL_SECONDS', '30')))
    guard_poll_seconds: float = field(default_factory=lambda: float(os.getenv('GUARD_POLL_SECONDS', '3600')))
    babysitter_command: list[str] = field(default_factory=lambda: json.loads(os.getenv('BABYSITTER_COMMAND', '[]')))
    babysitter_interval_seconds: float = field(default_factory=lambda: float(os.getenv('BABYSITTER_INTERVAL_SECONDS', '300')))
    babysitter_timeout_seconds: float = field(default_factory=lambda: float(os.getenv('BABYSITTER_TIMEOUT_SECONDS', '120')))

    def __post_init__(self):
        if self.capture_backend not in ('cdp', 'direct'):
            raise ValueError('采集后端需为cdp或direct')
        if not self.audio_format:
            self.audio_format = 'opus' if self.capture_backend == 'direct' else 'wav'
        if self.audio_format not in ('wav', 'mka', 'opus'):
            raise ValueError('音频保存格式需为wav、mka或opus')
        if not re.fullmatch(r'[1-9]\d{0,3}k', self.audio_bitrate):
            raise ValueError('音频码率格式无效，例如32k')
        if type(self.direct_audio_qn) is not int or not 1 <= self.direct_audio_qn <= 30000:
            raise ValueError('直连音频流画质档位direct_audio_qn须为1～30000的整数')
        if type(self.direct_video_qn) is not int or not 1 <= self.direct_video_qn <= 30000:
            raise ValueError('直连视频流画质档位direct_video_qn须为1～30000的整数')
        if not math.isfinite(self.direct_handshake_interval) or self.direct_handshake_interval < 0:
            raise ValueError('直连建连间隔不得为负数')
        if self.capture_backend == 'direct':
            if self.audio_format == 'wav':
                raise ValueError('direct模式无浏览器页面，音频保存格式需为opus或mka')
            if self.asr_enabled:
                raise ValueError('direct模式仅支持下播离线转写，不启用实时ASR')
            if self.preview_enabled:
                raise ValueError('direct模式无浏览器页面，不支持缩略图预览')
        if self.record_mode not in ('none', 'audio', 'video') or not 10 <= self.recording_segment_seconds <= 3600:
            raise ValueError('保存模式需为none/audio/video；录制分段需为10～3600秒')
        if not 5 <= self.chunk_seconds <= 60 or self.max_rooms < 1 or self.queue_size < 1:
            raise ValueError('配置越界：切片5～60秒，房间与队列上限至少为1')
        self.data_dir = self.data_dir.resolve()
        if self.max_reconnects < 1 or self.retry_delay < 0 or self.retry_max_delay <= 0:
            raise ValueError('连续重连次数至少为1，退避时间不得为负')
        if self.watch_poll_seconds < 5 or self.guard_poll_seconds < 60:
            raise ValueError('开播巡检至少5秒，舰队快照至少60秒')
        if (not isinstance(self.babysitter_command, list) or
                any(not isinstance(x, str) or not x for x in self.babysitter_command) or
                min(self.platform_tick_seconds, self.platform_start_concurrency,
                    self.babysitter_interval_seconds, self.babysitter_timeout_seconds) <= 0):
            raise ValueError('babysitter命令需为JSON字符串数组；调度间隔和并发需为正数')


CAPTURE_CONFIG_TYPES = {
    'capture_backend': str,
    'record_mode': str,
    'audio_format': str,
    'audio_bitrate': str,
    'direct_audio_qn': int,
    'direct_video_qn': int,
    'direct_handshake_interval': (int, float),
    'asr_enabled': bool,
    'preview_enabled': bool,
    'recording_segment_seconds': int,
    'max_rooms': int,
    'queue_size': int,
}

HOT_RELOADABLE_CAPTURE_KEYS = (
    'direct_audio_qn',
    'direct_video_qn',
    'direct_handshake_interval',
    'audio_bitrate',
    'recording_segment_seconds',
    'max_rooms',
)


def parse_capture_overrides(data: dict) -> dict:
    if not isinstance(data, dict) or 'capture' not in data:
        return {}
    raw = data['capture']
    if not isinstance(raw, dict):
        raise ValueError('automation.capture须为JSON对象')
    overrides = {}
    for key, value in raw.items():
        expected = CAPTURE_CONFIG_TYPES.get(key)
        if expected is None:
            raise ValueError(f'automation.capture包含不支持的字段：{key}')
        if expected is bool:
            if type(value) is not bool:
                raise ValueError(f'automation.capture.{key}须为布尔值')
            overrides[key] = value
        elif expected is int:
            if type(value) is not int:
                raise ValueError(f'automation.capture.{key}须为整数')
            overrides[key] = value
        elif expected == (int, float):
            if type(value) not in (int, float):
                raise ValueError(f'automation.capture.{key}须为数值')
            overrides[key] = float(value)
        else:
            if not isinstance(value, str):
                raise ValueError(f'automation.capture.{key}须为字符串')
            overrides[key] = value.strip().lower() if key in ('capture_backend', 'audio_format', 'audio_bitrate') else value
    if overrides:
        backend = overrides.get(
            'capture_backend',
            'direct'
            if overrides.get('audio_format') in ('opus', 'mka')
            and not overrides.get('asr_enabled')
            and not overrides.get('preview_enabled')
            else 'cdp',
        )
        neutral = _neutral_settings_kwargs(backend)
        neutral.update(overrides)
        Settings(**neutral)
    return overrides


def _neutral_settings_kwargs(backend: str = 'cdp') -> dict:
    return {
        'capture_backend': backend,
        'record_mode': 'audio',
        'audio_format': '',
        'audio_bitrate': '32k',
        'direct_audio_qn': 80,
        'direct_video_qn': 10000,
        'direct_handshake_interval': 2.5,
        'asr_enabled': False,
        'preview_enabled': False,
        'recording_segment_seconds': 300,
        'max_rooms': 10,
        'queue_size': 20,
        'silence_dbfs': -55.0,
        'max_reconnects': 20,
        'retry_delay': 2.0,
        'retry_max_delay': 300.0,
        'watch_poll_seconds': 30.0,
        'guard_poll_seconds': 3600.0,
        'babysitter_command': [],
        'babysitter_interval_seconds': 300.0,
        'babysitter_timeout_seconds': 120.0,
    }


def _safe_env_capture_baseline() -> dict:
    defaults = Settings(**_neutral_settings_kwargs('cdp'))
    baseline = {k: getattr(defaults, k) for k in HOT_RELOADABLE_CAPTURE_KEYS}
    env_parsers = (
        ('direct_audio_qn', 'DIRECT_AUDIO_QN', int),
        ('direct_video_qn', 'DIRECT_VIDEO_QN', int),
        ('direct_handshake_interval', 'DIRECT_HANDSHAKE_INTERVAL', float),
        ('audio_bitrate', 'AUDIO_BITRATE', lambda s: s.strip().lower() or '32k'),
        ('recording_segment_seconds', 'RECORDING_SEGMENT_SECONDS', int),
        ('max_rooms', 'MAX_ROOMS', int),
    )
    for key, env_name, parser in env_parsers:
        raw = os.getenv(env_name)
        if raw is None or not raw.strip():
            continue
        try:
            val = parser(raw)
            replace(defaults, **{key: val})
            baseline[key] = val
        except ValueError:
            pass
    return baseline


def _automation_capture_overrides(automation_config: str) -> dict:
    path_str = (automation_config or '').strip()
    if not path_str:
        return {}
    path = Path(path_str)
    if not path.is_file():
        return {}
    data = json.loads(path.read_text(encoding='utf-8'))
    return parse_capture_overrides(data)


def load_settings():
    load_dotenv()
    automation_config = os.getenv('AUTOMATION_CONFIG', '')
    settings = Settings(**_automation_capture_overrides(automation_config))
    settings._capture_baseline = _safe_env_capture_baseline()
    return settings
