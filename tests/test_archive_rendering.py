import copy
import json
import subprocess
import sys

import numpy as np
import pytest
from test_archive_validation import frame, info

from bilibili_live_monitor import archive_policy as policy
from bilibili_live_monitor import archive_validation as validation
from bilibili_live_monitor import recording_archive as archive


def unknown(pts='0'):
    return {key: value for key, value in frame(pts).items()
            if key not in ('sample_aspect_ratio', 'color_space', 'color_transfer', 'color_primaries')}


def test_unknown_native_and_mixed_reference_do_not_invent_source_declarations():
    original = info([unknown(), frame('0.033', 1920, 1080)])
    saved = copy.deepcopy(original)
    reference = validation.normalize_reference(original, mixed=True)
    assert original == saved
    assert reference['rendering']['mode'] == 'reference-render'
    assert reference['video_evidence']['colors'] == [['bt709', 'unknown', 'unknown']]
    assert len(original['video_evidence']['declaration_epochs']) == 2
    assert reference['video_evidence']['shapes'] == [[1920, 1440, 'yuv420p', '1:1']]
    vf = validation.video_filter(reference, mixed=True)
    assert 'in_color_matrix=auto:out_color_matrix=bt709:in_range=auto:out_range=tv' in vf
    assert 'color_trc=unknown:color_primaries=unknown' in vf
    native = validation.normalize_reference(info([unknown()]), mixed=False)
    assert native['rendering']['mode'] == 'preserve-unknown'
    assert native['video_evidence']['shapes'] == [[1600, 1280, 'yuv420p', 'unknown']]
    assert native['video_evidence']['colors'] == [['unknown'] * 3]
    assert 'scale=' not in validation.video_filter(native, mixed=False)


def test_policy_upgrade_retries_only_input_limitations(tmp_path):
    source = tmp_path / 'source.mkv'
    source.write_bytes(b'not-media')
    item = {'source': str(source), 'source_stat': archive.file_stat(source)}
    saved = {**item, 'status': 'skipped', 'reason': 'source_video_parameters', 'policy': policy.ARCHIVE_POLICY,
             'codec_policy': validation.POLICY, 'parameter_policy': archive.POLICY,
             'input_video_policy': 'decoded-video-parameters-v1', 'dependencies': policy.dependency_fingerprint(source)}
    assert not policy.reusable_skip(saved, item)
    saved['input_video_policy'] = validation.INPUT_VIDEO_POLICY
    assert policy.reusable_skip(saved, item)
    unrelated = {**item, 'status': 'skipped', 'reason': 'low_bitrate', 'policy': policy.ARCHIVE_POLICY}
    assert policy.reusable_skip(unrelated, item)


def test_output_range_change_rejected_before_audio_verification():
    reference = validation.normalize_reference(info([unknown()]), mixed=False)
    output = copy.deepcopy(reference)
    output['video_evidence']['ranges'] = ['pc']
    with pytest.raises(ValueError, match='色阶'):
        validation.verify_evidence(reference, output, None, lambda *_: pytest.fail('reached audio'))


def run(args):
    return subprocess.run(args, check=True, capture_output=True, timeout=30)


def rgb(path, matrix=None):
    color = f':in_color_matrix={matrix}' if matrix else ''
    vf = (f'scale=320:240:force_original_aspect_ratio=decrease{color},'
          'pad=320:240:(ow-iw)/2:(oh-ih)/2,format=rgb24')
    result = run(['ffmpeg', '-v', 'error', '-i', str(path), '-frames:v', '1', '-vf', vf,
                  '-f', 'rawvideo', '-pix_fmt', 'rgb24', 'pipe:1'])
    assert not result.stderr
    assert len(result.stdout) == 320 * 240 * 3
    return np.frombuffer(result.stdout, np.uint8).astype(float)


@pytest.mark.skipif(sys.platform != 'darwin', reason='真实VideoToolbox硬件像素/标签验收')
@pytest.mark.parametrize('declared', [False, True])
@pytest.mark.parametrize('full_range', [False, True])
@pytest.mark.parametrize('mixed', [False, True])
def test_hardware_reference_render_preserves_color_ramps_and_range(tmp_path, declared, full_range, mixed):
    """真实渐变色阶控制，不以改标签或mock成功替代像素语义验收。"""
    source, target = tmp_path / 'source.mkv', tmp_path / 'output.mp4'
    y = '255*X/W' if full_range else '16+219*X/W'
    expression = f"nullsrc=s=320x256:r=10,geq=lum='{y}':cb='32+192*Y/H':cr='224-192*X/W'"
    tags = ['-color_range', 'pc' if full_range else 'tv']
    source_filter = 'setsar=1'
    if declared:
        tags += ['-colorspace', 'bt709', '-color_trc', 'bt709', '-color_primaries', 'bt709']
        source_filter += ',setparams=colorspace=bt709:color_trc=bt709:color_primaries=bt709'
    run(['ffmpeg', '-v', 'error', '-n', '-f', 'lavfi', '-i', expression, '-t', '0.6', '-vf', source_filter,
         '-c:v', 'libx264', '-qp', '0', '-bf', '0', '-pix_fmt', 'yuv420p', *tags, str(source)])
    decoded = archive.probe(source, frames=True)
    assert decoded['video_evidence']['colors'] == [['bt709'] * 3 if declared else ['unknown'] * 3]
    if full_range:
        # 本次不扩展full-range/yuvj支持，验证拒绝而不是套limited标签。
        assert decoded['video_evidence']['ranges'] == ['pc']
        with pytest.raises(validation.InputVideoParametersError, match='色阶'):
            validation.normalize_reference(decoded, mixed=mixed)
        return
    assert np.array_equal(rgb(source), rgb(source, 'bt709' if declared else 'bt601'))
    reference = validation.normalize_reference(decoded, mixed=mixed)
    result = run(['ffmpeg', '-v', 'error', '-xerror', '-n', '-reinit_filter', '0', '-i', str(source),
                  '-noautoscale', '-vf', validation.video_filter(reference, mixed=mixed),
                  '-c:v', 'hevc_videotoolbox', '-allow_sw', '0', '-require_sw', '0', '-b:v', '2800k',
                  '-pix_fmt', 'yuv420p', *validation.video_tags(reference), '-tag:v', 'hvc1', str(target)])
    assert not result.stderr
    after = archive.probe(target, frames=True)['video_evidence']
    assert after['colors'] == reference['video_evidence']['colors']
    assert after['ranges'] == ['tv']
    assert after['shapes'] == reference['video_evidence']['shapes']
    error = float(np.mean(np.abs(rgb(source) - rgb(target))))
    assert error < 3, json.dumps({'rgb_mae': error, 'declared': declared, 'full_range': full_range})


@pytest.mark.skipif(sys.platform != 'darwin', reason='已取证的本机MP4非零起点样本边界')
def test_positive_aac_start_keeps_payload_pts_samples_and_trim(tmp_path):
    source, target = tmp_path / 'source.mka', tmp_path / 'output.mp4'
    seed = tmp_path / 'seed.aac'
    run(['ffmpeg', '-v', 'error', '-n', '-f', 'lavfi', '-i', 'sine=frequency=440:sample_rate=48000:duration=0.3',
         '-c:a', 'aac', '-f', 'adts', str(seed)])
    # ADTS无gapless裁剪声明；复现实际录制的无trim正起点，而非清掉合法padding。
    run(['ffmpeg', '-v', 'error', '-n', '-i', str(seed), '-c:a', 'copy', '-output_ts_offset', '0.021', str(source)])
    before = validation.audio_evidence(source, archive.command)
    assert validation.unpack(before['pts_us'])[0] > 0
    assert all(trim == [0, 0] for trim in validation.unpack(before['trims']))
    options = validation.audio_mux_options(before)
    assert options
    run(['ffmpeg', '-v', 'error', '-n', '-copyts', '-i', str(source), '-c:a', 'copy', '-copyinkf:a',
         *validation.audio_tail_filter(before), '-avoid_negative_ts', 'disabled', *options, str(target)])
    after = validation.audio_evidence(target, archive.command)
    for key in ('packets', 'payload_sha256', 'config', 'decoded_samples', 'trims'):
        assert before[key] == after[key], key
    assert max(abs(a - b) for a, b in zip(validation.unpack(before['pts_us']), validation.unpack(after['pts_us']))) <= 1000
    trimmed = copy.deepcopy(before)
    trimmed['trims'] = validation.pack([[10, 0], *validation.unpack(before['trims'])[1:]])
    assert not validation.audio_mux_options(trimmed)
