"""不可变发布包、组件登记和固定任务版本；不管理进程或调用部署API。"""
import argparse
import fcntl
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

SCHEMA = 1
COMPONENTS = ('capture', 'sessions', 'crm', 'archive', 'presentation')
KEY = re.compile(r'[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}\Z')


class MaintenancePaused(RuntimeError):
    """维护控制流，不属于业务失败。"""


def digest(data):
    return hashlib.sha256(data).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()


def read(path):
    return json.loads(Path(path).read_text())


def atomic(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix='.' + path.name, dir=path.parent)
    try:
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(encoded(value) + b'\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


@contextmanager
def locked(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a') as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        yield


def source_manifest(root):
    root = Path(root).resolve()
    paths = []
    for directory in ('src', 'scripts', 'tests', 'skills', 'docs', 'frontend'):
        if (root / directory).is_dir():
            paths.extend((root / directory).rglob('*'))
    paths.extend(root / name for name in ('pyproject.toml', 'README.md', 'AGENTS.md') if (root / name).exists())
    files = {}
    for path in sorted(paths):
        relative = path.relative_to(root)
        if any(part in ('node_modules', '__pycache__', '.pytest_cache', '.ruff_cache') for part in relative.parts):
            continue
        if path.is_symlink():
            raise ValueError('发布输入不接受符号链接：' + str(relative))
        if path.is_file():
            files[str(relative)] = digest(path.read_bytes())
    if 'src/bilibili_live_monitor/__init__.py' not in files or 'frontend/dist/index.html' not in files:
        raise ValueError('发布输入须包含完整源码和已构建前端')
    return files


def build(source, releases, *, allow_uncommitted=False):
    source, releases = Path(source).resolve(), Path(releases).resolve()
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=source, text=True).strip()
    dirty = subprocess.check_output(['git', 'status', '--porcelain'], cwd=source, text=True)
    if dirty and not allow_uncommitted:
        raise ValueError('正式发布必须来自干净提交；未提交候选须显式使用--allow-uncommitted')
    files = source_manifest(source)
    identity = digest(encoded({'base_commit': commit, 'files': files}))
    release_id = ('candidate-' if dirty else 'release-') + identity[:20]
    releases.mkdir(parents=True, exist_ok=True)
    destination = releases / release_id
    with locked(releases / '.build.lock'):
        if destination.exists():
            existing = verify(destination)
            if existing['source_identity'] != identity:
                raise ValueError('发布目录身份冲突')
            return existing
        with tempfile.TemporaryDirectory(prefix='.building-', dir=releases) as temporary:
            staging = Path(temporary) / release_id
            staging.mkdir()
            for name, expected in files.items():
                path = source / name
                data = path.read_bytes()
                if digest(data) != expected:
                    raise ValueError('构建期间输入发生变化：' + name)
                target = staging / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)
                target.chmod(0o555 if os.access(path, os.X_OK) else 0o444)
            metadata = {'schema': SCHEMA, 'release_id': release_id, 'base_commit': commit,
                        'source_identity': identity, 'files': files, 'clean_commit': not bool(dirty),
                        'status': 'candidate' if dirty else 'built', 'created_at': time.time()}
            atomic(staging / 'release.json', metadata)
            (staging / 'release.json').chmod(0o444)
            if source_manifest(source) != files:
                raise ValueError('构建期间输入清单发生变化')
            staging.rename(destination)
    return verify(destination)


def verify(release):
    release = Path(release).resolve()
    metadata = read(release / 'release.json')
    if metadata.get('schema') != SCHEMA or metadata.get('release_id') != release.name:
        raise ValueError('发布身份无效')
    files = source_manifest(release)
    identity = digest(encoded({'base_commit': metadata['base_commit'], 'files': files}))
    if files != metadata['files'] or identity != metadata['source_identity']:
        raise ValueError('发布包文件已改变')
    return {**metadata, 'path': str(release)}


def environment(python):
    python = str(Path(python).absolute())  # 不resolve：保留虚拟环境解释器身份。
    code = ('import importlib.metadata as m,json,sys;print(json.dumps({"python":sys.version,'
            '"packages":sorted((d.metadata["Name"].lower(),d.version) for d in m.distributions())}))')
    env = {k: v for k, v in os.environ.items() if k not in ('PYTHONPATH', 'PYTHONHOME')}
    data = json.loads(subprocess.check_output([python, '-I', '-c', code], env=env, text=True))
    return {'python': python, 'identity': digest(encoded(data)), 'details': data}


def register(registry, component, release, python, *, expected_revision=None, allow_candidate=False):
    if not KEY.fullmatch(component):
        raise ValueError('组件名称无效')
    package = verify(release)
    if not package['clean_commit'] and not allow_candidate:
        raise ValueError('候选包只能显式登记至隔离演练注册表')
    runtime = environment(python)
    with locked(str(registry) + '.lock'):
        state = read(registry) if Path(registry).exists() else {'schema': SCHEMA, 'revision': 0, 'components': {}}
        if expected_revision is not None and state['revision'] != expected_revision:
            raise ValueError('注册表已变化，请重新读取')
        previous = state['components'].get(component)
        binding = {'release': package['path'], 'release_id': package['release_id'],
                   'source_identity': package['source_identity'], 'runtime': runtime}
        state['components'][component] = binding
        state['revision'] += 1
        state.setdefault('history', []).append({'component': component, 'before': previous,
                                               'after': binding, 'at': time.time()})
        atomic(registry, state)
    return state


def select(registry, component, task=None):
    if task and Path(task).exists():
        pin = read(task)
        if pin['component'] != component or pin.get('schema') != SCHEMA:
            raise ValueError('任务版本标记不匹配')
        binding = pin['binding']
    else:
        binding = read(registry)['components'][component]
    validate_binding(binding)
    return binding


def validate_binding(binding):
    package = verify(binding['release'])
    if package['source_identity'] != binding['source_identity']:
        raise ValueError('登记的源码身份不符')
    if environment(binding['runtime']['python'])['identity'] != binding['runtime']['identity']:
        raise ValueError('解释器依赖已变化')
    return binding


def pin(registry, component, task):
    with locked(str(task) + '.lock'):
        if Path(task).exists():
            return select(registry, component, task)
        binding = select(registry, component)
        atomic(task, {'schema': SCHEMA, 'component': component, 'binding': binding, 'created_at': time.time()})
        return binding


def paused(component, *, new_work=False):
    path = os.environ.get('BILI_DEPLOYMENT_GUARD')
    if not path:
        return False
    state = read(path)  # 配置过的守卫丢失或损坏不能静默放行。
    mode = state.get('components', {}).get(component, state.get('mode', 'running'))
    if mode not in ('running', 'drain', 'paused'):
        raise ValueError('维护守卫模式无效')
    return mode == 'paused' or new_work and mode == 'drain'


def require_running(component, *, new_work=False):
    if paused(component, new_work=new_work):
        raise MaintenancePaused('deployment_maintenance_paused:' + component)


def child_environment():
    """Launcher子任务显式继承固定源码和守卫，不依赖Launcher自身环境。"""
    keys = ('BILI_RELEASE_ID', 'BILI_RELEASE_ROOT', 'BILI_DEPLOYMENT_GUARD',
            'BILI_DEPLOYMENT_COMPONENT', 'BILI_TASK_PIN', 'BILI_DEPLOYMENT_REGISTRY')
    values = {key: os.environ[key] for key in keys if key in os.environ}
    if os.environ.get('BILI_RELEASE_ROOT'):
        values['PYTHONPATH'] = str(Path(os.environ['BILI_RELEASE_ROOT']) / 'src')
        values['PYTHONSAFEPATH'] = '1'
        values['PYTHONDONTWRITEBYTECODE'] = '1'
    return values


def check_task(config, component='sessions'):
    """已有任务标记是强约束；旧任务无标记时须由迁移流程先登记。"""
    marker = (Path(os.environ['BILI_TASK_PIN']) if os.environ.get('BILI_TASK_PIN')
              else Path(config['workspace']) / 'deployment.json')
    if not marker.exists():
        if os.environ.get('BILI_RELEASE_ROOT'):
            raise RuntimeError('任务尚未登记固定版本：' + str(marker))
        return
    saved = read(marker)
    if saved['component'] != component:
        raise RuntimeError('任务组件不匹配')
    binding = validate_binding(saved['binding'])
    actual = str(Path(__file__).resolve().parents[2])
    if actual != binding['release'] or sys.executable != binding['runtime']['python']:
        raise RuntimeError('任务固定源码或解释器不匹配')


def pin_current(workspace, *, new_task=False):
    """新场次从组件注册表取版本，历史任务不得隐式迁移。"""
    path = Path(workspace) / 'deployment.json'
    registry = os.environ.get('BILI_DEPLOYMENT_REGISTRY')
    if path.exists():
        return
    if not registry and not os.environ.get('BILI_RELEASE_ROOT'):
        return
    if not new_task or not registry:
        raise RuntimeError('历史任务须显式登记固定版本')
    require_running('sessions', new_work=True)
    pin(registry, 'sessions', path)


def task_launch(workspace, module, *, marker=None):
    marker = Path(marker) if marker else Path(workspace) / 'deployment.json'
    env = child_environment()
    python = sys.executable
    if marker.exists():
        saved = read(marker)
        binding = validate_binding(saved['binding'])
        python = binding['runtime']['python']
        env.update(PYTHONPATH=str(Path(binding['release']) / 'src'),
                    BILI_RELEASE_ROOT=binding['release'], BILI_RELEASE_ID=binding['release_id'],
                    BILI_TASK_PIN=str(marker), BILI_DEPLOYMENT_COMPONENT=saved['component'],
                    PYTHONSAFEPATH='1', PYTHONDONTWRITEBYTECODE='1')
    return [python, '-m', module], env


def task_command(config, module, *, component='sessions'):
    marker = (Path(config['deployment_pin']) if config.get('deployment_pin')
              else Path(config['workspace']) / 'deployment.json')
    command, env = task_launch(config['workspace'], module, marker=marker)
    env.setdefault('PYTHONPATH', str(Path(__file__).resolve().parents[1]))
    env['PYTHONSAFEPATH'] = '1'
    env['BILI_DEPLOYMENT_COMPONENT'] = component
    return shlex.join(['env', *(f'{key}={value}' for key, value in sorted(env.items())), *command])


def component_call(component, marker, module, function, arguments, *, new_task=False):
    """组件不同版时在固定解释器中运行一次有限调用，返回更新后的参数。"""
    registry = os.environ.get('BILI_DEPLOYMENT_REGISTRY')
    if not registry:
        return None
    require_running(component.split('-')[0])
    marker = Path(marker)
    if not marker.exists():
        if not new_task:
            raise RuntimeError('历史任务须显式登记固定版本')
        require_running(component.split('-')[0], new_work=True)
        pin(registry, component, marker)
    binding = select(registry, component, marker)
    if (binding['release'] == str(Path(__file__).resolve().parents[2])
            and binding['runtime']['python'] == sys.executable):
        return None
    _, environment_values = task_launch(marker.parent, module, marker=marker)
    code = ('import importlib,json,sys\n'
            'from bilibili_live_monitor.deployment import MaintenancePaused\n'
            'from bilibili_live_monitor.session_budget import BudgetPaused\n'
            'a=json.load(sys.stdin)\n'
            'try:\n'
            f' r=getattr(importlib.import_module({module!r}),{function!r})(*a)\n'
            ' print(json.dumps({"result":r,"arguments":a}))\n'
            'except MaintenancePaused:\n'
            ' print(json.dumps({"maintenance_paused":True,"arguments":a}))\n'
            'except BudgetPaused:\n'
            ' print(json.dumps({"budget_paused":True,"arguments":a}))\n')
    output = subprocess.check_output([binding['runtime']['python'], '-P', '-c', code],
        input=json.dumps(arguments), text=True, env={**os.environ, **environment_values})
    result = json.loads(output.splitlines()[-1])
    if result.get('maintenance_paused'):
        raise MaintenancePaused('deployment_maintenance_paused:' + component)
    if result.get('budget_paused'):
        from .session_budget import BudgetPaused
        for original, updated in zip(arguments, result['arguments']):
            if isinstance(original, dict) and isinstance(updated, dict):
                original.clear()
                original.update(updated)
        raise BudgetPaused('跨组件任务预算暂停')
    return result


def prepare_run(registry, component, arguments, *, task=None):
    require_running(component, new_work=not (task and Path(task).exists()))
    binding = pin(registry, component, task) if task else select(registry, component)
    env = dict(os.environ)
    env.pop('PYTHONHOME', None)
    env['PYTHONPATH'] = str(Path(binding['release']) / 'src')
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    env['BILI_RELEASE_ID'] = binding['release_id']
    env['BILI_RELEASE_ROOT'] = binding['release']
    env['BILI_DEPLOYMENT_COMPONENT'] = component
    env['BILI_DEPLOYMENT_REGISTRY'] = str(Path(registry).resolve())
    if task:
        env['BILI_TASK_PIN'] = str(Path(task).absolute())
    # -P禁止cwd覆盖PYTHONPATH；业务sys.executable仍为登记的字面解释器路径。
    argv = [binding['runtime']['python'], '-P', *arguments]
    return binding, argv, env


def run(registry, component, arguments, *, task=None, dry_run=False):
    if not arguments:
        raise ValueError('缺少业务命令')
    if dry_run:
        require_running(component, new_work=not (task and Path(task).exists()))
        binding = select(registry, component, task)
        return {'binding': binding, 'argv': [binding['runtime']['python'], '-P', *arguments],
                'source': str(Path(binding['release']) / 'src'), 'executed': False}
    binding, argv, env = prepare_run(registry, component, arguments, task=task)
    intent = Path(registry).parent / 'launches' / f'{time.time_ns()}-{os.getpid()}.json'
    atomic(intent, {'component': component, 'binding': binding, 'pid': os.getpid(),
                    'argv': argv, 'at': time.time(), 'status': 'launch_intent_not_observed'})
    os.execve(argv[0], argv, env)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    builder = sub.add_parser('build')
    builder.add_argument('--source', required=True, type=Path)
    builder.add_argument('--releases', required=True, type=Path)
    builder.add_argument('--allow-uncommitted', action='store_true')
    check = sub.add_parser('verify')
    check.add_argument('release', type=Path)
    registration = sub.add_parser('register')
    registration.add_argument('--release', required=True, type=Path)
    registration.add_argument('--python', required=True)
    registration.add_argument('--expected-revision', type=int)
    registration.add_argument('--allow-candidate', action='store_true')
    for name in ('run', 'pin'):
        command = sub.add_parser(name)
        command.add_argument('--task', type=Path, required=name == 'pin')
        if name == 'run':
            command.add_argument('--dry-run', action='store_true')
            command.add_argument('arguments', nargs=argparse.REMAINDER)
        command.add_argument('--registry', required=True, type=Path)
        command.add_argument('--component', required=True)
    registration.add_argument('--registry', required=True, type=Path)
    registration.add_argument('--component', required=True)
    args = vars(parser.parse_args())
    action = args.pop('action')
    if action == 'run' and args['arguments'][:1] == ['--']:
        args['arguments'] = args['arguments'][1:]
    result = {'build': build, 'verify': verify, 'register': register, 'run': run, 'pin': pin}[action](**args)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
