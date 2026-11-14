"""稳定启动入口；仅依赖标准库，从登记的发布包加载控制实现。"""
import argparse
import json
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--registry', required=True, type=Path)
    parser.add_argument('--component', required=True)
    parser.add_argument('--task', type=Path)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('arguments', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.task and args.task.exists():
        pin = json.loads(args.task.read_text())
        if pin['component'] != args.component:
            raise ValueError('任务组件身份不符')
        binding = pin['binding']
    else:
        binding = json.loads(args.registry.read_text())['components'][args.component]
    sys.path.insert(0, str(Path(binding['release']) / 'src'))
    from bilibili_live_monitor.deployment import run
    arguments = args.arguments[1:] if args.arguments[:1] == ['--'] else args.arguments
    result = run(args.registry, args.component, arguments, task=args.task, dry_run=args.dry_run)
    if result is not None:
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
