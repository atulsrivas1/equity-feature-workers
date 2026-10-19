"""Synthetic native-accounting controls only; no performance evidence."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from supplied_native import counters


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--job', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--scenario', choices=('simple', 'children', 'orphan', 'fail', 'leaf'), required=True)
    parser.add_argument('--milliseconds', type=int, default=0)
    args = parser.parse_args()
    before = counters(args.job)
    start = time.process_time_ns()
    children = []
    if args.scenario in ('children', 'orphan'):
        for index, milliseconds in enumerate((40, 60, 0) if args.scenario == 'children' else (5000,)):
            output = args.output.with_name(args.output.stem + f'-child{index}.json')
            child = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '--job', args.job,
                '--output', str(output), '--scenario', 'leaf', '--milliseconds', str(milliseconds)],
                creationflags=subprocess.CREATE_NO_WINDOW)
            children.append((child, output))
        if args.scenario == 'children':
            for child, _ in children:
                if child.wait(timeout=30):
                    raise RuntimeError('synthetic child failed')
    elif args.scenario == 'leaf':
        deadline = time.process_time() + args.milliseconds / 1000
        number = 0
        while time.process_time() < deadline:
            number = (number * 17 + 3) % 65537
    else:
        time.sleep(0.05)
    after = counters(args.job)
    payload = {'pid': os.getpid(), 'scenario': args.scenario, 'before': before, 'after': after,
        'process_cpu_total_ns': time.process_time_ns(), 'phase_process_cpu_ns': time.process_time_ns() - start,
        'children': [child.pid for child, _ in children]}
    args.output.write_text(json.dumps(payload), encoding='utf-8')
    return 17 if args.scenario == 'fail' else 0


if __name__ == '__main__':
    raise SystemExit(main())
