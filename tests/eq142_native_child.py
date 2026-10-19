"""Original deterministic thread-lifetime observation, never a timing workload."""
import argparse
import json
import os
from pathlib import Path
import threading
import time

parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True)
args=parser.parse_args()
stop=threading.Event()
threads=[threading.Thread(target=stop.wait) for _ in range(3)]
for thread in threads:thread.start()
args.output.write_text(json.dumps({'pid':os.getpid(),'python_threads':threading.active_count()}),encoding='utf-8')
time.sleep(0.3)
stop.set()
for thread in threads:thread.join()
