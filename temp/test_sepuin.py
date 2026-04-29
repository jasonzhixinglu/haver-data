"""
Simple test: pull SEPUIN@DAILY and confirm we get data back.
Writes result to temp/test_sepuin.log and pushes to GitHub.
"""

import Haver as hv
import subprocess
import sys
from pathlib import Path
from datetime import datetime

REPO_ROOT = Path(__file__).parents[1]
LOG_PATH = REPO_ROOT / 'temp' / 'test_sepuin.log'

class Tee:
    def __init__(self, *streams):
        self.streams = streams
    def write(self, data):
        for s in self.streams: s.write(data)
    def flush(self):
        for s in self.streams: s.flush()

_log = open(LOG_PATH, 'w')
sys.stdout = Tee(sys.__stdout__, _log)
sys.stderr = Tee(sys.__stderr__, _log)

print(f"Test run: {datetime.now()}")
print()

STARTDATE = '2024-01-01'

# Test 1: native format (confirmed working in diagnostic)
print("--- Test 1: native format DAILY:SEPUIN ---")
try:
    data, meta, info = hv.data(['DAILY:SEPUIN'], startdate=STARTDATE, rtype='3tuple')
    print(f"data type: {type(data)}")
    if hasattr(data, 'shape'):
        print(f"shape: {data.shape}")
        print(f"columns: {list(data.columns)}")
        print(f"index sample: {list(data.index[:3])}")
        print(f"value sample:\n{data.head(3)}")
    else:
        print(f"data: {data}")
except Exception as e:
    print(f"ERROR: {e}")

print()

# Test 2: database= param approach
print("--- Test 2: database='DAILY', frequency='D' ---")
try:
    data, meta, info = hv.data(['SEPUIN'], database='DAILY', frequency='D', startdate=STARTDATE, rtype='3tuple')
    print(f"data type: {type(data)}")
    if hasattr(data, 'shape'):
        print(f"shape: {data.shape}")
        print(f"columns: {list(data.columns)}")
        print(f"value sample:\n{data.head(3)}")
    else:
        print(f"data: {data}")
except Exception as e:
    print(f"ERROR: {e}")

sys.stdout = sys.__stdout__
sys.stderr = sys.__stderr__
_log.close()

print(f"Log written to {LOG_PATH}")
try:
    subprocess.run(['git', 'add', str(LOG_PATH)], cwd=REPO_ROOT, check=True)
    subprocess.run(['git', 'commit', '-m', f'SEPUIN test log {datetime.now().strftime("%Y-%m-%d %H:%M")}'], cwd=REPO_ROOT, check=True)
    subprocess.run(['git', 'push'], cwd=REPO_ROOT, check=True)
    print("Pushed.")
except subprocess.CalledProcessError as e:
    print(f"Git error: {e}")
