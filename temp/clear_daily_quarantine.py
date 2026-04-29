"""
One-shot script to remove all INTDAILY and DAILY series from quarantine.
Run this on the server before the next pull to unblock Ken's series.

Usage:
    python temp/clear_daily_quarantine.py
"""

import sys, subprocess
from pathlib import Path
from datetime import datetime

REPO_ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(REPO_ROOT / 'src'))

import yaml

QUARANTINE = REPO_ROOT / 'config' / 'quarantine.yaml'

with open(QUARANTINE) as f:
    entries = yaml.safe_load(f) or []

before = len(entries)
kept = [e for e in entries if 'intdaily' not in e['code'].lower() and e['code'].lower().split('@')[-1] != 'daily']
removed = [e['code'] for e in entries if e not in kept]

with open(QUARANTINE, 'w') as f:
    yaml.dump(kept, f, default_flow_style=False, sort_keys=False)

print(f"Removed {len(removed)} daily series from quarantine ({before - len(kept)} total removed, {len(kept)} remain)")
for code in removed:
    print(f"  {code}")

print("\nPushing updated quarantine to GitHub...")
try:
    subprocess.run(['git', 'add', str(QUARANTINE)], cwd=REPO_ROOT, check=True)
    subprocess.run(['git', 'commit', '-m', f'Clear daily series from quarantine {datetime.now().strftime("%Y-%m-%d %H:%M")}'], cwd=REPO_ROOT, check=True)
    subprocess.run(['git', 'push'], cwd=REPO_ROOT, check=True)
    print("Pushed.")
except subprocess.CalledProcessError as e:
    print(f"Git error: {e}")
