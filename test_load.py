import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent / "src"))

from load import load_series, load_metadata

CODE = "i924pc@ifs"

s = load_series(CODE)
meta = load_metadata([CODE])

print(f"Loaded {CODE}: {meta.loc[CODE, 'descriptor']}")
print(f"  {len(s)} observations, {s.index.min().date()} to {s.index.max().date()}")
print()
print(s.tail(10))
