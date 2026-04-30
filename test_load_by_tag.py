import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent / "src"))

from load import load_by_tag, available_tags

print("Available tags:", available_tags())
print()

TAG = "monitoring"

df = load_by_tag(TAG, frequency="M")
print(f"Loaded tag '{TAG}': {df.shape[1]} series, {len(df)} dates ({df.index.min().date()} to {df.index.max().date()})")
print()
print(df.tail(5).iloc[:, :5])
