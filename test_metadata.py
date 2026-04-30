import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent / "src"))

from load import load_metadata, available_series

all_series = available_series()
print(f"Total series tracked: {len(all_series)}")
print()

CODES = ["jpcij@japan", "i924pc@ifs", "fin10@daily"]

meta = load_metadata(CODES)
print(meta[["descriptor", "frequency", "shortsource", "tags"]])
