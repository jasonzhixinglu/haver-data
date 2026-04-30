import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent / "src"))

from load import load_metadata, available_tags
from collections import Counter

meta = load_metadata()
tag_counts = Counter()
for tags in meta["tags"]:
    for t in tags:
        tag_counts[t] += 1

print(f"Total series in metadata: {len(meta)}")
print()
print("Tag counts (top 20):")
for tag, count in tag_counts.most_common(20):
    print(f"  {tag:<20} {count}")
