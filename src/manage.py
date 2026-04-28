import argparse
import yaml
import pandas as pd
import Haver as hv
from pathlib import Path
from datetime import datetime

REPO_ROOT = Path(__file__).parents[1]
CONFIG = REPO_ROOT / "config" / "series.yaml"
QUARANTINE = REPO_ROOT / "config" / "quarantine.yaml"

def load_config():
    with open(CONFIG) as f:
        return yaml.safe_load(f)

def save_config(config):
    with open(CONFIG, 'w') as f:
        f.write(f"defaults:\n")
        f.write(f"  startdate: \"{config['defaults']['startdate']}\"\n\n")
        f.write(f"series:\n")
        for s in config['series']:
            if 'tags' in s:
                tags_str = '[' + ', '.join(s['tags']) + ']'
                f.write(f"  - {{code: {s['code']}, frequency: {s['frequency']}, tags: {tags_str}}}\n")
            else:
                f.write(f"  - {{code: {s['code']}, frequency: {s['frequency']}}}\n")

def _append_series(entry: dict):
    """Append a new series block to series.yaml in block style, preserving all existing content."""
    lines = [f"- code: {entry['code']}\n",
             f"  frequency: {entry['frequency']}\n"]
    if entry.get('tags'):
        lines.append("  tags:\n")
        for tag in entry['tags']:
            lines.append(f"  - {tag}\n")
    with open(CONFIG, 'a') as f:
        f.writelines(lines)

def _remove_series(code: str) -> bool:
    """Remove a series block from series.yaml by exact code match. Returns True if found.
    Preserves all comments, formatting, and other entries."""
    with open(CONFIG) as f:
        lines = f.readlines()
    target = f'- code: {code}\n'
    result = []
    i = 0
    found = False
    while i < len(lines):
        if lines[i] == target:
            found = True
            i += 1
            # Skip all continuation lines (indented block content)
            while i < len(lines) and lines[i].startswith(' '):
                i += 1
        else:
            result.append(lines[i])
            i += 1
    if found:
        with open(CONFIG, 'w') as f:
            f.writelines(result)
    return found

def load_quarantine() -> dict:
    """Load quarantine list as a dict keyed by code@database."""
    if not QUARANTINE.exists():
        return {}
    with open(QUARANTINE) as f:
        entries = yaml.safe_load(f) or []
    return {e['code']: e for e in entries}

def save_quarantine(quarantine: dict):
    entries = sorted(quarantine.values(), key=lambda e: e['code'])
    with open(QUARANTINE, 'w') as f:
        yaml.dump(entries, f, default_flow_style=False, sort_keys=False)

def cmd_add(args):
    config = load_config()
    existing = [s['code'] for s in config['series']]

    # Check quarantine before adding
    quarantine = load_quarantine()
    if args.code in quarantine:
        q = quarantine[args.code]
        print(f"ERROR: {args.code} is quarantined and cannot be added.")
        print(f"  Reason:      {q['reason']}")
        print(f"  Quarantined: {q['quarantined']}")
        print(f"  To re-enable, run: python src/manage.py unquarantine {args.code}")
        return

    if args.code in existing:
        print(f"Already tracked: {args.code}")
        return

    entry = {'code': args.code, 'frequency': args.frequency}
    if args.tags:
        entry['tags'] = args.tags

    _append_series(entry)
    print(f"Added: {args.code} ({args.frequency}){' tags: ' + str(args.tags) if args.tags else ''}")

def cmd_remove(args):
    if not _remove_series(args.code):
        print(f"Not found: {args.code}")
        return
    print(f"Removed: {args.code}")

def cmd_list(args):
    config = load_config()
    quarantine = load_quarantine()

    try:
        meta = pd.read_parquet(REPO_ROOT / "data" / "metadata.parquet")
        for s in config['series']:
            code = s['code']
            freq = s['frequency']
            q_marker = ' [QUARANTINED]' if code in quarantine else ''
            if code in meta.index:
                desc = meta.loc[code, 'descriptor']
            else:
                desc = '(not yet pulled)'
            print(f"{code:<35} {freq:<12} {desc}{q_marker}")
    except Exception:
        for s in config['series']:
            q_marker = ' [QUARANTINED]' if s['code'] in quarantine else ''
            print(f"{s['code']:<35} {s['frequency']}{q_marker}")

    print(f"\nTotal: {len(config['series'])} series")
    if quarantine:
        print(f"Quarantined: {len(quarantine)} series (run 'manage.py quarantine-list' to view)")

def cmd_quarantine_list(args):
    """List all quarantined series."""
    quarantine = load_quarantine()
    if not quarantine:
        print("No quarantined series.")
        return
    print(f"{'Code':<35} {'Quarantined':<22} Reason")
    print("-" * 90)
    for code, q in sorted(quarantine.items()):
        print(f"{code:<35} {q['quarantined']:<22} {q['reason']}")
    print(f"\nTotal: {len(quarantine)} quarantined series")

def cmd_unquarantine(args):
    """Remove a series from quarantine so it can be re-added and pulled."""
    quarantine = load_quarantine()
    if args.code not in quarantine:
        print(f"Not quarantined: {args.code}")
        return
    del quarantine[args.code]
    save_quarantine(quarantine)
    print(f"Unquarantined: {args.code}")
    print(f"You can now re-add it with: python src/manage.py add {args.code} <frequency>")

def cmd_search(args):
    print(f"Searching '{args.keyword}' in database '{args.database}'...")
    meta = hv.metadata(database=args.database)
    matches = meta[meta['descriptor'].str.contains(args.keyword, case=False, na=False)]

    if len(matches) == 0:
        print("No matches found.")
        return

    config = load_config()
    quarantine = load_quarantine()
    tracked = [s['code'] for s in config['series']]

    for _, row in matches.iterrows():
        code = f"{row['code']}@{args.database}"
        if code in quarantine:
            status = "QUARANTINED"
        elif code in tracked:
            status = "TRACKED    "
        else:
            status = "           "
        print(f"{status}  {code:<35} {row['frequency']}  {row['descriptor']}")

    print(f"\n{len(matches)} matches found.")

def main():
    parser = argparse.ArgumentParser(
        description='Manage haver-data series coverage',
        formatter_class=argparse.RawTextHelpFormatter
    )
    subparsers = parser.add_subparsers(dest='command')

    # add
    p_add = subparsers.add_parser('add', help='Add a series to tracking')
    p_add.add_argument('code', help='Series code in code@database format e.g. jpcij@japan')
    p_add.add_argument('frequency', choices=['daily', 'monthly', 'quarterly', 'annual'], help='Frequency')
    p_add.add_argument('--tags', nargs='+', help='Optional tags e.g. --tags monitoring gdp_nowcast')
    p_add.set_defaults(func=cmd_add)

    # remove
    p_remove = subparsers.add_parser('remove', help='Remove a series from tracking')
    p_remove.add_argument('code', help='Series code in code@database format')
    p_remove.set_defaults(func=cmd_remove)

    # list
    p_list = subparsers.add_parser('list', help='List all tracked series')
    p_list.set_defaults(func=cmd_list)

    # search
    p_search = subparsers.add_parser('search', help='Search Haver metadata by keyword')
    p_search.add_argument('keyword', help='Search term e.g. "CPI"')
    p_search.add_argument('database', help='Haver database to search e.g. japan, emergepr, usecon')
    p_search.set_defaults(func=cmd_search)

    # quarantine-list
    p_qlist = subparsers.add_parser('quarantine-list', help='List all quarantined series')
    p_qlist.set_defaults(func=cmd_quarantine_list)

    # unquarantine
    p_unq = subparsers.add_parser('unquarantine', help='Remove a series from quarantine')
    p_unq.add_argument('code', help='Series code in code@database format')
    p_unq.set_defaults(func=cmd_unquarantine)

    args = parser.parse_args()
    if args.command is None:
        parser.print_help()
    else:
        args.func(args)

if __name__ == '__main__':
    main()
