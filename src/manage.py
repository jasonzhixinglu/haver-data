import argparse
import yaml
import pandas as pd
import Haver as hv
from pathlib import Path

REPO_ROOT = Path(__file__).parents[1]
CONFIG = REPO_ROOT / "config" / "series.yaml"

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

def cmd_add(args):
    config = load_config()
    existing = [s['code'] for s in config['series']]
    
    if args.code in existing:
        print(f"Already tracked: {args.code}")
        return
    
    entry = {'code': args.code, 'frequency': args.frequency}
    if args.tags:
        entry['tags'] = args.tags
    
    config['series'].append(entry)
    save_config(config)
    print(f"Added: {args.code} ({args.frequency}){' tags: ' + str(args.tags) if args.tags else ''}")

def cmd_remove(args):
    config = load_config()
    before = len(config['series'])
    config['series'] = [s for s in config['series'] if s['code'] != args.code]
    after = len(config['series'])
    
    if before == after:
        print(f"Not found: {args.code}")
        return
    
    save_config(config)
    print(f"Removed: {args.code}")

def cmd_list(args):
    config = load_config()
    codes = [s['code'] for s in config['series']]
    
    try:
        meta = pd.read_parquet(REPO_ROOT / "data" / "metadata.parquet")
        for s in config['series']:
            code = s['code']
            freq = s['frequency']
            if code in meta.index:
                desc = meta.loc[code, 'descriptor']
            else:
                desc = '(not yet pulled)'
            print(f"{code:<35} {freq:<12} {desc}")
    except Exception:
        # fallback if no metadata yet
        for s in config['series']:
            print(f"{s['code']:<35} {s['frequency']}")
    
    print(f"\nTotal: {len(config['series'])} series")

def cmd_search(args):
    print(f"Searching '{args.keyword}' in database '{args.database}'...")
    try:
        hv.path('r')  # restore default path
    except Exception:
        pass
    
    meta = hv.metadata(database=args.database)
    matches = meta[meta['descriptor'].str.contains(args.keyword, case=False, na=False)]
    
    if len(matches) == 0:
        print("No matches found.")
        return
    
    config = load_config()
    tracked = [s['code'] for s in config['series']]
    
    for _, row in matches.iterrows():
        code = f"{row['code']}@{args.database}"
        status = "TRACKED" if code in tracked else "      "
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
    p_add.set_defaults(func=cmd_add)
    p_add.add_argument('--tags', nargs='+', help='Optional tags e.g. --tags monitoring gdp_nowcast')

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

    args = parser.parse_args()
    if args.command is None:
        parser.print_help()
    else:
        args.func(args)

if __name__ == '__main__':
    main()