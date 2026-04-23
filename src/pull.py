import re
import Haver as hv
import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime

REPO_ROOT = Path(__file__).parents[1]
CONFIG = REPO_ROOT / "config" / "series.yaml"
DATA_OUT = REPO_ROOT / "data" / "data.parquet"
META_OUT = REPO_ROOT / "data" / "metadata.parquet"
LOG_FILE = REPO_ROOT / "logs" / "pull.log"

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

def load_config():
    with open(CONFIG) as f:
        return yaml.safe_load(f)

def convert_code(code):
    # CODE@DATABASE -> DATABASE:CODE (Haver native format)
    c, db = code.split('@')
    return f"{db}:{c}"

# ---------------------------------------------------------------------------
# Auto-tag derivation (Tier 1) — computed from Haver metadata at pull time
# ---------------------------------------------------------------------------

# Country lookup: (database, code_prefix) -> country tag.
# Code prefixes use the numeric country code embedded in Haver codes.
_COUNTRY_MAP = {
    # database-level defaults (when prefix matching isn't needed)
    ('japan',     None): 'japan',
    ('usecon',    None): 'us',
    ('uk',        None): 'uk',
    # emergepr: country identified by numeric prefix in the code
    ('emergepr',  '924'): 'china',
    ('emergepr',  '213'): 'brazil',
    ('emergepr',  '273'): 'mexico',
    ('emergepr',  '534'): 'india',
    ('emergepr',  '536'): 'indonesia',
    ('emergepr',  '542'): 'korea',
    ('emergepr',  '922'): 'russia',
    ('emergepr',  '186'): 'turkey',
    ('emergepr',  '199'): 'argentina',
    ('emergepr',  '193'): 'canada',
    ('emergepr',  '223'): 'colombia',
    # g10: country identified by numeric prefix
    ('g10',       '111'): 'us',
    ('g10',       '112'): 'uk',
    ('g10',       '132'): 'belgium',
    ('g10',       '134'): 'austria',
    ('g10',       '136'): 'netherlands',
    ('g10',       '156'): 'canada',
    ('g10',       '158'): 'japan',
    ('g10',       '184'): 'denmark',
    ('g10',       '193'): 'canada',
    ('g10',       '196'): 'sweden',
    # mktpmi: identified by numeric prefix
    ('mktpmi',    '924'): 'china',
    ('mktpmi',    '158'): 'japan',
    ('mktpmi',    '111'): 'us',
    ('mktpmi',    '112'): 'uk',
    ('mktpmi',    '273'): 'mexico',
    ('mktpmi',    '534'): 'india',
    ('mktpmi',    '542'): 'korea',
    # emergela
    ('emergela',  '213'): 'brazil',
    ('emergela',  '273'): 'mexico',
    ('emergela',  '223'): 'colombia',
    ('emergela',  '193'): 'canada',
    # emergecw
    ('emergecw',  '922'): 'russia',
    # emergema
    ('emergema',  '186'): 'turkey',
    ('emergema',  '199'): 'argentina',
}

def _country_tag(code: str, db: str) -> str | None:
    db = db.lower()
    # Try database-level default first
    if (_db_tag := _COUNTRY_MAP.get((db, None))):
        return _db_tag
    # Extract leading digits from code (strip leading letters)
    digits = re.match(r'[A-Za-z]*(\d+)', code)
    if digits:
        prefix = digits.group(1)[:3]
        if (tag := _COUNTRY_MAP.get((db, prefix))):
            return tag
    return None

def _transformation_tags(descriptor: str) -> list[str]:
    """Parse transformation and SA status from a Haver descriptor string."""
    tags = []
    d = descriptor.upper()

    # Transformation — check in order of specificity
    if re.search(r'Y/Y|YOY|YEAR.OVER.YEAR|ANNUAL.*%|%.*ANNUAL', d):
        tags.append('yoy')
    elif re.search(r'M/M|MOM|MONTH.OVER.MONTH', d):
        tags.append('mom')
    elif re.search(r'Q/Q|QOQ|QUARTER.OVER.QUARTER', d):
        tags.append('qoq')
    elif re.search(r'YTD|YEAR.TO.DATE|CUMULATIVE', d):
        tags.append('ytd')
    elif re.search(r'\d{4}=100|INDEX(?!\s+OF\s+(?:LEADING|COINCIDENT))', d):
        tags.append('index')
    else:
        tags.append('level')

    # Seasonal adjustment — check NSA before SA to avoid substring match
    if 'NSA' in d or 'NOT SEASONALLY ADJUSTED' in d or 'NOT SA' in d:
        tags.append('nsa')
    elif re.search(r'\bSA\b|SEASONALLY ADJUSTED|SEAS\.? ADJ', d):
        tags.append('sa')

    return tags

def _freq_tag(freq_char: str) -> str:
    return {'M': 'monthly', 'Q': 'quarterly', 'A': 'annual', 'D': 'daily'}.get(freq_char, freq_char.lower())

def _auto_tags(code: str, db: str, freq_char: str, descriptor: str) -> list[str]:
    tags = [_freq_tag(freq_char)]
    if country := _country_tag(code, db):
        tags.append(country)
    tags.extend(_transformation_tags(descriptor))
    return tags

def pull_all():
    config = load_config()
    startdate = config['defaults']['startdate']
    series = config['series']

    # build tags lookup
    tags_map = {s['code']: s.get('tags', []) for s in series}

    # group by (database, frequency)
    by_db_freq = {}
    for s in series:
        code, db = s['code'].split('@')
        freq = s['frequency'][0].upper()  # M, Q, A, D
        key = (db, freq)
        by_db_freq.setdefault(key, []).append(code)

    all_data = []
    all_meta = []

    BATCH_SIZE = 50  # Haver API limit per request

    for (db, freq), codes in by_db_freq.items():
        batches = [codes[i:i + BATCH_SIZE] for i in range(0, len(codes), BATCH_SIZE)]
        log(f"Pulling {len(codes)} {freq} series from {db} in {len(batches)} batch(es)")
        for batch in batches:
            try:
                data, metadata, info = hv.data(
                    batch,
                    database=db,
                    frequency=freq,
                    startdate=startdate,
                    rtype='3tuple'
                )

                # check for missing series
                noobs = info['codelists'].get('noobs', []) if info else []
                if noobs:
                    log(f"WARNING: no observations returned for {noobs}")

                # guard: Haver returns a dict (not DataFrame) when no codes exist
                if not isinstance(data, pd.DataFrame):
                    log(f"WARNING: {db} {freq} batch returned no data — codes may not exist in Haver: {batch}")
                    continue

                # melt to long format and tag with code@database
                data_long = data.reset_index().melt(
                    id_vars='index',
                    var_name='code',
                    value_name='value'
                ).rename(columns={'index': 'date'})
                data_long['code'] = data_long['code'] + '@' + db
                data_long['frequency'] = freq
                data_long['date'] = data_long['date'].dt.to_timestamp()
                all_data.append(data_long)

                # tag metadata with code@database as index
                metadata['id'] = metadata['code'] + '@' + metadata['database']
                metadata = metadata.set_index('id')

                # merge manual tags (series.yaml) with auto-derived tags (Tier 1)
                def merged_tags(row):
                    full_id = row.name  # code@database
                    manual = tags_map.get(full_id, [])
                    auto = _auto_tags(
                        code=row.get('code', ''),
                        db=db,
                        freq_char=freq,
                        descriptor=str(row.get('descriptor', '')),
                    )
                    return sorted(set(manual) | set(auto))

                metadata['tags'] = metadata.apply(merged_tags, axis=1)
                all_meta.append(metadata)

                log(f"OK: {db} {freq} batch of {len(batch)}")

            except Exception as e:
                log(f"ERROR: {db} {freq} batch {batch[:3]}... — {e}")

    if all_data:
        df_data = pd.concat(all_data, ignore_index=True)
        df_meta = pd.concat(all_meta)
        df_data.to_parquet(DATA_OUT)
        df_meta.to_parquet(META_OUT)
        log(f"Written to {DATA_OUT} and {META_OUT}")
    else:
        log("ERROR: no data pulled, nothing written")

if __name__ == "__main__":
    log("=== Pull started ===")
    pull_all()
    log("=== Pull complete ===")