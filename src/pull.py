import re
import Haver as hv
import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime

REPO_ROOT = Path(__file__).parents[1]
CONFIG = REPO_ROOT / "config" / "series.yaml"
QUARANTINE = REPO_ROOT / "config" / "quarantine.yaml"
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

def load_quarantine() -> dict:
    """Load quarantine list as a dict keyed by code@database (always lowercase)."""
    if not QUARANTINE.exists():
        return {}
    with open(QUARANTINE) as f:
        entries = yaml.safe_load(f) or []
    return {e['code'].lower(): e for e in entries}

def save_quarantine(quarantine: dict):
    """Write quarantine dict back to quarantine.yaml."""
    entries = sorted(quarantine.values(), key=lambda e: e['code'])
    with open(QUARANTINE, 'w') as f:
        yaml.dump(entries, f, default_flow_style=False, sort_keys=False)

def quarantine_code(code: str, db: str, reason: str, quarantine: dict):
    """Add a code@database entry to the quarantine dict and persist."""
    full_id = f"{code.lower()}@{db.lower()}"
    if full_id not in quarantine:
        log(f"QUARANTINE: {full_id} — {reason}")
        quarantine[full_id] = {
            'code': full_id,
            'quarantined': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'reason': reason,
        }
        save_quarantine(quarantine)

def convert_code(code):
    # CODE@DATABASE -> DATABASE:CODE (Haver native format)
    c, db = code.split('@')
    return f"{db}:{c}"

# ---------------------------------------------------------------------------
# Auto-tag derivation (Tier 1) — computed from Haver metadata at pull time
# ---------------------------------------------------------------------------

_COUNTRY_MAP = {
    ('japan',     None): 'japan',
    ('usecon',    None): 'us',
    ('uk',        None): 'uk',
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
    ('mktpmi',    '924'): 'china',
    ('mktpmi',    '158'): 'japan',
    ('mktpmi',    '111'): 'us',
    ('mktpmi',    '112'): 'uk',
    ('mktpmi',    '273'): 'mexico',
    ('mktpmi',    '534'): 'india',
    ('mktpmi',    '542'): 'korea',
    ('emergela',  '213'): 'brazil',
    ('emergela',  '273'): 'mexico',
    ('emergela',  '223'): 'colombia',
    ('emergela',  '193'): 'canada',
    ('emergecw',  '922'): 'russia',
    ('emergema',  '186'): 'turkey',
    ('emergema',  '199'): 'argentina',
}

def _country_tag(code: str, db: str) -> str | None:
    db = db.lower()
    if (_db_tag := _COUNTRY_MAP.get((db, None))):
        return _db_tag
    digits = re.match(r'[A-Za-z]*(\d+)', code)
    if digits:
        prefix = digits.group(1)[:3]
        if (tag := _COUNTRY_MAP.get((db, prefix))):
            return tag
    return None

def _transformation_tags(descriptor: str) -> list[str]:
    tags = []
    d = descriptor.upper()
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

def _process_batch(data, metadata, db, freq, tags_map, all_data, all_meta):
    """Melt and tag a successful Haver batch response, appending to all_data/all_meta."""
    data_long = data.reset_index().melt(
        id_vars='index',
        var_name='code',
        value_name='value'
    ).rename(columns={'index': 'date'})
    data_long['code'] = data_long['code'] + '@' + db
    data_long['frequency'] = freq
    data_long['date'] = data_long['date'].dt.to_timestamp()
    all_data.append(data_long)

    metadata['id'] = metadata['code'] + '@' + metadata['database']
    metadata = metadata.set_index('id')

    def merged_tags(row):
        full_id = row.name
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

def _pull_batch(batch, db, freq, startdate):
    """Call hv.data for a batch. Returns (data, metadata, info).

    Daily series require native DB:CODE format — passing database= with
    frequency='D' fails for some databases (e.g. INTDAILY) even when codes
    exist. Native format works universally for daily.
    """
    if freq == 'D':
        native = [f"{db}:{code}" for code in batch]
        return hv.data(native, startdate=startdate, rtype='3tuple')
    return hv.data(
        batch,
        database=db,
        frequency=freq,
        startdate=startdate,
        rtype='3tuple'
    )

def _retry_and_quarantine(batch, db, freq, startdate, quarantine):
    """
    Retry a failed batch one-by-one within the same db/freq.
    Returns (good_codes, data, metadata) for codes that succeed,
    and quarantines any codes that fail.
    """
    log(f"RETRY: {db} {freq} — testing {len(batch)} codes individually")
    good_codes, good_data, good_meta = [], [], []

    for code in batch:
        try:
            d, m, info = _pull_batch([code], db, freq, startdate)
            if isinstance(d, pd.DataFrame):
                good_codes.append(code)
                good_data.append(d)
                good_meta.append(m)
            else:
                # Extract reason from error dict if available
                codelists = (info or {}).get('codelists', {})
                not_found = codelists.get('codesnotfound', [])
                reason = 'codesnotfound' if not_found else 'batch_error_no_data'
                quarantine_code(code, db, reason, quarantine)
        except Exception as e:
            quarantine_code(code, db, str(e), quarantine)

    if not good_codes:
        return None, None, None

    # Re-pull good codes together in one clean batch for efficiency
    try:
        data, metadata, info = _pull_batch(good_codes, db, freq, startdate)
        if not isinstance(data, pd.DataFrame):
            # Shouldn't happen, but fall back to pre-concatenated individual pulls
            log(f"WARNING: {db} {freq} re-pull of good codes failed, using individual results")
            data = pd.concat(good_data, axis=1)
            metadata = pd.concat(good_meta, ignore_index=True)
    except Exception as e:
        log(f"WARNING: {db} {freq} re-pull of good codes raised {e}, using individual results")
        data = pd.concat(good_data, axis=1)
        metadata = pd.concat(good_meta, ignore_index=True)

    log(f"RETRY OK: {db} {freq} — {len(good_codes)}/{len(batch)} codes recovered")
    return good_codes, data, metadata

def pull_all():
    config = load_config()
    startdate = config['defaults']['startdate']
    series = config['series']

    # Normalize all codes to lowercase so Haver's lowercase returns match config
    # keys (e.g. 'R111M3M@INTDAILY' -> 'r111m3m@intdaily'). Also fixes any
    # case typos in database names (e.g. 'INTDAILy' -> 'intdaily').
    for s in series:
        s['code'] = s['code'].lower()

    # Load quarantine and normalize its keys to lowercase for consistent lookup
    quarantine = load_quarantine()
    quarantine = {k.lower(): v for k, v in quarantine.items()}

    if quarantine:
        quarantined_tracked = [s['code'] for s in series if s['code'] in quarantine]
        if quarantined_tracked:
            for code in quarantined_tracked:
                q = quarantine[code]
                log(f"SKIPPING quarantined series: {code} "
                    f"(reason: {q['reason']}, quarantined: {q['quarantined']})")

    # Filter out quarantined series before pulling
    series = [s for s in series if s['code'] not in quarantine]

    # Load existing parquets upfront for fallback
    existing_data = pd.read_parquet(DATA_OUT) if DATA_OUT.exists() else None
    existing_meta = pd.read_parquet(META_OUT) if META_OUT.exists() else None

    tags_map = {s['code']: s.get('tags', []) for s in series}

    # Group by (database, frequency)
    by_db_freq = {}
    for s in series:
        code, db = s['code'].split('@')
        freq = s['frequency'][0].upper()
        key = (db, freq)
        by_db_freq.setdefault(key, []).append(code)

    all_data = []
    all_meta = []

    BATCH_SIZE = 50

    for (db, freq), codes in by_db_freq.items():
        batches = [codes[i:i + BATCH_SIZE] for i in range(0, len(codes), BATCH_SIZE)]
        log(f"Pulling {len(codes)} {freq} series from {db} in {len(batches)} batch(es)")
        for batch in batches:
            try:
                data, metadata, info = _pull_batch(batch, db, freq, startdate)

                noobs = info['codelists'].get('noobs', []) if info else []
                if noobs:
                    log(f"WARNING: no observations returned for {noobs}")
                    for code in noobs:
                        quarantine_code(code, db, 'noobs', quarantine)

                if not isinstance(data, pd.DataFrame):
                    # Batch failed — retry individually to isolate bad codes
                    good_codes, data, metadata = _retry_and_quarantine(
                        batch, db, freq, startdate, quarantine
                    )
                    if data is None:
                        log(f"ERROR: {db} {freq} batch fully failed, all codes quarantined")
                        continue
                    batch = good_codes
                else:
                    # Batch succeeded but some codes may be silently absent from the DataFrame
                    missing_in_df = [c for c in batch if c not in data.columns]
                    if missing_in_df:
                        log(f"WARNING: {db} {freq} — {len(missing_in_df)} codes absent from returned data: {missing_in_df}")
                        for code in missing_in_df:
                            quarantine_code(code, db, 'not_in_returned_dataframe', quarantine)
                        batch = [c for c in batch if c not in missing_in_df]
                        if not batch:
                            continue

                _process_batch(data, metadata, db, freq, tags_map, all_data, all_meta)
                log(f"OK: {db} {freq} batch of {len(batch)}")

            except Exception as e:
                log(f"ERROR: {db} {freq} batch {batch[:3]}... — {e}")

    if all_data:
        df_data = pd.concat(all_data, ignore_index=True)
        df_meta = pd.concat(all_meta)

        expected_codes = set(s['code'] for s in series)
        pulled_codes   = set(df_data['code'].unique())
        missing_codes  = expected_codes - pulled_codes

        if missing_codes:
            if existing_data is not None:
                log(f"FALLBACK: {len(missing_codes)} series absent from pull — "
                    f"carrying forward from previous snapshot: {sorted(missing_codes)}")
                prev_rows = existing_data[existing_data['code'].isin(missing_codes)]
                if not prev_rows.empty:
                    df_data = pd.concat([df_data, prev_rows], ignore_index=True)
                if existing_meta is not None:
                    prev_meta = existing_meta[existing_meta.index.isin(missing_codes)]
                    if not prev_meta.empty:
                        df_meta = pd.concat([df_meta, prev_meta])
                        df_meta = df_meta[~df_meta.index.duplicated(keep='first')]
            else:
                log(f"WARNING: {len(missing_codes)} series missing from pull and "
                    f"no existing snapshot to fall back to: {sorted(missing_codes)}")

        df_data.to_parquet(DATA_OUT)
        df_meta.to_parquet(META_OUT)
        log(f"Written to {DATA_OUT} and {META_OUT}")
    else:
        log("ERROR: no data pulled, nothing written")

if __name__ == "__main__":
    log("=== Pull started ===")
    pull_all()
    log("=== Pull complete ===")
