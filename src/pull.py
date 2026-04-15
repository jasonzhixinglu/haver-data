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
        freq = s['frequency'][0].upper()  # M, Q, D
        key = (db, freq)
        by_db_freq.setdefault(key, []).append(code)

    all_data = []
    all_meta = []

    for (db, freq), codes in by_db_freq.items():
        log(f"Pulling {len(codes)} {freq} series from {db}: {codes}")
        try:
            data, metadata, info = hv.data(
                codes,
                database=db,
                frequency=freq,
                startdate=startdate,
                rtype='3tuple'
            )

            # check for missing series
            noobs = info['codelists'].get('noobs', [])
            if noobs:
                log(f"WARNING: no observations returned for {noobs}")

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
            all_meta.append(metadata)

            log(f"OK: {db} {freq} — {len(codes)} series, {len(data)} observations each")

        except Exception as e:
            log(f"ERROR: {db} {freq} — {e}")

    if all_data:
        df_data = pd.concat(all_data, ignore_index=True)
        df_meta = pd.concat(all_meta)
        
        # add tags column
        df_meta['tags'] = df_meta.index.map(lambda x: tags_map.get(x, []))
        
        df_data.to_parquet(DATA_OUT)
        df_meta.to_parquet(META_OUT)
        log(f"Written to {DATA_OUT} and {META_OUT}")
    else:
        log("ERROR: no data pulled, nothing written")

if __name__ == "__main__":
    log("=== Pull started ===")
    pull_all()
    log("=== Pull complete ===")