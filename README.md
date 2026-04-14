# Haver Data Pipeline

A lightweight data pipeline that pulls macroeconomic time series from Haver Analytics and version-controls them as Parquet files on GitHub. Designed to bridge network-restricted Haver access and a portable personal analysis environment.

---

## Infrastructure Overview

| Machine | Haver Access | GitHub Access | Notes |
|---|---|---|---|
| Work server (`EMDSWN45P`) | Yes | Yes | Runs the scheduled pull |
| Work laptop | Yes | No | Limited network access |
| Personal PC | No | Yes | Used for analysis |

The repo lives on the work server at:

```
\\EMDSWN45P\data\jlu2\haver_data\haver-data
```

Key paths on the server:

- **Python 3.11**: `D:\APPS\python\Python311\python.exe` — used by the scheduler (has Haver + pyarrow)
- **Python 3.12**: `D:\APPS\python\python312\python.exe` — used for interactive work in VS Code
- **Git**: `D:\Apps\Git\bin`
- **Launcher script**: `D:\Apps\haver_launcher.bat` — must live on a local drive (CMD cannot run scripts from UNC paths)

---

## Repo Structure

```
haver-data/
├── config/
│   └── series.yaml          # Master list of series to pull
├── data/
│   ├── data.parquet         # All time series in long format
│   └── metadata.parquet     # Haver metadata for all series
├── logs/
│   ├── pull.log             # Python pull script logs
│   └── scheduler.log        # Windows Task Scheduler logs
├── src/
│   ├── pull.py              # Pulls data from Haver, writes Parquet
│   ├── load.py              # Utility for consuming repos
│   └── manage.py            # CLI for managing series coverage
├── run_pull.bat             # Kept in repo for reference (not used by scheduler)
├── .gitignore
└── README.md
```

---

## How the Pipeline Works

1. **Windows Task Scheduler** runs `D:\Apps\haver_launcher.bat` daily at 7am
2. The launcher:
   - Maps `\\EMDSWN45P\data\jlu2\haver_data` to drive `Z:`
   - Runs `git pull` to pick up any config changes pushed from another machine
   - Runs `pull.py` which reads `series.yaml`, pulls from Haver, writes Parquet
   - Commits updated `data.parquet`, `metadata.parquet`, and `pull.log`
   - Pushes to GitHub
   - Unmaps `Z:`
3. Your **personal PC** runs `git pull` to get fresh data

---

## Data Format

### data/data.parquet

Long-format table with one row per series-date:

| column | type | description |
|---|---|---|
| `date` | datetime64 | Period start date (monthly: 1st of month, quarterly: 1st of quarter) |
| `code` | str | Series in `code@database` format e.g. `jpcij@japan` |
| `value` | float64 | Observed value in native Haver units |
| `frequency` | str | M = monthly, Q = quarterly, D = daily |

### data/metadata.parquet

Indexed by `code@database`, one row per series, 18 columns:

| column | description |
|---|---|
| `descriptor` | Full series name e.g. `Japan: CPI: All Items (2020=100)` |
| `frequency` | Native Haver frequency |
| `startdate` / `enddate` | Available history in Haver |
| `aggtype` | Aggregation type: AVG, SUM, EOP, or NDF |
| `datatype` | Data type e.g. %, INDEX, $ |
| `magnitude` | Scaling factor (0 = no scaling) |
| `geography1` | UN country code e.g. 158 = Japan, 924 = China |
| `shortsource` | Short source e.g. CNBS, MIC |
| `longsource` | Full source name |
| `database` | Haver database name |
| `code` | Raw Haver series code |

---

## Managing Series Coverage

Series are managed via `src/manage.py`, a CLI tool that adds, removes, lists, and searches series without editing `series.yaml` directly. Run all commands from the repo root on the server (requires Haver access for search).

### List all tracked series

```powershell
python src/manage.py list
```

Shows each series code, frequency, and descriptor from metadata. Series not yet pulled show `(not yet pulled)`.

### Add a series

```powershell
python src/manage.py add jpcij@japan monthly
```

Appends to `series.yaml`. Will not add duplicates. Then commit, push, and trigger a pull:

```powershell
git add config/series.yaml
git commit -m "Add [description]"
git push
```

### Remove a series

```powershell
python src/manage.py remove jpcij@japan
```

Then commit and push as above. Note: this removes the series from future pulls but does not delete historical data already in `data.parquet`.

### Search Haver for series by keyword

```powershell
python src/manage.py search CPI japan
python src/manage.py search "retail sales" emergepr
python src/manage.py search GDP usecon
```

Returns all matching series in the database with their code, frequency, and descriptor. Already-tracked series are marked `TRACKED`. Use this to discover series codes before adding them.

### series.yaml format (for reference)

```yaml
defaults:
  startdate: "1990-01-01"   # global start date for all series

series:
  - {code: jpcij@japan, frequency: monthly}
  - {code: jsngpcp@japan, frequency: quarterly}
  - {code: usfedfunds@usecon, frequency: daily}
```

The code format is always `seriescode@databasename` in lowercase.

---

## Triggering a Manual Pull

Open **Command Prompt** (not PowerShell) and run:

```
D:\Apps\haver_launcher.bat
```

To check the result:

```
type "\\EMDSWN45P\data\jlu2\haver_data\haver-data\logs\scheduler.log"
```

---

## Loading Data in a Consuming Repo

### Step 1: Clone haver-data

```bash
git clone https://github.com/jasonzhixinglu/haver-data.git
```

### Step 2: Keep data fresh

```bash
cd haver-data
git pull
```

### Step 3: Load data using load.py

```python
import sys
sys.path.append(r'path\to\haver-data\src')
from load import load_series, load_multiple, load_metadata, available_series

# list all available series
available_series()

# load a single series as pd.Series indexed by date
cpi = load_series('jpcij@japan')
cpi_recent = load_series('jpcij@japan', start='2010-01-01')

# load multiple series as wide DataFrame
df = load_multiple(['jpcij@japan', 'jpsiip@japan', 'jpcije@japan'])

# load with date range
df = load_multiple(['jpcij@japan', 'jpsiip@japan'], start='2000-01-01', end='2023-12-31')

# load metadata
meta = load_metadata()
meta = load_metadata(['jpcij@japan', 'jpsiip@japan'])
print(meta[['descriptor', 'aggtype', 'shortsource']])
```

### Option: Read Parquet directly (without load.py)

```python
import pandas as pd

data = pd.read_parquet(r'path\to\haver-data\data\data.parquet')
meta = pd.read_parquet(r'path\to\haver-data\data\metadata.parquet')

# get a single series
s = data[data['code'] == 'jpcij@japan'].set_index('date')['value'].dropna()

# get all monthly series as wide DataFrame
monthly = (
    data[data['frequency'] == 'M']
    .pivot(index='date', columns='code', values='value')
    .dropna(how='all')
)
```

---

## Task Scheduler

The scheduled task is named `HaverDataPull` and runs daily at 7:00 AM.

Check status:

```
schtasks /query /tn "HaverDataPull"
```

Run immediately:

```
schtasks /run /tn "HaverDataPull"
```

Delete and recreate (e.g. if launcher path changes):

```
schtasks /delete /tn "HaverDataPull" /f
schtasks /create /tn "HaverDataPull" /tr "D:\Apps\haver_launcher.bat" /sc daily /st 07:00 /ru "%USERNAME%" /f
```

---

## Current Series Coverage

| Country | Database | Frequency | Count |
|---|---|---|---|
| China | emergepr | Monthly | 65 |
| China + Japan | mktpmi | Monthly | 23 |
| Japan | japan | Monthly | 68 |
| Japan | japan | Quarterly | 7 |
| **Total** | | | **163** |

---

## Troubleshooting

**Pull fails with `ImportError: pyarrow`**

```
D:\APPS\python\Python311\python.exe -m pip install pyarrow
```

**Git not recognized**

```
set PATH=%PATH%;D:\Apps\Git\bin
```

**Launcher fails with "UNC path not supported"**

Make sure you are running from Command Prompt (not PowerShell) and that `haver_launcher.bat` lives at `D:\Apps\` (local drive), not on the network share.

**Authentication fails on push**

The PAT may have expired. Generate a new one at github.com -> Settings -> Developer Settings -> Personal Access Tokens -> Tokens (classic), then update the remote URL:

```
git remote set-url origin https://YOUR_USERNAME:YOUR_NEW_TOKEN@github.com/jasonzhixinglu/haver-data.git
```