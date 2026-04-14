# haver-data

A lightweight data pipeline that pulls macroeconomic time series from Haver Analytics and version-controls them as Parquet files on GitHub. Designed to bridge IMF network-restricted Haver access and a portable personal analysis environment.

---

## Infrastructure Overview

| Machine | Haver Access | GitHub Access | Notes |
|---|---|---|---|
| Work server (`EMDSWN45P`) | ✅ | ✅ | Runs the scheduled pull |
| Work laptop | ✅ | ❌ | VPN blocks GitHub |
| Personal PC | ❌ | ✅ | Used for analysis |

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
│   └── load.py              # Utility for consuming repos
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

### `data/data.parquet`

Long-format table with one row per series-date:

| column | type | description |
|---|---|---|
| `date` | datetime64 | Period start date (monthly: 1st of month, quarterly: 1st of quarter) |
| `code` | str | Series in `code@database` format e.g. `jpcij@japan` |
| `value` | float64 | Observed value in native Haver units |
| `frequency` | str | `M` = monthly, `Q` = quarterly, `D` = daily |

### `data/metadata.parquet`

Indexed by `code@database`, one row per series, 18 columns:

| column | description |
|---|---|
| `descriptor` | Full series name e.g. `Japan: CPI: All Items (2020=100)` |
| `frequency` | Native Haver frequency |
| `startdate` / `enddate` | Available history in Haver |
| `aggtype` | Aggregation type: `AVG`, `SUM`, `EOP`, or `NDF` |
| `datatype` | Data type e.g. `%`, `INDEX`, `$` |
| `magnitude` | Scaling factor (0 = no scaling) |
| `geography1` | UN country code e.g. `158` = Japan, `924` = China |
| `shortsource` | Short source e.g. `CNBS`, `MIC` |
| `longsource` | Full source name |
| `database` | Haver database name |
| `code` | Raw Haver series code |

---

## Adding or Removing Series

Edit `config/series.yaml` from any machine with GitHub access, commit, and push. The server picks up changes on the next scheduled pull via `git pull` at the start of the launcher.

### series.yaml format

```yaml
defaults:
  startdate: "1990-01-01"   # global start date for all series

series:
  - code: jpcij@japan       # FORMAT: havercode@database (lowercase)
    frequency: monthly       # monthly, quarterly, or daily

  - code: jsngpcp@japan
    frequency: quarterly

  - code: usfedfunds@usecon
    frequency: daily
```

### Finding series codes

Use the Haver DLX desktop application to search for series. The code format is always `seriescode@databasename` in lowercase. You can also search metadata programmatically:

```python
import Haver as hv

# search all series in a database by keyword
hmd = hv.metadata(database='japan')
matches = hmd[hmd.descriptor.str.contains('CPI', case=False)]
print(matches[['code', 'descriptor', 'frequency']])
```

### Adding a series (step by step)

1. Find the series code in Haver DLX or via metadata search
2. Open `config/series.yaml` in VS Code
3. Add a new entry under `series:`
4. Save, commit, and push:
```powershell
   git add config/series.yaml
   git commit -m "Add [series description]"
   git push
```
5. The server will pull it and include it in the next scheduled run, or trigger manually (see below)

---

## Triggering a Manual Pull

Open **Command Prompt** (not PowerShell — CMD cannot run from UNC paths) and run:

```bat
D:\Apps\haver_launcher.bat
```

To check the result:
```bat
type "\\EMDSWN45P\data\jlu2\haver_data\haver-data\logs\scheduler.log"
```

---

## Loading Data in a Consuming Repo

### Option 1: Clone haver-data as a sibling repo

```bash
git clone https://github.com/jasonzhixinglu/haver-data.git
```

Then in your analysis code:

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
meta = load_metadata()                              # all series
meta = load_metadata(['jpcij@japan', 'jpsiip@japan'])  # subset
print(meta[['descriptor', 'aggtype', 'shortsource']])
```

### Option 2: Read Parquet directly

If you just want the raw data without using `load.py`:

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

### Keeping data fresh on personal PC

After cloning, just run `git pull` in the haver-data directory to get the latest data:

```bash
cd path/to/haver-data
git pull
```

---

## Task Scheduler

The scheduled task is named `HaverDataPull` and runs daily at 7:00 AM.

To check status:
```bat
schtasks /query /tn "HaverDataPull"
```

To run immediately:
```bat
schtasks /run /tn "HaverDataPull"
```

To delete and recreate (e.g. if launcher path changes):
```bat
schtasks /delete /tn "HaverDataPull" /f
schtasks /create /tn "HaverDataPull" /tr "D:\Apps\haver_launcher.bat" /sc daily /st 07:00 /ru "%USERNAME%" /f
```

---

## Current Series Coverage

| Country | Database | Frequency | Count |
|---|---|---|---|
| China | `emergepr` | Monthly | 65 |
| China + Japan | `mktpmi` | Monthly | 23 |
| Japan | `japan` | Monthly | 65 |
| Japan | `japan` | Quarterly | 7 |
| **Total** | | | **160** |

---

## Troubleshooting

**Pull fails with `ImportError: pyarrow`**
```bat
D:\APPS\python\Python311\python.exe -m pip install pyarrow
```

**Git not recognized**
```bat
set PATH=%PATH%;D:\Apps\Git\bin
```

**Launcher fails with "UNC path not supported"**
Make sure you are running from Command Prompt (not PowerShell) and that `haver_launcher.bat` lives at `D:\Apps\` (local drive), not on the network share.

**Authentication fails on push**
The PAT may have expired. Generate a new one at github.com → Settings → Developer Settings → Personal Access Tokens → Tokens (classic), then update the remote URL:
```bat
git remote set-url origin https://YOUR_USERNAME:YOUR_NEW_TOKEN@github.com/jasonzhixinglu/haver-data.git
```