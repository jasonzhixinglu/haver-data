import pandas as pd
from pathlib import Path

DATA_PATH = Path(__file__).parents[1] / "data" / "data.parquet"
META_PATH = Path(__file__).parents[1] / "data" / "metadata.parquet"

def load_series(code: str, start: str = None, end: str = None) -> pd.Series:
    """Load a single series by code@database, returned as a pd.Series indexed by date."""
    df = pd.read_parquet(DATA_PATH)
    s = df[df["code"] == code].set_index("date")["value"].dropna()
    if start: s = s[s.index >= start]
    if end:   s = s[s.index <= end]
    return s

def load_multiple(codes: list, start: str = None, end: str = None) -> pd.DataFrame:
    """Load multiple series, returned as a wide DataFrame with date index."""
    df = pd.read_parquet(DATA_PATH)
    df = df[df["code"].isin(codes)]
    wide = df.pivot(index="date", columns="code", values="value").dropna(how="all")
    if start: wide = wide[wide.index >= start]
    if end:   wide = wide[wide.index <= end]
    return wide

def load_metadata(codes: list = None) -> pd.DataFrame:
    """Load metadata for all series, or a subset by code list."""
    meta = pd.read_parquet(META_PATH)
    if codes:
        meta = meta[meta.index.isin(codes)]
    return meta

def available_series() -> list:
    """List all series currently tracked in the repo."""
    meta = pd.read_parquet(META_PATH)
    return meta.index.tolist()