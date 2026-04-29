"""
Diagnostic script for daily-frequency Haver data pulling.

Run this on the server to identify how hv.data() behaves for daily series.
Results will help fix src/pull.py to handle daily frequency correctly.

Usage:
    python temp/test_daily_pull.py
"""

import Haver as hv
import pandas as pd

# A handful of Ken's daily series spread across both databases and asset classes
INTDAILY_CODES = ['R111M3M', 'FCM10', 'S111SP5', 'X111JPJ', 'R111RDT']
DAILY_CODES    = ['FCM10', 'FCM20', 'SPVIX', 'FXWSJ']

STARTDATE = '2024-01-01'

SEP = '-' * 70

def print_result(label, result):
    print(f"\n  {label}:")
    if result is None:
        print("    -> None")
        return
    if isinstance(result, tuple):
        print(f"    -> tuple of {len(result)} items")
        for i, item in enumerate(result):
            _describe(f"item[{i}]", item)
    elif isinstance(result, pd.DataFrame):
        _describe("DataFrame", result)
    else:
        print(f"    -> {type(result).__name__}: {result}")

def _describe(label, obj):
    if isinstance(obj, pd.DataFrame):
        print(f"    {label}: DataFrame shape={obj.shape}")
        print(f"      columns: {list(obj.columns[:10])}")
        print(f"      index type: {type(obj.index).__name__}, dtype: {obj.index.dtype}")
        if not obj.empty:
            print(f"      index sample: {list(obj.index[:3])}")
    elif isinstance(obj, dict):
        print(f"    {label}: dict keys={list(obj.keys())}")
        for k, v in obj.items():
            print(f"      {k}: {v}")
    elif obj is None:
        print(f"    {label}: None")
    else:
        print(f"    {label}: {type(obj).__name__} = {str(obj)[:200]}")


# ── 1. Haver version / available attributes ──────────────────────────────────
print(SEP)
print("1. Haver package info")
print(SEP)
print(f"  hv version: {getattr(hv, '__version__', 'unknown')}")
print(f"  hv attributes: {[a for a in dir(hv) if not a.startswith('_')]}")


# ── 2. Per-series metadata probe ──────────────────────────────────────────────
print(f"\n{SEP}")
print("2. Per-series metadata probe — one code at a time")
print(SEP)
for db, code in [('INTDAILY', 'R111M3M'), ('INTDAILY', 'S111SP5'), ('DAILY', 'FCM10'), ('DAILY', 'SPVIX')]:
    for db_case in [db, db.lower()]:
        try:
            meta = hv.metadata(database=db_case, codes=[code])
            print(f"  hv.metadata(database='{db_case}', codes=['{code}']): {type(meta).__name__} shape={getattr(meta, 'shape', 'N/A')}")
            if isinstance(meta, pd.DataFrame) and not meta.empty:
                print(f"    columns: {list(meta.columns)}")
                print(f"    row: {meta.iloc[0].to_dict()}")
        except TypeError:
            # If codes= param isn't supported, try without it and note
            print(f"  hv.metadata(database='{db_case}', codes=['{code}']): codes= param not supported — skipping full-db call")
        except Exception as e:
            print(f"  hv.metadata(database='{db_case}', codes=['{code}']): ERROR — {e}")


# ── 3. hv.data() with frequency='D' (current approach) ───────────────────────
print(f"\n{SEP}")
print("3. hv.data() with frequency='D' — current pull.py approach")
print(SEP)
for db, codes in [('INTDAILY', INTDAILY_CODES[:3]), ('DAILY', DAILY_CODES[:3])]:
    print(f"\n  database='{db}', codes={codes}")
    for rtype in ['3tuple', 'dict', None]:
        kwargs = dict(database=db, frequency='D', startdate=STARTDATE)
        if rtype:
            kwargs['rtype'] = rtype
        label = f"rtype={rtype!r}"
        try:
            result = hv.data(codes, **kwargs)
            print_result(label, result)
        except Exception as e:
            print(f"  {label}: ERROR — {e}")


# ── 4. hv.data() without frequency param ─────────────────────────────────────
print(f"\n{SEP}")
print("4. hv.data() with NO frequency param")
print(SEP)
for db, codes in [('INTDAILY', INTDAILY_CODES[:3]), ('DAILY', DAILY_CODES[:3])]:
    print(f"\n  database='{db}', codes={codes}")
    try:
        result = hv.data(codes, database=db, startdate=STARTDATE, rtype='3tuple')
        print_result("no freq", result)
    except Exception as e:
        print(f"  no freq: ERROR — {e}")


# ── 5. hv.data() with lowercase database name ─────────────────────────────────
print(f"\n{SEP}")
print("5. hv.data() with lowercase database name")
print(SEP)
for db, codes in [('intdaily', INTDAILY_CODES[:3]), ('daily', DAILY_CODES[:2])]:
    print(f"\n  database='{db}', codes={codes}")
    try:
        result = hv.data(codes, database=db, frequency='D', startdate=STARTDATE, rtype='3tuple')
        print_result("lowercase db", result)
    except Exception as e:
        print(f"  lowercase db: ERROR — {e}")


# ── 6. Single-code pull with full native Haver format (db:code) ───────────────
print(f"\n{SEP}")
print("6. Single code in native Haver format 'DATABASE:CODE' (no database param)")
print(SEP)
native_codes = ['INTDAILY:R111M3M', 'INTDAILY:S111SP5', 'DAILY:FCM10', 'DAILY:SPVIX']
for code in native_codes:
    try:
        result = hv.data([code], startdate=STARTDATE, rtype='3tuple')
        print_result(code, result)
    except Exception as e:
        print(f"  {code}: ERROR — {e}")


# ── 7. hv.data() single series, no rtype, no freq ────────────────────────────
print(f"\n{SEP}")
print("7. Minimal call — hv.data(code, startdate=...) with native format")
print(SEP)
for code in ['INTDAILY:R111M3M', 'DAILY:FCM10']:
    try:
        result = hv.data(code, startdate=STARTDATE)
        print_result(code, result)
    except Exception as e:
        print(f"  {code}: ERROR — {e}")


print(f"\n{SEP}")
print("Done.")
print(SEP)
