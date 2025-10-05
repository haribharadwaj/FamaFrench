#!/usr/bin/env python3
# build_factors.py — refresh Fama–French factor artifacts for this repo
# Outputs (percent units, month-end index):
#   data/us_ff5_mom.(parquet|csv.gz)            meta/us_ff5_mom.json
#   data/global_exus_ff5_mom.(parquet|csv.gz)   meta/global_exus_ff5_mom.json
#
# Notes:
# - The ex-US artifact uses the Developed ex-US series (current and maintained),
#   but keeps the legacy stem "global_exus_ff5_mom" for backward compatibility.

from __future__ import annotations

import io
import json
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

# ---------- Output locations ----------
OUT_DIR = Path("data")
META_DIR = Path("meta")
OUT_DIR.mkdir(exist_ok=True)
META_DIR.mkdir(exist_ok=True)

# ---------- Source URLs (current, maintained) ----------
US_5F_URL   = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_CSV.zip"
US_MOM_URL  = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_CSV.zip"

DEXUS_5F_URL  = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Developed_ex_US_5_Factors_CSV.zip"
DEXUS_MOM_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Developed_ex_US_Mom_Factor_CSV.zip"

# ---------- Robust monthly CSV extractor for Ken French zips ----------
def _read_ff_zip_monthly(url: str) -> pd.DataFrame:
    """Download a Ken French ZIP, extract the monthly table, return DataFrame with month-end index."""
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        csv_name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
        text = zf.open(csv_name).read().decode("latin-1", errors="ignore")

    lines = text.splitlines()
    yyyymm = re.compile(r"^\s*([12]\d{3})\s*[-/ ]?\s*(\d{2})\s*([,;]|\s|$)")

    # find header line immediately above the first monthly row
    first_row = None
    for i, ln in enumerate(lines):
        first = ln.split(",")[0].strip()
        if yyyymm.match(first):
            first_row = i
            break
    if first_row is None:
        raise ValueError(f"Monthly block not found in: {url}")

    header_idx = None
    for j in range(first_row - 1, max(-1, first_row - 60), -1):
        if lines[j].strip():
            header_idx = j
            break
    if header_idx is None:
        raise ValueError(f"Header line not found for monthly block: {url}")

    # capture contiguous monthly rows
    block = [lines[header_idx]]
    for ln in lines[first_row:]:
        first = ln.split(",")[0].strip()
        if not first or not yyyymm.match(first):
            break
        block.append(ln)

    df = pd.read_csv(io.StringIO("\n".join(block)))
    df.columns = [c.strip() for c in df.columns]
    date_col = df.columns[0]
    for c in df.columns[1:]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    def _to_me(s: str):
        s = str(s).strip()
        m = re.match(r"^(\d{4})[-/ ]?(\d{2})$", s)
        if m:
            return pd.to_datetime(f"{m.group(1)}-{m.group(2)}") + pd.offsets.MonthEnd(0)
        return pd.to_datetime(s).to_period("M").to_timestamp("M")

    df[date_col] = df[date_col].map(_to_me)
    df = df.dropna(subset=[date_col]).set_index(date_col).sort_index()
    return df

def _harmonize_cols(df: pd.DataFrame, require: Iterable[str]) -> pd.DataFrame:
    rename = {"Mkt-RF": "MKT_RF", "MKT-RF": "MKT_RF", "Mkt_RF": "MKT_RF", "Mom   ": "Mom"}
    df = df.rename(columns={c: rename.get(c, c) for c in df.columns})
    keep = [c for c in require if c in df.columns]
    return df[keep].copy()

def _ensure_all_cols(df: pd.DataFrame, ordered_cols: Iterable[str]) -> pd.DataFrame:
    for c in ordered_cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df[list(ordered_cols)]

def _coverage(df: pd.DataFrame) -> str:
    return f"{df.index.min():%Y-%m} → {df.index.max():%Y-%m}  (n={len(df)})"

def _write_artifacts(stem: str, df: pd.DataFrame, sources: list[str], extras: dict | None = None):
    df = df.sort_index()
    # parquet
    (OUT_DIR / f"{stem}.parquet").write_bytes(df.to_parquet(index=True))
    # csv.gz
    df.to_csv(OUT_DIR / f"{stem}.csv.gz", compression="gzip", float_format="%.6f")

    meta = {
        "dataset": stem,
        "first_date": df.index.min().strftime("%Y-%m-%d"),
        "last_date": df.index.max().strftime("%Y-%m-%d"),
        "rows": int(df.shape[0]),
        "columns": list(df.columns),
        "units": "percent",
        "index": "month_end",
        "sources": sources,
        "built_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "notes": "Columns in percent; month-end index. Missing columns (if any) filled with NaN before save.",
    }
    if extras:
        meta.update(extras)
    (META_DIR / f"{stem}.json").write_text(json.dumps(meta, indent=2))

# ---------- US FF5 + Momentum ----------
def build_us_ff5_mom():
    print("Building US FF5 + Momentum …")
    five = _harmonize_cols(_read_ff_zip_monthly(US_5F_URL), ["MKT_RF", "SMB", "HML", "RMW", "CMA", "RF"])
    mom  = _harmonize_cols(_read_ff_zip_monthly(US_MOM_URL),  ["Mom"])
    print("  5F:",  _coverage(five))
    print("  MOM:", _coverage(mom))

    df = _ensure_all_cols(five.join(mom, how="left"), ["MKT_RF","SMB","HML","RMW","CMA","Mom","RF"])
    _write_artifacts("us_ff5_mom", df, [US_5F_URL, US_MOM_URL], extras={"universe": "US", "includes_emerging": False})
    print("  ✅ US FF5+Mom:", _coverage(df))

# ---------- Developed ex-US FF5 + Momentum (saved under legacy global_exus_ff5_mom) ----------
def build_developed_exus_ff5_mom_as_global_stem():
    print("Building Developed ex-US FF5 + Momentum (output: global_exus_ff5_mom) …")
    five = _harmonize_cols(_read_ff_zip_monthly(DEXUS_5F_URL),  ["MKT_RF","SMB","HML","RMW","CMA","RF"])
    mom  = _harmonize_cols(_read_ff_zip_monthly(DEXUS_MOM_URL), ["Mom"])
    print("  5F:",  _coverage(five))
    print("  MOM:", _coverage(mom))

    df = _ensure_all_cols(five.join(mom, how="left"), ["MKT_RF","SMB","HML","RMW","CMA","Mom","RF"])
    _write_artifacts(
        "global_exus_ff5_mom", df,
        [DEXUS_5F_URL, DEXUS_MOM_URL],
        extras={"universe": "Developed ex-US", "includes_emerging": False}
    )
    print("  ✅ Developed ex-US FF5+Mom:", _coverage(df))

# ---------- Main ----------
if __name__ == "__main__":
    build_us_ff5_mom()
    build_developed_exus_ff5_mom_as_global_stem()
    print("Done. Files written to ./data and metadata to ./meta")
