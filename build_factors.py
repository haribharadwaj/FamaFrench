#!/usr/bin/env python3
# build_factors.py — rebuild Fama–French factor artifacts with alignment checks

from __future__ import annotations

import io
import json
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Tuple

import pandas as pd
import requests

# ---------- Output folders ----------
OUT_DIR = Path("data")
META_DIR = Path("meta")
OUT_DIR.mkdir(exist_ok=True)
META_DIR.mkdir(exist_ok=True)

# ---------- Source URLs (current) ----------
US_5F_URL   = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_CSV.zip"
US_MOM_URL  = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_CSV.zip"

DEXUS_5F_URL  = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Developed_ex_US_5_Factors_CSV.zip"
DEXUS_MOM_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Developed_ex_US_Mom_Factor_CSV.zip"

# ---------- Parsing helpers ----------
def _read_ff_zip_monthly(url: str) -> pd.DataFrame:
    """Download a Ken French ZIP, extract the MONTHLY table, return DataFrame with a
    clean month-end DateTimeIndex and numeric columns coerced to float."""
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        csv_name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
        text = zf.open(csv_name).read().decode("latin-1", errors="ignore")

    lines = text.splitlines()
    yyyymm = re.compile(r"^\s*([12]\d{3})\s*[-/ ]?\s*(\d{2})\s*([,;]|\s|$)")

    # locate first monthly row
    first_row = None
    for i, ln in enumerate(lines):
        first = ln.split(",")[0].strip()
        if yyyymm.match(first):
            first_row = i
            break
    if first_row is None:
        raise ValueError(f"Monthly block not found in: {url}")

    # header is the last non-empty line above the first monthly row
    header_idx = None
    for j in range(first_row - 1, max(-1, first_row - 60), -1):
        if lines[j].strip():
            header_idx = j
            break
    if header_idx is None:
        raise ValueError(f"Header line for monthly block not found: {url}")

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

    # coerce numerics
    for c in df.columns[1:]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # to month-end index
    def _to_me(s: str):
        s = str(s).strip()
        m = re.match(r"^(\d{4})[-/ ]?(\d{2})$", s)
        if m:
            return pd.to_datetime(f"{m.group(1)}-{m.group(2)}") + pd.offsets.MonthEnd(0)
        return pd.to_datetime(s).to_period("M").to_timestamp("M")

    df[date_col] = df[date_col].map(_to_me)
    df = df.dropna(subset=[date_col]).set_index(date_col)
    # de-duplicate & sort
    df = df[~df.index.duplicated(keep="last")].sort_index()
    return df

def _harmonize_five(df: pd.DataFrame) -> pd.DataFrame:
    """Return 5F+RF with canonical names."""
    rename = {
        "Mkt-RF": "MKT_RF", "MKT-RF": "MKT_RF", "Mkt_RF": "MKT_RF",
        "SMB": "SMB", "HML": "HML", "RMW": "RMW", "CMA": "CMA", "RF": "RF",
    }
    df = df.rename(columns={c: rename.get(c, c) for c in df.columns})
    keep = [c for c in ["MKT_RF","SMB","HML","RMW","CMA","RF"] if c in df.columns]
    return df[keep].copy()

def _harmonize_mom(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect the momentum column and return it standardized as 'Mom'.
    Ken French files may label momentum as 'Mom', 'Mom   ', or 'WML'.
    """
    # Cleaned view of column names
    cols = list(df.columns)
    norm = {c: c.strip() for c in cols}
    lower = {c: norm[c].lower() for c in cols}

    # Preferred candidates (in order)
    # - any column whose cleaned name equals 'Mom'
    # - any column containing 'mom'
    # - any column whose cleaned name equals 'WML' (winners minus losers)
    mom_col = None
    for c in cols:
        if norm[c] == "Mom":
            mom_col = c
            break
    if mom_col is None:
        for c in cols:
            if "mom" in lower[c]:
                mom_col = c
                break
    if mom_col is None:
        for c in cols:
            if norm[c].upper() == "WML":
                mom_col = c
                break

    if mom_col is None:
        raise ValueError(f"No momentum column found. Columns: {cols}")

    out = df.copy()
    if norm[mom_col] != "Mom":
        out = out.rename(columns={mom_col: "Mom"})

    # Keep only the standardized column
    return out[["Mom"]].copy()

def _coverage(df: pd.DataFrame) -> str:
    return f"{df.index.min():%Y-%m} → {df.index.max():%Y-%m}  (n={len(df)})"

def _ensure_all_cols(df: pd.DataFrame, ordered_cols: Iterable[str]) -> pd.DataFrame:
    for c in ordered_cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df[list(ordered_cols)]

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
        "notes": "Columns in percent; month-end index. Alignment diagnostics applied; build fails if momentum is empty.",
    }
    if extras:
        meta.update(extras)
    (META_DIR / f"{stem}.json").write_text(json.dumps(meta, indent=2))

def _align_and_join_with_diagnostics(
    five: pd.DataFrame,
    mom: pd.DataFrame,
    required_cols: Iterable[str],
    mom_name: str = "Mom",
) -> Tuple[pd.DataFrame, dict]:
    """Force month-end, de-dup, align, diagnose, and join. Fail if overlap==0 or Mom empty."""
    def _me(df: pd.DataFrame) -> pd.DataFrame:
        idx = pd.to_datetime(df.index).to_period("M").to_timestamp("M")
        out = df.copy()
        out.index = idx
        out = out[~out.index.duplicated(keep="last")].sort_index()
        return out

    five_me = _me(five)
    mom_me  = _me(mom)

    diag = {
        "five_first": five_me.index.min(), "five_last": five_me.index.max(), "five_len": len(five_me),
        "mom_first": mom_me.index.min(),   "mom_last":   mom_me.index.max(),   "mom_len":  len(mom_me),
        "overlap_len": len(five_me.index.intersection(mom_me.index)),
        "mom_columns": list(mom_me.columns),
    }

    print("  ├─ 5F window:", _coverage(five_me))
    print("  ├─ MOM window:", _coverage(mom_me))
    print("  └─ Overlap months:", diag["overlap_len"])

    if diag["overlap_len"] == 0:
        raise RuntimeError(f"No overlapping dates between five-factor and momentum tables. mom_cols={diag['mom_columns']}")

    # join
    df = five_me.join(mom_me[[mom_name]], how="left")
    df = _ensure_all_cols(df, list(required_cols))

    # sanity: momentum must not be entirely NA
    if df[mom_name].notna().sum() == 0:
        raise RuntimeError(f"Momentum series is empty after join; mom_cols={diag['mom_columns']}")

    return df, diag

# ---------- Builders ----------
def build_us_ff5_mom():
    print("Building US FF5 + Momentum …")
    five = _harmonize_five(_read_ff_zip_monthly(US_5F_URL))
    mom  = _harmonize_mom(_read_ff_zip_monthly(US_MOM_URL))

    df, _ = _align_and_join_with_diagnostics(
        five, mom, ["MKT_RF","SMB","HML","RMW","CMA","Mom","RF"], mom_name="Mom"
    )
    _write_artifacts("us_ff5_mom", df, [US_5F_URL, US_MOM_URL], extras={"universe": "US", "includes_emerging": False})
    print("  ✅ US FF5+Mom:", _coverage(df))

def build_developed_exus_ff5_mom_as_global_stem():
    print("Building Developed ex-US FF5 + Momentum (output: global_exus_ff5_mom) …")
    five = _harmonize_five(_read_ff_zip_monthly(DEXUS_5F_URL))
    mom  = _harmonize_mom(_read_ff_zip_monthly(DEXUS_MOM_URL))

    df, _ = _align_and_join_with_diagnostics(
        five, mom, ["MKT_RF","SMB","HML","RMW","CMA","Mom","RF"], mom_name="Mom"
    )
    _write_artifacts(
        "global_exus_ff5_mom", df, [DEXUS_5F_URL, DEXUS_MOM_URL],
        extras={"universe": "Developed ex-US", "includes_emerging": False}
    )
    print("  ✅ Developed ex-US FF5+Mom:", _coverage(df))

# ---------- Main ----------
if __name__ == "__main__":
    build_us_ff5_mom()
    build_developed_exus_ff5_mom_as_global_stem()
    print("Done. Files written to ./data and metadata to ./meta")
