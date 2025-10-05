# build_factors.py
import io, zipfile, re, json, requests, pandas as pd
from pathlib import Path
from datetime import datetime

OUT = Path("data"); META = Path("meta")
OUT.mkdir(exist_ok=True); META.mkdir(exist_ok=True)

# ---------- robust CSV slicer for Ken French zips ----------
def _read_ff_zip(url, require_cols):
    """
    Download a Ken French ZIP, slice the monthly section only,
    return a DataFrame indexed by month-end (percent units).
    """
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content))
    csv_name = [n for n in z.namelist() if n.lower().endswith(".csv")][0]
    text = z.open(csv_name).read().decode("latin-1", errors="ignore")
    lines = text.splitlines()

    # First monthly row looks like YYYYMM (allow 1963-07 variants)
    yyyymm = re.compile(r"^\s*([12]\d{3})\s*[-/ ]?\s*(\d{2})\s*([,;]|\s|$)")

    # Find start of monthly block and the header just above it
    data_start = None
    for i, ln in enumerate(lines):
        first = ln.split(",")[0].strip()
        if yyyymm.match(first):
            data_start = i; break
    if data_start is None:
        raise ValueError("Monthly block not found.")

    header_idx = None
    for j in range(data_start-1, max(-1, data_start-50), -1):
        if lines[j].strip():
            header_idx = j; break
    if header_idx is None:
        raise ValueError("Header line not found.")

    # Collect contiguous monthly lines
    block = [lines[header_idx]]
    for ln in lines[data_start:]:
        first = ln.split(",")[0].strip()
        if not first or not yyyymm.match(first):
            break
        block.append(ln)

    df = pd.read_csv(io.StringIO("\n".join(block)))
    df.columns = [c.strip() for c in df.columns]
    date_col = df.columns[0]
    for c in df.columns:
        if c != date_col:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # Parse YYYYMM/1963-07 to month-end
    def to_me(s):
        s = str(s).strip()
        m = re.match(r"^(\d{4})[-/ ]?(\d{2})$", s)
        if m:
            return pd.to_datetime(f"{m.group(1)}-{m.group(2)}") + pd.offsets.MonthEnd(0)
        return pd.to_datetime(s).to_period("M").to_timestamp("M")
    df[date_col] = df[date_col].map(to_me)
    df = df.dropna(subset=[date_col]).set_index(date_col).sort_index()
    # Standardize names
    df = df.rename(columns={"Mkt-RF":"MKT_RF","MKT-RF":"MKT_RF"})
    # Keep only needed/available
    keep = [c for c in require_cols if c in df.columns]
    return df[keep]

def _ensure_all_cols(df, all_cols):
    for c in all_cols:
        if c not in df.columns:
            df[c] = pd.NA
    # canonical order
    return df[[c for c in all_cols]]

def _write_all(name, df, sources):
    df = df.sort_index()
    # Write parquet and gzipped csv
    df.to_parquet(OUT / f"{name}.parquet", index=True)
    df.to_csv(OUT / f"{name}.csv.gz", compression="gzip", float_format="%.6f")
    meta = {
        "dataset": name,
        "first_date": df.index.min().strftime("%Y-%m-%d"),
        "last_date": df.index.max().strftime("%Y-%m-%d"),
        "rows": int(df.shape[0]),
        "columns": df.columns.tolist(),
        "units": "percent",
        "index": "month_end",
        "sources": sources,
        "built_utc": datetime.utcnow().isoformat() + "Z",
        "notes": "Always include columns MKT_RF, SMB, HML, RMW, CMA, Mom, RF; missing columns filled with NaN."
    }
    (META / f"{name}.json").write_text(json.dumps(meta, indent=2))

# ---------- US FF5 + Momentum ----------
def build_us_ff5_mom():
    five = _read_ff_zip(
        "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_CSV.zip",
        require_cols=["MKT_RF","SMB","HML","RMW","CMA","RF"]
    )
    mom = _read_ff_zip(
        "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_CSV.zip",
        require_cols=["Mom"]
    )
    df = five.join(mom, how="left")
    df = _ensure_all_cols(df, ["MKT_RF","SMB","HML","RMW","CMA","Mom","RF"])
    _write_all("us_ff5_mom", df, sources=[
        "Ken French Data Library: F-F_Research_Data_5_Factors_2x3 (Monthly)",
        "Ken French Data Library: F-F_Momentum_Factor (Monthly)"
    ])

# ---------- Global ex-US FF5 + Momentum (multiple fallbacks) ----------
def build_global_exus_ff5_mom():
    # try Global ex-US zip first
    five = _read_ff_zip(
        "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Global_5_Factors_EX_US_CSV.zip",
        require_cols=["MKT_RF","SMB","HML","RMW","CMA","RF"]
    )
    # momentum: ex-US if available, else global momentum
    try:
        mom = _read_ff_zip(
            "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Global_ex_US_MOM_Factor_CSV.zip",
            require_cols=["Mom"]
        )
    except Exception:
        mom = _read_ff_zip(
            "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Global_MOM_Factor_CSV.zip",
            require_cols=["Mom"]
        )
    df = five.join(mom, how="left")
    df = _ensure_all_cols(df, ["MKT_RF","SMB","HML","RMW","CMA","Mom","RF"])
    _write_all("global_exus_ff5_mom", df, sources=[
        "Ken French Data Library: Global_5_Factors_EX_US (Monthly)",
        "Ken French Data Library: Global_ex_US_MOM_Factor / Global_MOM_Factor (Monthly)"
    ])

if __name__ == "__main__":
    print("Building US FF5 + Momentum…")
    build_us_ff5_mom()
    print("Building Global ex-US FF5 + Momentum…")
    build_global_exus_ff5_mom()
    print("Done. Files in ./data and metadata in ./meta")
