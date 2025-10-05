# FamaFrench Factor Datasets

This repository provides harmonized Fama–French monthly factor datasets (US FF5 + Momentum and Global ex-US FF5 + Momentum) in a clean, consistent format.

- Each dataset includes columns: `MKT_RF`, `SMB`, `HML`, `RMW`, `CMA`, `Mom`, and `RF`
- Units are in **percent**, monthly frequency, indexed to **month-end**
- Files are provided as `.parquet` and `.csv.gz` under `data/`
- Metadata in JSON format under `meta/`

## Updating

```bash
python build_factors.py
git add data meta
git commit -m "Refresh factors"
git push
