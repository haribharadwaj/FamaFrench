# Fama–French Factor Datasets

This repository provides harmonized monthly **Fama–French factor datasets** in a clean, consistent format for research and teaching.

- `us_ff5_mom` — U.S. **Five-Factor + Momentum** dataset  
- `global_exus_ff5_mom` — **Developed ex-U.S. Five-Factor + Momentum** dataset  
  (named “global_exus_ff5_mom” for backward compatibility, but derived from the *Developed ex-U.S.* Fama–French series)

Each dataset includes:
```
MKT_RF, SMB, HML, RMW, CMA, Mom, RF
```

- Units: **percent**
- Frequency: **monthly**, indexed to **month-end**
- Formats: `.parquet` and `.csv.gz` under `data/`
- Metadata: `.json` files under `meta/` describing date range, columns, sources, and notes

---

## Data Sources

All data are downloaded directly from the official **Kenneth R. French Data Library**  
([https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html](https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html)).

| Dataset | Source Files | Coverage | Universe |
|----------|---------------|-----------|-----------|
| `us_ff5_mom` | - `F-F_Research_Data_5_Factors_2x3_CSV.zip`  <br> - `F-F_Momentum_Factor_CSV.zip` | 1963-07 → latest | U.S. |
| `global_exus_ff5_mom` | - `Developed_ex_US_5_Factors_CSV.zip`  <br> - `Developed_ex_US_Mom_Factor_CSV.zip` | 1990-07 → latest | Developed ex-U.S. (no emerging markets) |

> **Note:**  
> The older *Global ex-U.S.* factor series (ending in June 2019) is no longer maintained by the Fama–French library.  
> The **Developed ex-U.S.** datasets used here continue through the present and provide nearly identical coverage for most global equity research applications.

---

## Repository Structure

```
├── build_factors.py     # Script to rebuild and refresh all datasets
├── data/                # Compressed Parquet and CSV data files
├── meta/                # Corresponding JSON metadata
└── README.md            # This documentation
```

---

## Updating the Datasets

Run the build script to download the latest updates from the Fama–French Data Library:

```bash
python build_factors.py
git add data meta
git commit -m "Refresh Fama–French factors"
git push
```

This will:
- Download the latest published monthly updates.
- Harmonize variable names and align to month-end.
- Regenerate the `.parquet`, `.csv.gz`, and `.json` artifacts.

---

## Attribution and Licensing

The Fama–French data are provided by:

> **Kenneth R. French – Dartmouth College**  
> Fama/French Data Library, Tuck School of Business  
> [https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html](https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html)

### Terms of Use
These datasets are made available for **academic, educational, and non-commercial research** purposes.  
By using or redistributing this repository, you agree to comply with the [Kenneth R. French Data Library Terms of Use](https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html).

Derived harmonized files (`.parquet`, `.csv.gz`, `.json`) are provided **under the same conditions** — for reproducible academic work only.

---

## Citation

If you use these datasets in publications, please cite:

> Eugene F. Fama and Kenneth R. French, “A Five-Factor Asset Pricing Model,” *Journal of Financial Economics*, 116 (1): 1-22, 2015.

and acknowledge the **Kenneth R. French Data Library** as the data source.

Example:
> *Data obtained from the Fama/French Data Library at Dartmouth College (Ken French’s website).*  

---

## Contact

For reproducibility issues or harmonization questions, open an issue or contact the repository maintainer.

---

*Last updated: 2025-10-05*
