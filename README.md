# recoverpy: Public code repository for the Network of Biostatisticians for RECOVER

**`recoverpy`** is a highly optimized Python package utilizing **DuckDB** and **Parquet** to process REDCap survey data into analysis-ready core datasets for multiple RECOVER cohorts (Adult, Pediatric, Congenital, Pregnancy). 

This repository was recently migrated from its legacy R-based `dplyr` pipeline into a unified Python architecture. It is based on the original codebase located at [mgb-recover-drc/recover-nbr-public](https://github.com/mgb-recover-drc/recover-nbr-public).

## Installation

You can install `recoverpy` directly from this GitHub repository using `pip`:

```bash
pip install git+https://github.com/hyungrok-do/recoverpy.git
```

This will automatically install all required dependencies, including `pandas`, `duckdb`, `pyarrow`, and `numpy`.

## Quick Start & Usage

### 1. Using the Command Line Interface (CLI)
After installation, you can use the built-in CLI commands to process REDCap data for specific cohorts. This will output highly compressed `.parquet` files.

```bash
# Process the Adult cohort
recover-adult --data_loc project-files --out_dir project-files/DM --dt 20251206

# Other available cohort commands (in development):
# recover-pediatric
# recover-congenital
# recover-pregnancy
```

### 2. Using Python APIs natively (e.g., in Jupyter Notebooks)
You can directly import the core functions to build datasets natively as Pandas DataFrames, without necessarily saving intermediate files:

```python
from recoverpy import load_data

# This single line handles everything:
# 1. Checks if compiled Parquet data already exists; if so, it loads it instantly via tqdm.
# 2. If not (or if recompile=True), it locates the REDCap files, processes them, and saves the output.
core_df, formds_dict = load_data(
    cohort="adult", 
    dt="20251206", 
    base_dir="project-files", 
    out_dir="project-files/DM",
    recompile=False
)

print(f"Core dataset has {len(core_df)} participants.")
```
rticipants.")
```
