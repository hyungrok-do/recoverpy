import pandas as pd
import numpy as np
import duckdb
import re
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from tqdm.auto import tqdm

logger = logging.getLogger(__name__)

def load_data(cohort: str, dt: str, base_dir: str = "project-files", out_dir: str = "project-files/DM", recompile: bool = False) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """
    Universal data loader. Checks for compiled data first; builds if missing or if recompile=True.
    """
    if not recompile:
        try:
            env = get_env_dict(cohort=cohort, dt=dt, dm_base_dir=out_dir)
            logger.info(f"Found compiled {cohort} data. Loaded directly from Parquet.")
            return env.get("core", pd.DataFrame()), env.get("formds_list", {})
        except FileNotFoundError:
            logger.info(f"Compiled {cohort} datasets not found. Proceeding to build from raw data...")
            pass
            
    # Dynamically call the builder to avoid circular imports
    cohort_lower = cohort.lower()
    if cohort_lower == "adult":
        from recoverpy.cohorts.adult.setup import build_adult_datasets
        return build_adult_datasets(dt=dt, data_loc=base_dir, out_dir=out_dir)
    elif cohort_lower in ["ped", "pediatric"]:
        from recoverpy.cohorts.pediatric.setup import build_pediatric_datasets
        return build_pediatric_datasets(dt=dt, data_loc=base_dir, out_dir=out_dir)
    elif cohort_lower in ["cong", "congenital"]:
        from recoverpy.cohorts.congenital.setup import build_congenital_datasets
        return build_congenital_datasets(dt=dt, data_loc=base_dir, out_dir=out_dir)
    elif cohort_lower == "pregnancy":
        from recoverpy.cohorts.pregnancy.setup import build_pregnancy_datasets
        return build_pregnancy_datasets(dt=dt, data_loc=base_dir, out_dir=out_dir)
    else:
        raise ValueError(f"Unknown cohort: {cohort}")

def load_raw_data(cohort: str, dt: str, base_dir: str = "project-files") -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Automatically locates and loads the raw REDCap dataset, data dictionary, and event map
    for a given cohort and date based on the standard Seven Bridges directory structure.
    """
    base_path = Path(base_dir)
    cohort_capitalized = cohort.capitalize()
    if cohort.lower() == "ped": cohort_capitalized = "Pediatric"
    if cohort.lower() == "cong": cohort_capitalized = "Congenital"
    
    # 1. Find top level folder (e.g. RECOVERAdult_Data_*)
    top_level_folders = list(base_path.glob(f"RECOVER{cohort_capitalized}_Data_*"))
    
    if not top_level_folders:
        top_level_data_loc = base_path
    else:
        # Try to find one matching year and month of dt
        dt_y = dt[:4]
        dt_m = dt[4:6]
        matched_folders = [f for f in top_level_folders if f"{dt_y}{dt_m}" in f.name or f"{dt_y}.{dt_m}" in f.name]
        if matched_folders:
            top_level_data_loc = max(matched_folders) # In case of multiple, pick latest
        else:
            top_level_data_loc = max(top_level_folders) # Pick absolute latest
            
    # Use recursive globbing to find the files ANYWHERE inside the top-level directory.
    # This guarantees we find them even if the inner folders were unzipped differently.
    dd_files = list(top_level_data_loc.rglob("RECOVER*_DataDictionary_*.csv"))
    event_map_files = list(top_level_data_loc.rglob("RECOVER*_eventmap_*.csv"))
    raw_data_files = list(top_level_data_loc.rglob("*redcap_data.tsv"))
            
    if not dd_files or not event_map_files or not raw_data_files:
         raise FileNotFoundError(f"Could not find all required REDCap files recursively inside {top_level_data_loc}")
         
    logger.info(f"Loading raw REDCap data from {raw_data_files[0].parent}...")
    
    # Initialize DuckDB with out-of-core processing to prevent OOM
    duckdb.execute("PRAGMA temp_directory='/tmp/duckdb_swap'")
    duckdb.execute("PRAGMA memory_limit='8GB'")
    duckdb.execute("PRAGMA threads=4")
    
    # Keep the data completely on disk! Do NOT materialize it into Python/Pandas.
    # Create a DuckDB Relation that acts as a lazy pointer to the CSV.
    raw_data = duckdb.read_csv(str(raw_data_files[0]), header=True, sep='\t', all_varchar=True)
    
    raw_dd = pd.read_csv(dd_files[0])
    clean_dd = dd_prep_col_nms(raw_dd)
    event_map = pd.read_csv(event_map_files[0])
    
    return raw_data, clean_dd, event_map

def dd_prep_col_nms(ds_dd_initial: pd.DataFrame) -> pd.DataFrame:
    """Prepares data dictionary column names, handling different 7B REDCap DD versions."""
    df = ds_dd_initial.copy()
    has_underscore = any('_' in col for col in df.columns)
    
    if has_underscore:
        df.columns = [col.replace('_', '.') for col in df.columns]
        df = df.rename(columns={
            'field.name': 'vr.name',
            'select.choices.or.calculations': 'choices.calculations.or.slider.labels'
        })
    else:
        new_cols = []
        for col in df.columns:
            c = col.lower()
            c = re.sub(r'\s\(.*?\)', '', c)
            c = re.sub(r'[^\w\s]', '', c)
            c = re.sub(r'\s+', '.', c)
            new_cols.append(c)
        df.columns = new_cols
        if 'variable.field.name' in df.columns:
            df = df.rename(columns={'variable.field.name': 'vr.name'})
    return df

def get_cur_form_ds(full_ds, dd: pd.DataFrame, eventmap: pd.DataFrame, 
                    id_vrs: List[str], peds_cohort_flag: bool = False) -> Dict[str, str]:
    """
    Creates a dictionary of datasets for each form. 
    Uses DuckDB to optimize the filtering of completed rows.
    Returns a dictionary mapping form names to DuckDB View names to avoid memory duplication.
    """
    if isinstance(full_ds, pd.DataFrame):
        all_variable_names = list(full_ds.columns)
        duckdb.register('full_ds', full_ds)
    else:
        all_variable_names = full_ds.columns
        full_ds.create_view('full_ds', replace=True)

    forms = dd['form.name'].dropna().unique()
    out_dict = {}
    
    duckdb.register('eventmap', eventmap)
    
    for cur_form in tqdm(forms, desc="Processing REDCap forms"):
        form_vrs = dd[(dd['form.name'] == cur_form) & (~dd['field.type'].isin(['notes']))]['vr.name'].tolist()
        form_vrs_ms = [col for col in all_variable_names if any(col.startswith(v + "___") for v in form_vrs)]
        sel_vars = list(set(id_vrs + form_vrs + form_vrs_ms).intersection(all_variable_names))
        if peds_cohort_flag and f"{cur_form}_complete" in all_variable_names:
            sel_vars.append(f"{cur_form}_complete")
            
        check_cols = list(set(form_vrs) - set(id_vrs))
        if f"{cur_form}_complete" in all_variable_names:
            check_cols.append(f"{cur_form}_complete")
        check_cols = [c for c in check_cols if c in sel_vars]
        
        if not check_cols:
            continue
            
        select_clause = ", ".join([f'"{c}"' for c in sel_vars])
        sum_clause = " + ".join([f"(CASE WHEN \"{c}\" IS NOT NULL AND \"{c}\" != '' THEN 1 ELSE 0 END)" for c in check_cols])
        
        query = f"""
        SELECT {select_clause}, '{cur_form}' as form
        FROM full_ds
        WHERE ({sum_clause}) > 0
        """
        if cur_form == "covid_treatment":
            query += " AND redcap_event_name = 'baseline_arm_1'"
            
        view_name = f"df_{cur_form}"
        duckdb.execute(f"CREATE OR REPLACE VIEW {view_name} AS {query}")
        
        # We can perform the uniqueness check directly in SQL without pulling data
        grp_cols = "record_id, redcap_event_name"
        if 'redcap_repeat_instance' in sel_vars:
            grp_cols += ", redcap_repeat_instance"
            
        check_query = f"""
        SELECT COUNT(*) as cnt, COUNT(DISTINCT ROW({grp_cols})) as dist_cnt 
        FROM {view_name}
        """
        res = duckdb.query(check_query).fetchone()
        if res and res[0] != res[1]:
            raise ValueError(f"Error: too many rows per identifier for form {cur_form}")
            
        out_dict[cur_form] = view_name

    duckdb.unregister('eventmap')
    # Do not unregister full_ds yet, the views depend on it!
    return out_dict

def mk_vrs_bin(vr_series: pd.Series, yes_vals: List[Any], na_vals: List[Any]) -> pd.Series:
    """Helper to binarize variables."""
    return pd.Series(np.where(vr_series.isin(yes_vals), 1, 
                     np.where(vr_series.isna() | vr_series.isin(na_vals), np.nan, 0)))

def flip_bin(vr_series: pd.Series) -> pd.Series:
    """Flips binary 0/1 values."""
    return pd.Series(np.where(vr_series.isna(), np.nan, np.where(vr_series == 1, 0, 1)))

def fix_year(yr: pd.Series) -> pd.Series:
    this_year_num = datetime.now().year % 100
    
    def _fix(y):
        if pd.isna(y): return np.nan
        y_str = str(y).strip()[-2:]
        if not y_str.isdigit(): return np.nan
        y_num = int(y_str)
        if 19 <= y_num <= this_year_num:
            return 2000 + y_num
        return np.nan
        
    return yr.apply(_fix)

def fix_month(m: pd.Series) -> pd.Series:
    def _fix(x):
        try:
            val = float(x)
            if 0 <= val <= 12: return val
        except (ValueError, TypeError):
            pass
        return np.nan
    return m.apply(_fix)

def fix_yeardt(ds: pd.DataFrame, vr_pf: str) -> pd.DataFrame:
    """Fix year/month/date creation from distinct components via DuckDB inplace SQL."""
    duckdb.register('ds', ds)
    this_year = datetime.now().year
    
    query = f"""
    SELECT *,
      CASE 
        WHEN TRY_CAST(RIGHT(CAST({vr_pf}_dty AS VARCHAR), 2) AS INTEGER) BETWEEN 19 AND {this_year % 100}
        THEN 2000 + TRY_CAST(RIGHT(CAST({vr_pf}_dty AS VARCHAR), 2) AS INTEGER)
        ELSE NULL 
      END AS _yr,
      CASE 
        WHEN TRY_CAST({vr_pf}_dtm AS FLOAT) BETWEEN 0 AND 12
        THEN TRY_CAST({vr_pf}_dtm AS INTEGER)
        ELSE NULL 
      END AS _mn
    FROM ds
    """
    tmp = duckdb.query(query).df()
    
    # Construct date
    dt_chr = tmp.apply(
        lambda row: f"{int(row['_yr'])}-{int(row['_mn']) if pd.notna(row['_mn']) else 6}-1" 
        if pd.notna(row['_yr']) else np.nan, axis=1
    )
    tmp[f"{vr_pf}_date"] = pd.to_datetime(dt_chr, errors='coerce')
    tmp.drop(columns=['_yr', '_mn'], inplace=True)
    duckdb.unregister('ds')
    return tmp

def zip_region_fxn(ds: pd.DataFrame, join_vr: str, zip_db_loc: str, vr_ext: str = "") -> pd.DataFrame:
    """Appends US region data based on Zip codes using DuckDB."""
    state_to_region = {
        "CT": "Northeast: New England", "ME": "Northeast: New England", "MA": "Northeast: New England",
        "NH": "Northeast: New England", "RI": "Northeast: New England", "VT": "Northeast: New England",
        "NJ": "Northeast: Middle Atlantic", "NY": "Northeast: Middle Atlantic", "PA": "Northeast: Middle Atlantic",
        "IL": "Midwest: East North Central", "IN": "Midwest: East North Central", "MI": "Midwest: East North Central",
        "OH": "Midwest: East North Central", "WI": "Midwest: East North Central", "IA": "Midwest: West North Central",
        "KS": "Midwest: West North Central", "MN": "Midwest: West North Central", "MO": "Midwest: West North Central",
        "NE": "Midwest: West North Central", "ND": "Midwest: West North Central", "SD": "Midwest: West North Central",
        "DE": "South: South Atlantic", "DC": "South: South Atlantic", "FL": "South: South Atlantic",
        "GA": "South: South Atlantic", "MD": "South: South Atlantic", "NC": "South: South Atlantic",
        "SC": "South: South Atlantic", "VA": "South: South Atlantic", "WV": "South: South Atlantic",
        "AL": "South: East South Central", "KY": "South: East South Central", "MS": "South: East South Central",
        "TN": "South: East South Central", "AR": "South: West South Central", "LA": "South: West South Central",
        "OK": "South: West South Central", "TX": "South: West South Central",
        "AZ": "West: Mountain", "CO": "West: Mountain", "ID": "West: Mountain", "MT": "West: Mountain",
        "NV": "West: Mountain", "NM": "West: Mountain", "UT": "West: Mountain", "WY": "West: Mountain",
        "AK": "West: Pacific", "CA": "West: Pacific", "HI": "West: Pacific", "OR": "West: Pacific",
        "WA": "West: Pacific"
    }
    state_mapping_df = pd.DataFrame(list(state_to_region.items()), columns=['region_state', 'region'])
    
    # Load and clean ZIP DB
    zip_code_ds = pd.read_csv(zip_db_loc, dtype=str)
    zip_code_ds['zip'] = zip_code_ds['zip'].str.zfill(5)
    
    # Pre-processing 3 digit matches
    zip_code_ds_3dig = zip_code_ds.copy()
    zip_code_ds_3dig['zip'] = zip_code_ds_3dig['zip'].str[:3]
    
    zip_combined = pd.concat([zip_code_ds, zip_code_ds_3dig]).groupby('zip')['state'].apply(lambda x: "/".join(x.unique())).reset_index()
    zip_combined.rename(columns={'state': 'region_state'}, inplace=True)
    
    duckdb.register('ds', ds)
    duckdb.register('zip_combined', zip_combined)
    duckdb.register('state_mapping', state_mapping_df)
    
    query = f"""
    SELECT ds.*, 
           zc.region_state AS region_state{vr_ext},
           COALESCE(sm.region, 'Other') AS region{vr_ext}
    FROM ds
    LEFT JOIN zip_combined zc ON ds."{join_vr}" = zc.zip
    LEFT JOIN state_mapping sm ON zc.region_state = sm.region_state
    """
    
    result = duckdb.query(query).df()
    duckdb.unregister('ds')
    duckdb.unregister('zip_combined')
    duckdb.unregister('state_mapping')
    
    return result

def get_env_dict(cohort: str, dt: Optional[str] = None, dm_base_dir: str = "project-files/DM") -> Dict[str, pd.DataFrame]:
    """
    Python equivalent of R's get_env_list().
    Reads all processed Parquet datasets from a specific cohort and date directory into a dictionary.
    """
    if cohort == "cong": 
        cohort = "congenital"
    
    base_path = Path(dm_base_dir) / cohort
    if not base_path.exists():
        base_path = Path(dm_base_dir)
        
    if dt is None:
        date_dirs = [d.name for d in base_path.iterdir() if d.is_dir() and d.name.isdigit()]
        if not date_dirs:
            raise FileNotFoundError(f"No date directories found in {base_path}")
        dt = max(date_dirs)
        
    env_loc = base_path / dt
    if not env_loc.exists():
        env_loc = Path(dm_base_dir) / f"{cohort}_{dt}"
        if not env_loc.exists():
            raise FileNotFoundError(f"Data directory for date {dt} not found at {env_loc}.")
            
    out_dict = {}
    formds_list = {}
    
    parquet_files = list(env_loc.glob("*.parquet"))
    if not parquet_files:
        logger.warning(f"No parquet files found in {env_loc}")
        
    for fl in tqdm(parquet_files, desc=f"Loading {cohort} Parquet files"):
        obj_name = fl.stem
        df = pd.read_parquet(fl)
        
        if obj_name.startswith("formds_list_"):
            form_name = obj_name.replace("formds_list_", "")
            formds_list[form_name] = df
        else:
            out_dict[obj_name] = df
            
    if formds_list:
        out_dict["formds_list"] = formds_list
        
    return out_dict
