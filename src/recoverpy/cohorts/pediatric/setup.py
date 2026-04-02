import pandas as pd
import duckdb
import logging
from pathlib import Path
from typing import Tuple, Dict
from recoverpy.core.helpers import dd_prep_col_nms, get_cur_form_ds, load_raw_data, get_env_dict

logger = logging.getLogger(__name__)

def build_pediatric_datasets(dt: str, data_loc: str = "project-files", out_dir: str = "project-files/DM", force_rebuild: bool = False) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """
    Core function to build the Pediatric core dataset and form-specific datasets.
    """
    if not force_rebuild:
        try:
            env = get_env_dict(cohort="pediatric", dt=dt, dm_base_dir=out_dir)
            logger.info("Found existing compiled Pediatric datasets. Loading directly from Parquet...")
            return env.get("core", pd.DataFrame()), env.get("formds_list", {})
        except FileNotFoundError:
            logger.info("Compiled datasets not found. Proceeding to build from raw data...")
            pass
            
    full_ds, ds_dd, event_map = load_raw_data(cohort="pediatric", dt=dt, base_dir=data_loc)

    id_vrs = ["record_id", "redcap_event_name", "redcap_repeat_instrument", "redcap_repeat_instance"]
    
    logger.info("Splitting raw data into REDCap forms for Pediatric cohort...")
    formds_views = get_cur_form_ds(full_ds, ds_dd, event_map, id_vrs, peds_cohort_flag=True)
    
    # Placeholder for actual pediatric left join logic using DuckDB views
    duckdb.execute("CREATE OR REPLACE VIEW core_pediatric AS SELECT NULL AS record_id, NULL AS redcap_event_name WHERE FALSE")
    
    out_path = Path(out_dir) / f"pediatric_{dt}"
    out_path.mkdir(parents=True, exist_ok=True)
    
    logger.info("Saving processed Peds datasets to Parquet...")
    formds_list = {}
    for form_name, view_name in formds_views.items():
        out_file = out_path / f"formds_list_{form_name}.parquet"
        duckdb.execute(f"COPY (SELECT * FROM {view_name}) TO '{out_file}' (FORMAT PARQUET)")
        formds_list[form_name] = pd.read_parquet(out_file)
        
    core_parquet_path = out_path / "core.parquet"
    duckdb.execute(f"COPY (SELECT * FROM core_pediatric) TO '{core_parquet_path}' (FORMAT PARQUET)")
    core = pd.read_parquet(core_parquet_path)
    
    return core, formds_list

def run_pediatric_setup(data_loc: str, out_dir: str, dt: str):
    """
    CLI-compatible entry point for Pediatric cohort dataset setup.
    """
    from recoverpy.core.helpers import load_data
    load_data(cohort="pediatric", dt=dt, base_dir=data_loc, out_dir=out_dir, recompile=True)
    logger.info(f"Pediatric setup complete. Output saved to {out_dir}")