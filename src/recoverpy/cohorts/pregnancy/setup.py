import pandas as pd
import duckdb
import logging
from pathlib import Path
from typing import Tuple, Dict
from recoverpy.core.helpers import dd_prep_col_nms, get_cur_form_ds, load_raw_data

logger = logging.getLogger(__name__)

def build_pregnancy_datasets(dt: str, data_loc: str = "project-files", out_dir: str = "project-files/DM") -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """
    Core function to build the Pregnancy core dataset.
    """
    full_ds, ds_dd, event_map = load_raw_data(cohort="pregnancy", dt=dt, base_dir=data_loc)
    
    # Placeholder for pregnancy data building logic
    formds_list = {}
    core = pd.DataFrame()
    
    out_path = Path(out_dir) / f"pregnancy_{dt}"
    out_path.mkdir(parents=True, exist_ok=True)
    core.to_parquet(out_path / "core.parquet", engine='pyarrow', index=False)
    
    return core, formds_list

def run_pregnancy_setup(data_loc: str, out_dir: str, dt: str):
    """
    Main entry point for Pregnancy cohort dataset setup.
    """
    from recoverpy.core.helpers import load_data
    load_data(cohort="pregnancy", dt=dt, base_dir=data_loc, out_dir=out_dir, recompile=True)
    logger.info(f"Pregnancy setup complete. Output saved to {out_dir}")
