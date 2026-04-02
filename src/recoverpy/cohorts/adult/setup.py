import pandas as pd
import duckdb
import logging
from pathlib import Path
from typing import Tuple, Dict
from recoverpy.core.helpers import dd_prep_col_nms, get_cur_form_ds, load_raw_data, get_env_dict

logger = logging.getLogger(__name__)

def build_adult_datasets(dt: str, data_loc: str = "project-files", out_dir: str = "project-files/DM", force_rebuild: bool = False) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """
    Core function to build the Adult core dataset and form-specific datasets.
    Loads raw data, builds the datasets lazily via DuckDB views, saves directly to Parquet.
    """
    if not force_rebuild:
        try:
            env = get_env_dict(cohort="adult", dt=dt, dm_base_dir=out_dir)
            logger.info("Found existing compiled Adult datasets. Loading directly from Parquet...")
            return env.get("core", pd.DataFrame()), env.get("formds_list", {})
        except FileNotFoundError:
            logger.info("Compiled datasets not found. Proceeding to build from raw data...")
            pass
            
    full_ds, ds_dd, event_map = load_raw_data(cohort="adult", dt=dt, base_dir=data_loc)

    id_vrs = ["record_id", "redcap_event_name", "redcap_repeat_instrument", "redcap_repeat_instance"]
    
    logger.info("Splitting raw data into REDCap forms...")
    formds_views = get_cur_form_ds(full_ds, ds_dd, event_map, id_vrs)
    
    logger.info("Building Core Initial...")
    form_counts = event_map.groupby('form').size()
    single_instance_forms = form_counts[form_counts == 1].index.tolist()
    
    registered_forms = []
    for form in single_instance_forms:
        if form in formds_views:
            view_name = formds_views[form]
            registered_forms.append(view_name)
    
    if registered_forms:
        join_clause = f"SELECT t0.* "
        from_clause = f"FROM {registered_forms[0]} t0 "
        for i, df_name in enumerate(registered_forms[1:], start=1):
            cols = [row[0] for row in duckdb.query(f"PRAGMA table_info('{df_name}')").fetchall()]
            cols_to_add = [f"t{i}.\"{c}\"" for c in cols if c != 'record_id' and c not in id_vrs]
            if cols_to_add:
                join_clause += ", " + ", ".join(cols_to_add)
            from_clause += f" LEFT JOIN {df_name} t{i} ON t0.record_id = t{i}.record_id "
        
        query = join_clause + " " + from_clause
        duckdb.execute(f"CREATE OR REPLACE VIEW core_initial AS {query}")
    else:
        duckdb.execute("CREATE OR REPLACE VIEW core_initial AS SELECT NULL AS record_id WHERE FALSE")
    
    logger.info("Calculating derived Core variables...")
    vacc_view = formds_views.get('vaccine_status')
    if vacc_view:
        duckdb.execute(f"""
        CREATE OR REPLACE VIEW vacc_status AS 
        SELECT record_id,
               MAX(CASE WHEN redcap_event_name = 'baseline_arm_1' THEN vacc_vaccyn END) as vacc_base_f,
               MAX(vacc_vaccyn) as vacc_vaccyn_max
        FROM {vacc_view}
        GROUP BY record_id
        """)
    else:
        duckdb.execute("CREATE OR REPLACE VIEW vacc_status AS SELECT NULL AS record_id, NULL AS vacc_base_f, NULL AS vacc_vaccyn_max WHERE FALSE")
    
    core_query = """
    SELECT c.*, 
           v.vacc_base_f,
           CASE 
             WHEN c.infect_yn = '1' AND c.acute_yn = '1' THEN 'Acute Infected'
             WHEN c.infect_yn = '0' AND c.acute_yn = '1' THEN 'Acute Uninfected'
             WHEN c.infect_yn = '1' AND c.acute_yn = '0' THEN 'Post-acute Infected'
             WHEN c.infect_yn = '0' AND c.acute_yn = '0' THEN 'Post-acute Uninfected'
           END AS study_grp,
           SUBSTRING(c.record_id, 4, 4) AS site
    FROM core_initial c
    LEFT JOIN vacc_status v ON c.record_id = v.record_id
    """
    
    out_path = Path(out_dir) / f"adult_{dt}"
    out_path.mkdir(parents=True, exist_ok=True)
    
    core_parquet_path = out_path / "core.parquet"
    logger.info(f"Writing Core dataset directly to disk at {core_parquet_path}...")
    duckdb.execute(f"COPY ({core_query}) TO '{core_parquet_path}' (FORMAT PARQUET)")
    
    logger.info("Saving individual form datasets to Parquet...")
    formds_list = {}
    for form_name, view_name in formds_views.items():
        out_file = out_path / f"formds_list_{form_name}.parquet"
        duckdb.execute(f"COPY (SELECT * FROM {view_name}) TO '{out_file}' (FORMAT PARQUET)")
        formds_list[form_name] = pd.read_parquet(out_file)
        
    core = pd.read_parquet(core_parquet_path)
    return core, formds_list

def run_adult_setup(data_loc: str, out_dir: str, dt: str):
    """
    CLI-compatible entry point for Adult cohort dataset setup.
    """
    from recoverpy.core.helpers import load_data
    load_data(cohort="adult", dt=dt, base_dir=data_loc, out_dir=out_dir, recompile=True)
    logger.info(f"Adult setup complete. Output saved to {out_dir}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 3:
        run_adult_setup(sys.argv[1], sys.argv[2], sys.argv[3])