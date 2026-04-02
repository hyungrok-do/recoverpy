import argparse
import sys
from recoverpy.cohorts.adult.setup import run_adult_setup

def _get_parser():
    parser = argparse.ArgumentParser(description="RECOVER NBR Dataset Setup CLI")
    parser.add_argument("command", choices=["run-adult", "run-pediatric", "run-congenital", "run-pregnancy"], help="Cohort to run")
    parser.add_argument("--data_loc", required=True, help="Path to raw REDCap data folder")
    parser.add_argument("--out_dir", required=True, help="Path to save processed Parquet files")
    parser.add_argument("--dt", required=True, help="Data timestamp string (e.g. 20251206)")
    return parser

def main():
    parser = _get_parser()
    args = parser.parse_args()
    
    if args.command == "run-adult":
        print(f"Running Adult setup for {args.dt}...")
        run_adult_setup(args.data_loc, args.out_dir, args.dt)
    elif args.command == "run-pediatric":
        print(f"Pediatric setup for {args.dt} (Not fully implemented)")
    elif args.command == "run-congenital":
        print(f"Congenital setup for {args.dt} (Not fully implemented)")
    elif args.command == "run-pregnancy":
        print(f"Pregnancy setup for {args.dt} (Not fully implemented)")

if __name__ == "__main__":
    main()
