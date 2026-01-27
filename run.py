import argparse

from urqmdvis.pipeline import run_pipeline


def parse_args():
    parser = argparse.ArgumentParser(description="Run the UrQMD visualization pipeline.")
    parser.add_argument("input_file", help="Path to the UrQMD .dat file")
    parser.add_argument("event_number", type=int, help="Event number to process (1-based)")
    parser.add_argument("--event-name", default="event", help="Prefix for output files")
    parser.add_argument("--parquet-folder", default="outputs/parquet", help="Folder for Parquet outputs")
    parser.add_argument("--csv-folder", default="outputs/csv", help="Folder for CSV outputs")
    parser.add_argument("--skip-reduce", action="store_true", help="Skip the file reduction step")
    parser.add_argument("--skip-parse", action="store_true", help="Skip the parsing/Parquet step")
    parser.add_argument("--skip-convert", action="store_true", help="Skip the Parquet to CSV step")
    return parser.parse_args()


def main():
    args = parse_args()
    run_pipeline(
        input_file=args.input_file,
        event_number=args.event_number,
        event_name=args.event_name,
        parquet_folder=args.parquet_folder,
        csv_folder=args.csv_folder,
        run_reduce=not args.skip_reduce,
        run_parse=not args.skip_parse,
        run_convert=not args.skip_convert,
    )


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as fnf_error:
        print(f"File error: {fnf_error}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
