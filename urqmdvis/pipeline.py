from pathlib import Path

from .f14reducer import F14Reducer
from .parquetToCsv import ParquetToCSVConverter
from .urqmdParser import UrqmdParser


def reduce_file(input_filename, output_filename=None, progress_cb=None):
    reducer = F14Reducer(input_filename, output_filename=output_filename, progress_cb=progress_cb)
    return reducer.reduce()


def parse_to_parquet(base_filename, event_num, parquet_output_path, progress_cb=None):
    parser = UrqmdParser(base_filename, event_num, parquet_output_path, progress_cb=progress_cb)
    parser.run()
    return parquet_output_path


def convert_parquet_to_csv(parquet_path, output_folder, prefix, progress_cb=None):
    converter = ParquetToCSVConverter(parquet_path, output_folder, prefix, progress_cb=progress_cb)
    converter.run()


def run_pipeline(
    input_file,
    event_number,
    event_name,
    parquet_folder,
    csv_folder,
    run_reduce=True,
    run_parse=True,
    run_convert=True,
    progress_cb=None,
):
    input_path = Path(input_file)
    if not input_path.is_file():
        raise FileNotFoundError(f"The input file '{input_file}' does not exist. Please check the path.")

    parquet_folder_path = Path(parquet_folder)
    parquet_file_path = parquet_folder_path / f"{event_name}_{event_number}.parquet"
    steps = [s for s in (run_reduce, run_parse, run_convert) if s]
    total_steps = len(steps) if steps else 1
    step_idx = 0

    if run_reduce:
        step_idx += 1
        if progress_cb:
            progress_cb("pipeline", step_idx, total_steps, "Reducing file")
        reduce_file(str(input_path), progress_cb=progress_cb)

    if run_parse:
        base_filename = str(input_path.with_suffix(""))
        step_idx += 1
        if progress_cb:
            progress_cb("pipeline", step_idx, total_steps, "Parsing to Parquet")
        parse_to_parquet(base_filename, event_number, str(parquet_file_path), progress_cb=progress_cb)

    if run_convert:
        step_idx += 1
        if progress_cb:
            progress_cb("pipeline", step_idx, total_steps, "Converting to CSV")
        convert_parquet_to_csv(
            str(parquet_file_path),
            str(csv_folder),
            f"{event_name}{event_number}",
            progress_cb=progress_cb,
        )

    return str(parquet_file_path)
