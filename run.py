import os
from f14reducer import F14Reducer
from urqmdParser import UrqmdParser
from parquetToCsv import ParquetToCSVConverter

# Input configuration
filename = "../files/urqmd_AuAu_0-3fm/urqmd_1_14.dat"
event_name = "event"
event_number = 9

# Output folder configuration
parquet_folder = "output_parquet_files"
csv_folder = "output_csv_files"
parquet_file_path = f"./{parquet_folder}/{event_name}_{event_number}.parquet"

# Validate input file path
if not os.path.isfile(filename):
    raise FileNotFoundError(f"The input file '{filename}' does not exist. Please check the path.")

try:
    # Step 1: File Reduction
    def reduce_file(input_filename):
        reducer = F14Reducer(input_filename)
        reducer.reduce()
        print("File reduction completed successfully.")

    # Step 2: Parsing and Parquet Conversion
    def parse_to_parquet(base_filename, event_num, parquet_output_path):
        parser = UrqmdParser(base_filename, event_num, parquet_output_path)
        parser.run()
        print("Parsing and Parquet file creation completed successfully.")

    # Step 3: CSV Conversion
    def convert_parquet_to_csv(parquet_path, output_folder, prefix):
        converter = ParquetToCSVConverter(parquet_path, output_folder, prefix)
        converter.run()
        print("CSV conversion completed successfully.")

    # Execute each step in sequence
    reduce_file(filename)
    parse_to_parquet(os.path.splitext(filename)[0], event_number, parquet_file_path)
    convert_parquet_to_csv(parquet_file_path, csv_folder, f"{event_name}{event_number}")

except FileNotFoundError as fnf_error:
    print(f"File error: {fnf_error}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
