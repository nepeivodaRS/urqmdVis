import os
import sys
from pathlib import Path

import pandas as pd

class ParquetToCSVConverter:
    def __init__(self, parquet_file, output_folder, output_prefix, progress_cb=None):
        """Initialize with the Parquet file, output folder, and output file prefix."""
        self.parquet_file = parquet_file
        self.output_folder = output_folder
        self.output_prefix = output_prefix
        self.progress_cb = progress_cb

        # Create output folder if it doesn't exist
        output_folder = Path(self.output_folder)
        if str(output_folder) and not output_folder.is_dir():
            output_folder.mkdir(parents=True, exist_ok=True)
            print(f"Created output folder: {output_folder}")

    def load_parquet(self):
        """Load the Parquet file."""
        df = pd.read_parquet(self.parquet_file)
        print("Parquet file loaded successfully.")
        return df

    def convert_to_csv(self, df):
        """Convert each time slice in the Parquet data to a separate CSV file."""
        frame = 0
        successful_conversions = 0
        failed_conversions = 0
        df = df.sort_values("t")
        total_slices = len(df["t"].unique())  # Total number of unique time slices

        print("Starting CSV conversion...")
        if self.progress_cb:
            self.progress_cb("convert", 0, total_slices, "Converting to CSV")

        # Group data by time slice and export each group as a CSV file
        for i, (nSlice, sliceData) in enumerate(df.groupby("t", sort=True)):
            sliceData = sliceData[["t", "x", "y", "z", "m", "ityp"]]
            output_path = os.path.join(self.output_folder, f"{self.output_prefix}_frame_{frame}.csv")
            
            try:
                sliceData.to_csv(output_path, index=False)
                successful_conversions += 1
            except Exception as e:
                print(f"\nFailed to create CSV for time slice {nSlice}: {e}")
                failed_conversions += 1

            if self.progress_cb:
                self.progress_cb("convert", i + 1, total_slices)

            frame += 1

        print(f"\nConversion completed: {successful_conversions} files created, {failed_conversions} failed.")


    def run(self):
        """Run the conversion from Parquet to CSV."""
        df = self.load_parquet()
        self.convert_to_csv(df)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.stderr.write("Usage: python parquetToCsv.py <parquet_file> <output_folder> <output_prefix>\n")
        sys.exit(1)

    parquet_file_path = sys.argv[1]
    output_folder = sys.argv[2]
    output_prefix = sys.argv[3]

    try:
        converter = ParquetToCSVConverter(parquet_file_path, output_folder, output_prefix)
        converter.run()
    except FileNotFoundError:
        sys.stderr.write(f"Error: Parquet file '{parquet_file_path}' not found.\n")
        sys.exit(1)
    except Exception as e:
        sys.stderr.write(f"Error loading Parquet file: {e}\n")
        sys.exit(1)
