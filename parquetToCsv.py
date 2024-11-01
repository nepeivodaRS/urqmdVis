import pandas as pd
import os
import sys

class ParquetToCSVConverter:
    def __init__(self, parquet_file, output_folder, output_prefix):
        """Initialize with the Parquet file, output folder, and output file prefix."""
        self.parquet_file = parquet_file
        self.output_folder = output_folder
        self.output_prefix = output_prefix

        # Create output folder if it doesn't exist
        if not os.path.isdir(self.output_folder):
            os.makedirs(self.output_folder)
            print(f"Created output folder: {self.output_folder}")

    def load_parquet(self):
        """Load the Parquet file."""
        try:
            df = pd.read_parquet(self.parquet_file)
            print("Parquet file loaded successfully.")
            return df
        except FileNotFoundError:
            print(f"Error: Parquet file '{self.parquet_file}' not found.")
            sys.exit(1)
        except Exception as e:
            print(f"Error loading Parquet file: {e}")
            sys.exit(1)

    def convert_to_csv(self, df):
        """Convert each time slice in the Parquet data to a separate CSV file."""
        frame = 0
        successful_conversions = 0
        failed_conversions = 0
        total_slices = len(df['t'].unique())  # Total number of unique time slices

        print("Starting CSV conversion...")

        # Group data by time slice and export each group as a CSV file
        for i, (nSlice, sliceData) in enumerate(df.groupby('t')):
            sliceData = sliceData[['t', 'x', 'y', 'z', 'm', 'ityp']]
            output_path = os.path.join(self.output_folder, f"{self.output_prefix}_frame_{frame}.csv")
            
            try:
                sliceData.to_csv(output_path, index=False)
                successful_conversions += 1
            except Exception as e:
                print(f"\nFailed to create CSV for time slice {nSlice}: {e}")
                failed_conversions += 1

            # Update progress bar
            bar_length = 20
            progress = (i + 1) / total_slices
            bar = "#" * int(bar_length * progress) + "-" * (bar_length - int(bar_length * progress))
            sys.stdout.write(f"\r[{bar}] {i + 1} / {total_slices} frames processed")
            sys.stdout.flush()

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

    converter = ParquetToCSVConverter(parquet_file_path, output_folder, output_prefix)
    converter.run()
