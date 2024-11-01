import pandas as pd
import numpy as np
import gc
import sys
import os

class urqmdParser:
    def __init__(self, pr14, eventNum, pr_save):
        """Initialize with the base filename, event number, and output path for Parquet files."""
        self.pr14 = pr14
        self.eventNum = eventNum
        self.pr_save = pr_save
        self.tottime = None
        self.dtime = None
        self.massA = None
        self.chA = None
        self.massB = None
        self.chB = None
        self.b = None
        self.time_arr = None
        self.seps = None
        self.sliceStart = None
        self.evStart = None

    def load_pr14(self):
        """Load and prepare data from the reduced CSV file."""
        try:
            print(f"Loading data from: {self.pr14}_reduced.csv...")
            df14 = pd.read_csv(
                f"{self.pr14}_reduced.csv", sep=' ', 
                names=['t', 'x', 'y', 'z', 'p0', 'px', 'py', 'pz', 'm', 'ityp', 'di3', 'ch', 'pcn', 'ncoll', 'ppt', 'eta', 'nev'],
                dtype=str
            )
            print("Data loaded successfully.")
        except FileNotFoundError:
            print(f"Error: File '{self.pr14}_reduced.csv' not found.")
            sys.exit(1)
        except Exception as e:
            print(f"Error loading data: {e}")
            sys.exit(1)

        # Identify event starts and calculate initial parameters
        evStarts = df14[df14['t'] == "UQMD"].index
        if self.eventNum - 1 >= len(evStarts):
            print("Error: Event number out of range.")
            sys.exit(1)

        self.evStart = evStarts[self.eventNum - 1]
        self.seps = df14[df14['y'].isna()].index
        self._set_collision_parameters(df14)
        
        return df14

    def _set_collision_parameters(self, df14):
        """Extract and print collision parameters from the data."""
        self.tottime = float(df14.iloc[self.evStart + 5, 7])
        self.dtime = float(df14.iloc[self.evStart + 5, 9])
        self.massA = int(df14.iloc[self.evStart + 1, 3])
        self.massB = int(df14.iloc[self.evStart + 1, 8])
        self.chA = int(df14.iloc[self.evStart + 1, 4])
        self.chB = int(df14.iloc[self.evStart + 1, 9])
        self.b = df14.iloc[self.evStart + 3, 1]
        
        print("Collision Parameters:")
        print(f"  Total Time (tottime): {self.tottime}")
        print(f"  Delta Time (dtime):  {self.dtime}")
        print(f"  Impact parameter: {self.b} fm")
        print(f"  Mass and Charge of Particles:")
        print(f"    Particle A - Mass: {self.massA}, Charge: {self.chA}")
        print(f"    Particle B - Mass: {self.massB}, Charge: {self.chB}")
        
        self.sliceStart = (self.eventNum - 1) * int(self.tottime / self.dtime)

    def process_pr14(self, df14):
        """Process the loaded data and extract time slices, saving them to a Parquet file."""
        data14 = None
        total_slices = int(self.tottime / self.dtime)
        print(f"Total slices to process: {total_slices}")

        for i, sep in enumerate(self.seps[self.sliceStart:], start=1):
            eSlice = df14.iloc[sep + 2 : sep + 2 + int(df14.iloc[sep]['t'])]
            eSlice = eSlice.astype(float)
            p = np.sqrt(np.square(eSlice['px']) + np.square(eSlice['py']) + np.square(eSlice['pz']))
            eSlice['eta'] = np.log((p + eSlice['pz']) / (p - eSlice['pz']))
            eSlice['nev'] = self.eventNum

            # Aggregate data into data14
            data14 = pd.concat((data14, eSlice), ignore_index=True) if data14 is not None else eSlice.copy()
            # Progress the time
            bar = "#" * int(20 * (i / total_slices)) + "-" * (20 - int(20 * (i / total_slices)))
            sys.stdout.write(f"\r[{bar}] {i} / {total_slices} slices processed")
            sys.stdout.flush()

            if i >= total_slices:
                break

        if data14 is not None:
            data14.to_parquet(self.pr_save)
            print(f"\nData saved to {self.pr_save}")
        else:
            print("No data to save.")

        return data14

    def run(self):
        """Execute the parsing and processing workflow."""
        print("Starting parsing...")
        df14 = self.load_pr14()
        data14 = self.process_pr14(df14)
        del df14, data14
        gc.collect()
        print("Parsing completed, memory cleaned up.")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.stderr.write("Usage: python urqmdParser.py <input_basename> <event_number> <output_parquet>\n")
        sys.exit(1)

    input_basename = sys.argv[1]
    event_number = int(sys.argv[2])
    output_parquet = sys.argv[3]

    parser = urqmdParser(input_basename, event_number, output_parquet)
    parser.run()
