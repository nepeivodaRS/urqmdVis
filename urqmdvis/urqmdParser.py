import gc
import sys
from pathlib import Path

import numpy as np
import pandas as pd

class UrqmdParser:
    def __init__(self, pr14, eventNum, pr_save, progress_cb=None):
        """Initialize with the base filename, event number, and output path for Parquet files."""
        self.pr14 = pr14
        self.eventNum = eventNum
        self.pr_save = pr_save
        self.progress_cb = progress_cb
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
        self.evEnd = None
        self.seps_event = None
        self.expected_slices = None

        # Create output folder if it doesn't exist
        output_folder = Path(self.pr_save).parent
        if str(output_folder) and not output_folder.is_dir():
            output_folder.mkdir(parents=True, exist_ok=True)
            print(f"Created output folder: {output_folder}")

    def load_pr14(self):
        """Load and prepare data from the reduced CSV file."""
        try:
            reduced_path = self._resolve_reduced_path(self.pr14)
            print(f"Loading data from: {reduced_path}...")
            df14 = pd.read_csv(
                reduced_path,
                sep=" ",
                names=[
                    "t",
                    "x",
                    "y",
                    "z",
                    "p0",
                    "px",
                    "py",
                    "pz",
                    "m",
                    "ityp",
                    "di3",
                    "ch",
                    "pcn",
                    "ncoll",
                    "ppt",
                    "eta",
                    "nev",
                ],
                dtype=str
            )
            print("Data loaded successfully.")
        except FileNotFoundError:
            raise FileNotFoundError(f"File '{reduced_path}' not found.")
        except Exception as e:
            raise RuntimeError(f"Error loading data: {e}") from e

        # Identify event starts and calculate initial parameters
        evStarts = df14[df14['t'] == "UQMD"].index
        if self.eventNum < 1 or self.eventNum - 1 >= len(evStarts):
            raise ValueError("Event number out of range.")

        self.evStart = evStarts[self.eventNum - 1]
        self.evEnd = evStarts[self.eventNum] if self.eventNum < len(evStarts) else len(df14)
        self.seps = df14[df14["y"].isna()].index
        self.seps_event = self.seps[(self.seps > self.evStart) & (self.seps < self.evEnd)]
        self._set_collision_parameters(df14)
        
        return df14

    @staticmethod
    def _resolve_reduced_path(pr14):
        path = Path(pr14)
        if path.suffix == ".csv":
            return path
        if path.suffix:
            path = path.with_suffix("")
        return Path(f"{path}_reduced.csv")

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
        
        self.expected_slices = int(self.tottime / self.dtime) if self.dtime else None
        if self.expected_slices is not None and self.seps_event is not None:
            actual_slices = len(self.seps_event)
            if actual_slices != self.expected_slices:
                print(
                    f"Warning: expected {self.expected_slices} slices, "
                    f"found {actual_slices} in event {self.eventNum}."
                )

    def process_pr14(self, df14):
        """Process the loaded data and extract time slices, saving them to a Parquet file."""
        slices = []
        total_slices = len(self.seps_event) if self.seps_event is not None else 0
        if self.expected_slices is not None:
            total_slices = min(total_slices, self.expected_slices)
        print(f"Total slices to process: {total_slices}")
        if self.progress_cb:
            self.progress_cb("parse", 0, total_slices, "Parsing slices")

        for i, sep in enumerate(self.seps_event[:total_slices], start=1):
            try:
                slice_len = int(float(df14.iloc[sep]["t"]))
            except (ValueError, TypeError):
                print(f"Warning: invalid slice length at index {sep}, skipping.")
                continue

            eSlice = df14.iloc[sep + 2 : sep + 2 + slice_len].copy()
            numeric_cols = [
                "t",
                "x",
                "y",
                "z",
                "p0",
                "px",
                "py",
                "pz",
                "m",
                "ityp",
                "di3",
                "ch",
                "pcn",
                "ncoll",
                "ppt",
            ]
            eSlice[numeric_cols] = eSlice[numeric_cols].apply(pd.to_numeric, errors="coerce")
            p = np.sqrt(np.square(eSlice["px"]) + np.square(eSlice["py"]) + np.square(eSlice["pz"]))
            denom = p - eSlice["pz"]
            valid = (p > 0) & (denom > 0)
            eSlice["eta"] = np.where(
                valid,
                np.log((p + eSlice["pz"]) / denom),
                np.nan,
            )
            eSlice["nev"] = self.eventNum

            slices.append(eSlice)
            # Progress the time
            if total_slices and self.progress_cb:
                self.progress_cb("parse", i, total_slices)

        if slices:
            data14 = pd.concat(slices, ignore_index=True)
            data14.to_parquet(self.pr_save, index=False)
            print(f"\nData saved to {self.pr_save}")
        else:
            print("No data to save.")

        return data14 if slices else None

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

    try:
        parser = UrqmdParser(input_basename, event_number, output_parquet)
        parser.run()
    except Exception as e:
        sys.stderr.write(f"{e}\n")
        sys.exit(1)
