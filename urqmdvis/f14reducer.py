import os
import sys
from pathlib import Path

class F14Reducer:
    def __init__(self, filename, output_filename=None, progress_cb=None):
        """Initialize the f14Reducer with input and output filenames."""
        self.filename = Path(filename)
        self.progress_cb = progress_cb
        # If no output filename is specified, default to '<filename>_reduced.csv'
        if output_filename is None:
            base = self.filename.with_suffix("")
            self.output_filename = Path(f"{base}_reduced.csv")
        else:
            self.output_filename = Path(output_filename)

    def reduce(self):
        """Reduce the input file by removing extra whitespace and saving to a new file."""
        print(f"Starting file reduction for: {self.filename}")
        if self.progress_cb:
            self.progress_cb("reduce", 0, 1, "Starting file reduction")
        line_count = 0
        with open(self.filename, "r") as fr, open(self.output_filename, "w") as fw:
            for line in fr:
                # Strip extra whitespace from each line and write to output
                fw.write(" ".join(line.split()) + "\n")
                line_count += 1
        print(f"Finished processing {line_count} lines.")
        print(f"Reduction complete.\nOutput saved to: {self.output_filename}")
        if self.progress_cb:
            self.progress_cb("reduce", 1, 1, "File reduction complete")
        return str(self.output_filename)

if __name__ == "__main__":
    # Ensure the script receives exactly one argument
    if len(sys.argv) != 2:
        sys.stderr.write("Usage: python f14reducer.py <input_filename>\n")
        sys.exit(1)

    # Initialize reducer and perform reduction
    filename = sys.argv[1]
    try:
        reducer = F14Reducer(filename)
        reducer.reduce()
    except FileNotFoundError:
        sys.stderr.write(f"Error: The file '{filename}' was not found.\n")
        sys.exit(1)
    except OSError as e:
        sys.stderr.write(f"An I/O error occurred: {e}\n")
        sys.exit(1)
