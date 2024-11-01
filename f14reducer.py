import sys
import os

class f14Reducer:
    def __init__(self, filename, output_filename=None):
        """Initialize the f14Reducer with input and output filenames."""
        self.filename = filename
        # If no output filename is specified, default to '<filename>_reduced.csv'
        self.output_filename = output_filename or f"{os.path.splitext(filename)[0]}_reduced.csv"

    def reduce(self):
        """Reduce the input file by removing extra whitespace and saving to a new file."""
        print(f"Starting file reduction for: {self.filename}")
        line_count = 0
        try:
            with open(self.filename, 'r') as fr, open(self.output_filename, 'w') as fw:
                for line in fr:
                    # Strip extra whitespace from each line and write to output
                    fw.write(" ".join(line.split()) + '\n')
                    line_count += 1
            print(f"Finished processing {line_count} lines.")
            print(f"Reduction complete.\nOutput saved to: {self.output_filename}")
        except FileNotFoundError:
            print(f"Error: The file '{self.filename}' was not found.")
        except IOError as e:
            print(f"An I/O error occurred: {e}")

if __name__ == "__main__":
    # Ensure the script receives exactly one argument
    if len(sys.argv) != 2:
        sys.stderr.write("Usage: python f14reducer.py <input_filename>\n")
        sys.exit(1)

    # Initialize reducer and perform reduction
    filename = sys.argv[1]
    reducer = f14Reducer(filename)
    reducer.reduce()
