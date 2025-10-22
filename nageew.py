""" parse_age_from_header.py

Standalone Python script that reads an Excel file named input_header_values.xlsx (located in the same folder), parses the column header_col_value into Min_Age and Max_Age according to the rules you requested, and writes the result to output_with_min_max.xlsx.

If input_header_values.xlsx does not exist the script will create a sample input file covering many variants seen in your screenshots so you can test immediately.

Rules implemented:

Single numeric value like "67"  -> Min_Age = 0, Max_Age = 67

Range like "0-17"            -> Min_Age = 0, Max_Age = 17

Plus like "18+" or "100+"   -> Min_Age = 18 (or the number), Max_Age = 120 (default)

Trailing 'Y' like "120Y"      -> treat like numeric "120"

"Birth" or only-words        -> Min_Age=0, Max_Age=0

"None" / "NONE" / "No"    -> Min_Age=0, Max_Age=0

"18 & Older" / contains older-> Min_Age=18, Max_Age=120

"18 & Younger" / contains younger-> Min_Age=0, Max_Age=18

Default maximum age is 120 when applicable.


Usage: python parse_age_from_header.py

The script will create or read input_header_values.xlsx and produce output_with_min_max.xlsx. """

import re from pathlib import Path import pandas as pd

DEFAULT_MAX_AGE = 120 INPUT_FILE = Path("input_header_values.xlsx") OUTPUT_FILE = Path("output_with_min_max.xlsx")

def create_sample_input(path: Path): samples = [ "67", "0-17", "18+", "120", "120Y", "18Y", "Birth", "None", "NONE", "18 & Older", "18 & Younger", "100+", "0", "100", "1", "1-27", "29", "18 y", "18 years", "Age 18", "AGE RESTRICTIONS", "Accepts Maximum Patient Age", "Infants", "19 & older", "19 & Younger", "0-120", "126", "129 0", "110-123", "20-29", "No", "Yes", "", "  ", "unknown" ] df = pd.DataFrame({"header_col_value": samples}) df.to_excel(path, index=False) print(f"Sample input written to {path}") return df

def parse_age(value: str, default_max=DEFAULT_MAX_AGE): """Parse a header_col_value string and return (min_age, max_age).

The parsing follows the user's specification and is robust to common variants.
"""
if value is None:
    return 0, 0
s = str(value).strip()
if s == "":
    return 0, 0
sl = s.lower().strip()

# Known "no-value" tokens => (0,0)
if sl in {"none", "no", "unknown", "n/a"}:
    return 0, 0

# "birth" => (0,0)
if "birth" in sl:
    return 0, 0

# If string contains no digits at all (pure word/phrase), set to (0,0)
if not re.search(r"\d", sl):
    return 0, 0

# Hyphen range like '0-17', '1 - 27'
m = re.search(r"(\d{1,3})\s*[-–]\s*(\d{1,3})", sl)
if m:
    low = int(m.group(1))
    high = int(m.group(2))
    return max(0, low), min(high, default_max)

# Find all numbers in the string
numbers = re.findall(r"(\d{1,3})", sl)

# Single number cases
if len(numbers) == 1:
    num = int(numbers[0])
    # If a plus sign exists ("18+"), treat as lower bound -> max = default_max
    if "+" in sl or sl.endswith("plus"):
        return num, default_max
    # If contains 'older' -> min=num, max=default_max
    if "older" in sl:
        return num, default_max
    # If contains 'younger' -> min=0, max=num
    if "younger" in sl:
        return 0, num
    # Trailing Y or 'year' words treat as single numeric value -> Min=0, Max=num
    if re.search(r"\d+\s*y\b", sl) or "year" in sl:
        return 0, min(num, default_max)
    # Default single number: Min=0, Max=num (but cap to default_max)
    return 0, min(num, default_max)

# Two-or-more numbers: interpret first as min, second as max unless order reversed
if len(numbers) >= 2:
    low = int(numbers[0])
    high = int(numbers[1])
    if low > high:
        # if second is zero it's probably a single-value with stray 0 -> treat as single
        if high == 0:
            return 0, min(low, default_max)
        # otherwise swap to keep min<=max
        low, high = high, low
    return max(0, low), min(high, default_max)

# Fallback
return 0, 0

def main(): # Ensure pandas is available try: import pandas as pd  # already imported above; this is just a sanity check except Exception as e: print("pandas is required to run this script. Install it with: pip install pandas openpyxl") raise

# If input file doesn't exist, create sample input
if not INPUT_FILE.exists():
    print(f"Input file {INPUT_FILE} not found. Creating a sample input file...")
    input_df = create_sample_input(INPUT_FILE)
else:
    input_df = pd.read_excel(INPUT_FILE)
    # If column missing, show helpful message
    if "header_col_value" not in input_df.columns:
        print("Input file must contain a column named 'header_col_value'.")
        print("Creating a sample input file instead...")
        input_df = create_sample_input(INPUT_FILE)

# Apply parsing
parsed = input_df.copy()
parsed["Min_Age"], parsed["Max_Age"] = zip(*parsed["header_col_value"].map(parse_age))

# Save output
parsed.to_excel(OUTPUT_FILE, index=False)
print(f"Output written to {OUTPUT_FILE}")

if name == "main": main()