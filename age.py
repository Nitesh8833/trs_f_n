import re
import os
import pandas as pd
from typing import Tuple, Optional

# ========================== CONFIG ========================== #
DEFAULT_MAX_AGE = 150  # Default upper bound for open-ended expressions like 18+
INPUT_EXCEL_PATH = r"C:\Users\n299141\NiteshTransformation\2025\Oct\week4\age\age.xlsx"
SHEET_NAME = None
OUTPUT_EXCEL_PATH = r"C:\Users\n299141\NiteshTransformation\2025\Oct\week4\age\age_output31.xlsx"

# ========================== LABEL MAPPING ========================== #
LABEL_MAP = {
    "adult": (18, DEFAULT_MAX_AGE),
    "adults": (18, DEFAULT_MAX_AGE),
    "adult only": (18, DEFAULT_MAX_AGE),
    "adolescents": (10, 21),
    "adolescent": (10, 21),
    "adolescents to adult": (10, 21),
    "pediatric": (0, 18),
    "pediatrics": (0, 18),
    "children": (0, 12),
    "child": (0, 12),
    "adolescent & adult": (6, 21),
    "pediatric and adult": (0, 21),
    "pediatric/adult": (0, 21),
    "pediatric only": (0, 17),
    "children only": (0, 17),
    "neonatal": (0, 0.25),  # ~3 months
    "infants": (0, 1),
    "birth to 18": (0, 18),
    "birth to 21": (0, 21),
    "no pediatric patients": (18, DEFAULT_MAX_AGE),
    "preferred 18 and over": (18, DEFAULT_MAX_AGE),
    "female specialist": (0, 18),
    "females only": (None, None),
    "permanent females": (None, None),
    "infectious pts only 18-99": (18, 99),
}

# ========================== REGEX HELPERS ========================== #
SPACES_RE = re.compile(r"\s+")
RANGE_RE = re.compile(r"(\d+)\s*(?:to|-|–|—)\s*(\d+)", re.IGNORECASE)
PLUS_RE = re.compile(r"\+", re.IGNORECASE)
AND_UP_RE = re.compile(r"(\d+)\s*(?:\+|and up|older|over|above)", re.IGNORECASE)
AND_YOUNGER_RE = re.compile(r"(\d+)\s*(?:and under|and younger)", re.IGNORECASE)
YEARS_SUFFIX_RE = re.compile(r"(\d+)\s*(?:yrs?|years?)", re.IGNORECASE)
MONTHS_PHRASE_RE = re.compile(r"(\d+)\s*(?:months?)", re.IGNORECASE)
DAYS_RE = re.compile(r"(\d+)\s*(?:days?)", re.IGNORECASE)
GENERIC_TWO_UNIT_RE = re.compile(r"(\d+)\s*(?:yrs?|years?)\s*(?:and\s*(?:\d+)\s*(?:months?))", re.IGNORECASE)
NUMBER_ONLY_RE = re.compile(r"^\d+$")

# ========================== UTILS ========================== #
def clamp_age(value: float) -> Optional[float]:
    if value is None:
        return None
    return min(value, DEFAULT_MAX_AGE)

def parse_numeric(value: str) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None

# ========================== SPECIAL RULES ========================== #
def apply_special_rules(raw_value: Optional[str], header_name: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    if not raw_value:
        return (None, None)

    val = raw_value.lower().strip()

    if header_name and "female" in header_name.lower() and "male" not in header_name.lower():
        if val in ["all", "both"]:
            return (0.0, DEFAULT_MAX_AGE)
        elif val in ["adult", "adults"]:
            return (18, DEFAULT_MAX_AGE)
        elif val in ["children", "child"]:
            return (0, 12)

    return (None, None)

# ========================== MAIN PARSER ========================== #
def parse_age_entry(raw: str) -> Tuple[Optional[float], Optional[float]]:
    if not raw:
        return (None, None)

    s_clean = str(raw).strip().lower()
    s_clean = s_clean.replace("&", "and").replace("–", "-")
    s_clean = SPACES_RE.sub(" ", s_clean)

    # Label-based mapping
    if s_clean in LABEL_MAP:
        return LABEL_MAP[s_clean]

    # Range like "5-10" or "6 to 12"
    for m in RANGE_RE.finditer(s_clean):
        min_age = parse_numeric(m.group(1))
        max_age = parse_numeric(m.group(2))
        return (min_age, clamp_age(max_age))

    # Open-ended: "18+", "18 and up"
    m = AND_UP_RE.search(s_clean)
    if m:
        min_age = parse_numeric(m.group(1))
        return (min_age, DEFAULT_MAX_AGE)

    # "x and younger"
    m = AND_YOUNGER_RE.search(s_clean)
    if m:
        max_age = parse_numeric(m.group(1))
        return (0, clamp_age(max_age))

    # "18 years"
    m = YEARS_SUFFIX_RE.match(s_clean)
    if m:
        age = parse_numeric(m.group(1))
        return (age, age)

    # "6 months"
    m = MONTHS_PHRASE_RE.match(s_clean)
    if m:
        months = parse_numeric(m.group(1))
        years = months / 12 if months is not None else None
        return (0, years)

    # "30 days"
    m = DAYS_RE.match(s_clean)
    if m:
        days = parse_numeric(m.group(1))
        years = days / 365 if days is not None else None
        return (0, years)

    # Single number
    if NUMBER_ONLY_RE.match(s_clean):
        num = parse_numeric(s_clean)
        return (num, num)

    # "no age restrictions"
    if "no restriction" in s_clean or "no age restriction" in s_clean or "no age restrictions" in s_clean:
        return (0, DEFAULT_MAX_AGE)

    # Extract digits as fallback
    nums = re.findall(r"\d+", s_clean)
    if len(nums) == 1:
        n = parse_numeric(nums[0])
        return (0, clamp_age(n))
    elif len(nums) >= 2:
        a = parse_numeric(nums[0])
        b = parse_numeric(nums[1])
        if a is not None and b is not None:
            return (min(a, b), clamp_age(max(a, b)))

    return (None, None)

# ========================== EXCEL PROCESSOR ========================== #
def process_excel(
    input_path: str,
    sheet_name: Optional[str] = None,
    header_col_value: Optional[str] = None,
    output_path: Optional[str] = None,
) -> str:
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    xl = pd.ExcelFile(input_path)
    if sheet_name is None:
        if len(xl.sheet_names) == 1:
            sheet_name = xl.sheet_names[0]
        else:
            raise ValueError(f"Specify sheet_name; found multiple: {xl.sheet_names}")

    if sheet_name not in xl.sheet_names:
        raise ValueError(f"Sheet {sheet_name!r} not found in workbook")

    df = xl.parse(sheet_name=sheet_name)

    if header_col_value is None:
        raise ValueError("Column header_col_value not provided")

    if header_col_value not in df.columns:
        raise ValueError(f"Column {header_col_value!r} not found in {sheet_name!r}")

    # Parse each row
    min_vals, max_vals = [], []
    for _, row in df.iterrows():
        mn, mx = apply_special_rules(row[header_col_value], header_name=header_col_value)
        if mn is None and mx is None:
            mn, mx = parse_age_entry(row[header_col_value])
        min_vals.append(mn)
        max_vals.append(mx)

    # Insert results
    df.insert(df.columns.get_loc(header_col_value) + 1, "min_Age", min_vals)
    df.insert(df.columns.get_loc(header_col_value) + 2, "max_Age", max_vals)

    out_path = output_path or re.sub(r"(\.xlsx?)$", r"_output\1", input_path)
    df.to_excel(out_path, index=False)
    return out_path

# ========================== MAIN ========================== #
if __name__ == "__main__":
    print("💡 Running age band parser...")
    print(f"Input: {INPUT_EXCEL_PATH}")
    try:
        written = process_excel(
            input_path=INPUT_EXCEL_PATH,
            sheet_name=SHEET_NAME,
            header_col_value="Age_Band",
            output_path=OUTPUT_EXCEL_PATH,
        )
        print(f"✅ Successfully processed. Output file saved at:\n{written}")
    except Exception as e:
        print(f"❌ Error occurred: {e}")
