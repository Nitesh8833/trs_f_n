# Update: Output exactly these FOUR columns in a single sheet:
# 1) "Accepts Minimum Patient Age"  (raw value from the first detected Min/From column; empty if none)
# 2) "Accepts Maximum Patient Age"  (raw value from the first detected Max/To column; empty if none)
# 3) "min_age"  (parsed per rules, only from non-numeric inputs)
# 4) "max_age"  (parsed per rules, only from non-numeric inputs)
#
# Rules preserved: 100+ -> 100/150; 'newborn and 10' -> 0/10; 'fetal'->0; days->years; month ranges; etc.
# Fallback: if both min & max blank after parsing but a generic Age column has non-numeric text, parse it to both.
# Source file preference remains Book2222.xlsx if present.

import os, re, math
from typing import List, Tuple, Optional, Any, Dict
import numpy as np
import pandas as pd
from caas_jupyter_tools import display_dataframe_to_user

MAX_AGE = 150.0
PREFER_PATHS = ["/mnt/data/Book2222.xlsx", "/mnt/data/Book2221.xlsx"]
SOURCE_PATH = next((p for p in PREFER_PATHS if os.path.exists(p)), PREFER_PATHS[-1])
OUT_PATH = "/mnt/data/Book2221_minmax_textual_only.xlsx"

def _tokens(h: str) -> List[str]:
    return [t for t in re.split(r'[^A-Za-z0-9]+', str(h).strip().lower()) if t]

def is_min_header(h: str) -> bool:
    toks = set(_tokens(h))
    return any(t in toks for t in ("minimum","min","from"))

def is_max_header(h: str) -> bool:
    toks = set(_tokens(h))
    return any(t in toks for t in ("maximum","max","to"))

def is_age_header(h: str) -> bool:
    toks = set(_tokens(h))
    if "age" in toks or "ages" in toks:
        return True
    squashed = re.sub(r'[^a-z0-9]+','', str(h).strip().lower())
    if any(x in squashed for x in ("agerange","agegroup","ageband","agebracket","minage","maxage","ageyrs")):
        return True
    if "age" in toks and {"group","range","band","bracket"} & toks:
        return True
    return False

def categorize_headers(df: pd.DataFrame) -> Dict[str, List[str]]:
    min_cols, max_cols, age_cols = [], [], []
    for c in df.columns:
        if is_min_header(c): min_cols.append(c)
        if is_max_header(c): max_cols.append(c)
        if is_age_header(c): age_cols.append(c)
    return {"min_cols": min_cols, "max_cols": max_cols, "age_cols": age_cols}

def _is_pure_numeric(v: Any) -> bool:
    if isinstance(v, (int, float, np.integer, np.floating)):
        try:
            return math.isfinite(float(v))
        except Exception:
            return False
    if isinstance(v, str):
        return re.fullmatch(r'[+-]?\d+(?:\.\d+)?', v.strip()) is not None
    return False

def _days_to_years(d: float) -> float:
    return round(float(d) / 365.0, 4)

def parse_age_expression(value: Any) -> Tuple[Optional[float], Optional[float], str]:
    if value is None or (isinstance(value, float) and np.isnan(value)) or (isinstance(value, str) and value.strip()==""):
        return None, None, "empty"
    s = str(value).strip().lower()
    s = s.replace("–","-").replace("—","-")
    s = re.sub(r'\s+',' ', s)

    # special combos
    if re.search(r'\bfetal\b', s):
        return 0.0, 0.0, "special_fetal_0"

    if re.search(r'\bnewborns?\b', s) and re.search(r'\b(\d{1,3})\b', s):
        nums = [float(x) for x in re.findall(r'\b(\d{1,3})\b', s)]
        if nums:
            hi = nums[-1]
            if 0 <= hi <= MAX_AGE:
                return 0.0, hi, "special_newborn_and_number"

    # synonyms
    synonyms = [
        (r'\bnewborns?\b', (0.0, 0.0, "syn_newborn_0")),
        (r'\binfants?\b', (0.0, 1.0, "syn_infant_0_1y")),
        (r'\btoddlers?\b', (1.0, 3.0, "syn_toddler_1_3y")),
        (r'\bchildren\b|\bkids?\b', (0.0, 12.0, "syn_children_0_12y")),
        (r'\bteens?\b|\bteenagers?\b', (13.0, 19.0, "syn_teen_13_19y")),
        (r'\byoung adults?\b', (18.0, 24.0, "syn_young_adult_18_24y")),
        (r'\badults?\b|\badult only\b|\badults only\b', (18.0, MAX_AGE, "syn_adult_18_plus")),
        (r'\bseniors?\b|\belderly\b|\bold(er|est)? adults?\b', (60.0, MAX_AGE, "syn_senior_60_plus")),
    ]
    for pat, out in synonyms:
        if re.search(pat, s):
            return out

    # day-based
    m = re.fullmatch(r'(\d{1,3})\s*(days?|d)', s)
    if m:
        v = float(m.group(1))
        yrs = _days_to_years(v)
        return yrs, yrs, "days_exact_to_years"

    m = re.fullmatch(r'(\d{1,3})\s*(days?|d)\s*(?:-|to)\s*(\d{1,3})\s*(days?|d)', s)
    if m:
        a = float(m.group(1)); b = float(m.group(3))
        lo = _days_to_years(a); hi = _days_to_years(b)
        if lo <= hi:
            return lo, hi, "days_range_to_years"

    # numeric ranges and bounds
    m = re.fullmatch(r'(\d{1,3})\s*(?:-|to)\s*(\d{1,3})', s)
    if m:
        lo, hi = float(m.group(1)), float(m.group(2))
        if 0 <= lo <= hi <= MAX_AGE:
            return lo, hi, "range_numeric"

    m = re.fullmatch(r'between\s+(\d{1,3})\s+and\s+(\d{1,3})', s)
    if m:
        lo, hi = float(m.group(1)), float(m.group(2))
        if 0 <= lo <= hi <= MAX_AGE:
            return lo, hi, "range_between"

    m = re.fullmatch(r'(\d{1,3})\s*(\+|\*|and\s*(?:over|older|above|up)|&\s*older)', s)
    if m:
        base = float(m.group(1)); return base, MAX_AGE, "lower_bound_plus"

    m = re.fullmatch(r'(>=|≥|>)\s*(\d{1,3})', s)
    if m:
        op, num = m.group(1), float(m.group(2))
        base = num if op in (">=","≥") else num + 1.0
        return base, MAX_AGE, "lower_bound_ge_gt"

    m = re.fullmatch(r'(?:under|below|less\s*than)\s*(\d{1,3})', s)
    if m:
        hi = max(0.0, float(m.group(1)) - 1.0); return 0.0, hi, "upper_bound_under"

    m = re.fullmatch(r'(?:<=|≤|up\s*to|upto|to)\s*(\d{1,3})', s)
    if m:
        hi = float(m.group(1)); return 0.0, hi, "upper_bound_le_to"

    m = re.fullmatch(r'<\s*(\d{1,3})', s)
    if m:
        hi = max(0.0, float(m.group(1)) - 1.0); return 0.0, hi, "upper_bound_lt"

    # exact units
    m = re.fullmatch(r'(\d{1,3})\s*(years?|yrs?|ys?)', s)
    if m:
        v = float(m.group(1)); return v, v, "years_exact"

    m = re.fullmatch(r'(\d{1,3})\s*(months?|mos?|m)', s)
    if m:
        v = float(m.group(1)); yr = round(v/12.0, 2); return yr, yr, "months_exact"

    m = re.fullmatch(r'(\d{1,3})\s*(months?|mos?|m)\s*(?:-|to)\s*(\d{1,3})\s*(months?|mos?|m|years?|yrs?)', s)
    if m:
        a = float(m.group(1)); b = float(m.group(3)); right = m.group(4)
        lo = round(a/12.0, 2); hi = round(b/12.0, 2) if right.startswith(("m","mo")) else b
        if lo <= hi:
            return lo, hi, "range_months_to_?"

    m = re.fullmatch(r'(\d{1,3})\s*and\s*up', s)
    if m:
        base = float(m.group(1)); return base, MAX_AGE, "lower_bound_and_up"

    # fallback
    nums = re.findall(r'(\d{1,3})', s)
    if len(nums) == 2:
        lo, hi = float(nums[0]), float(nums[1])
        if 0 <= lo <= hi <= MAX_AGE:
            return lo, hi, "range_two_numbers_fallback"
    if len(nums) == 1:
        v = float(nums[0])
        if 0 <= v <= MAX_AGE:
            return v, v, "single_number_fallback"

    return None, None, "unparsed"

def first_non_null(row: pd.Series, cols: List[str]) -> Tuple[Optional[Any], Optional[str]]:
    for c in cols:
        if c in row and pd.notna(row[c]) and str(row[c]).strip() != "":
            return row[c], c
    return None, None

def transform_exact_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = ['_'.join([str(x) for x in tup if x is not None]) for tup in df.columns]

    cats = categorize_headers(df)
    min_cols, max_cols, age_cols = cats["min_cols"], cats["max_cols"], cats["age_cols"]

    # Choose the *first* detected min and max columns to copy raw into the two required output columns
    min_col_name = min_cols[0] if min_cols else None
    max_col_name = max_cols[0] if max_cols else None

    out_rows = []
    for _, row in df.iterrows():
        vmin = row[min_col_name] if min_col_name else None
        vmax = row[max_col_name] if max_col_name else None

        # Initialize outputs
        mn_out = np.nan
        mx_out = np.nan

        # Parse non-numeric only
        if vmin is not None and not _is_pure_numeric(vmin):
            mn, mx, rule = parse_age_expression(vmin)
            if mn is not None: mn_out = mn
            if mx is not None: mx_out = mx

        if vmax is not None and not _is_pure_numeric(vmax):
            mn, mx, rule = parse_age_expression(vmax)
            # Fill whichever sides are provided; do not overwrite already set values unless needed
            if mn is not None and (pd.isna(mn_out)): mn_out = mn
            if mx is not None: mx_out = mx  # max from max-side takes precedence

        # Fallback to generic age text ONLY if both still NaN
        if pd.isna(mn_out) and pd.isna(mx_out):
            vage, cage = first_non_null(row, age_cols)
            if vage is not None and not _is_pure_numeric(vage):
                mn, mx, rule = parse_age_expression(vage)
                if mn is not None: mn_out = mn
                if mx is not None: mx_out = mx

        out_rows.append({
            "Accepts Minimum Patient Age": vmin,
            "Accepts Maximum Patient Age": vmax,
            "min_age": mn_out,
            "max_age": mx_out
        })

    out = pd.DataFrame(out_rows, columns=[
        "Accepts Minimum Patient Age",
        "Accepts Maximum Patient Age",
        "min_age",
        "max_age"
    ])
    return out

# Execute and write
if os.path.exists(SOURCE_PATH):
    try:
        df_src = pd.read_excel(SOURCE_PATH)
    except Exception:
        df_src = pd.read_excel(SOURCE_PATH, engine="openpyxl")
    out_df = transform_exact_columns(df_src)
    with pd.ExcelWriter(OUT_PATH, engine="xlsxwriter") as xw:
        out_df.to_excel(xw, index=False, sheet_name="result")
    try:
        display_dataframe_to_user("Preview — Exact 4 columns (result)", out_df.head(50))
    except Exception as e:
        print("Preview display failed:", e)

OUT_PATH
