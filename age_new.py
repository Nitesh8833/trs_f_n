# Modify the transformation to ensure:
# - Patterns that yield BOTH bounds (e.g., "100+", "newborn and 10", ranges, days-ranges) will populate BOTH min_age and max_age
#   regardless of whether the text was found in a Min/Max/Generic Age column.
# - "fetal" in generic Age sets both to 0; in min/max sets that respective side to 0 (existing behavior retained).
# Then re-run and write the Excel.

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
    """Parse textual/complex age values to (min, max, rule)."""
    if value is None or (isinstance(value, float) and np.isnan(value)) or (isinstance(value, str) and value.strip()==""):
        return None, None, "empty"
    s = str(value).strip().lower()
    s = s.replace("–","-").replace("—","-")
    s = re.sub(r'\s+',' ', s)

    # --- special combos first ---
    if re.search(r'\bfetal\b', s):
        return 0.0, 0.0, "special_fetal_0"

    if re.search(r'\bnewborns?\b', s) and re.search(r'\b(\d{1,3})\b', s):
        nums = [float(x) for x in re.findall(r'\b(\d{1,3})\b', s)]
        if nums:
            hi = nums[-1]
            if 0 <= hi <= MAX_AGE:
                return 0.0, hi, "special_newborn_and_number"

    # --- synonyms ---
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

    # --- day-based patterns ---
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

    # numeric range in years
    m = re.fullmatch(r'(\d{1,3})\s*(?:-|to)\s*(\d{1,3})', s)
    if m:
        lo, hi = float(m.group(1)), float(m.group(2))
        if 0 <= lo <= hi <= MAX_AGE:
            return lo, hi, "range_numeric"

    # between X and Y
    m = re.fullmatch(r'between\s+(\d{1,3})\s+and\s+(\d{1,3})', s)
    if m:
        lo, hi = float(m.group(1)), float(m.group(2))
        if 0 <= lo <= hi <= MAX_AGE:
            return lo, hi, "range_between"

    # lower-bounds (handles "100+","100*","100 and over/older/above/up","& older")
    m = re.fullmatch(r'(\d{1,3})\s*(\+|\*|and\s*(?:over|older|above|up)|&\s*older)', s)
    if m:
        base = float(m.group(1)); return base, MAX_AGE, "lower_bound_plus"

    m = re.fullmatch(r'(>=|≥|>)\s*(\d{1,3})', s)
    if m:
        op, num = m.group(1), float(m.group(2))
        base = num if op in (">=","≥") else num + 1.0
        return base, MAX_AGE, "lower_bound_ge_gt"

    # upper-bounds
    m = re.fullmatch(r'(?:under|below|less\s*than)\s*(\d{1,3})', s)
    if m:
        hi = max(0.0, float(m.group(1)) - 1.0); return 0.0, hi, "upper_bound_under"

    m = re.fullmatch(r'(?:<=|≤|up\s*to|upto|to)\s*(\d{1,3})', s)
    if m:
        hi = float(m.group(1)); return 0.0, hi, "upper_bound_le_to"

    m = re.fullmatch(r'<\s*(\d{1,3})', s)
    if m:
        hi = max(0.0, float(m.group(1)) - 1.0); return 0.0, hi, "upper_bound_lt"

    # exact with units (years/months)
    m = re.fullmatch(r'(\d{1,3})\s*(years?|yrs?|ys?)', s)
    if m:
        v = float(m.group(1)); return v, v, "years_exact"

    m = re.fullmatch(r'(\d{1,3})\s*(months?|mos?|m)', s)
    if m:
        v = float(m.group(1)); yr = round(v/12.0, 2); return yr, yr, "months_exact"

    # mixed month ranges
    m = re.fullmatch(r'(\d{1,3})\s*(months?|mos?|m)\s*(?:-|to)\s*(\d{1,3})\s*(months?|mos?|m|years?|yrs?)', s)
    if m:
        a = float(m.group(1)); b = float(m.group(3)); right = m.group(4)
        lo = round(a/12.0, 2); hi = round(b/12.0, 2) if right.startswith(("m","mo")) else b
        if lo <= hi:
            return lo, hi, "range_months_to_?"

    # "x and up"
    m = re.fullmatch(r'(\d{1,3})\s*and\s*up', s)
    if m:
        base = float(m.group(1)); return base, MAX_AGE, "lower_bound_and_up"

    # fallback with numbers present
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

def transform_v3(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = ['_'.join([str(x) for x in tup if x is not None]) for tup in df.columns]

    cats = categorize_headers(df)
    min_cols, max_cols, age_cols = cats["min_cols"], cats["max_cols"], cats["age_cols"]

    out = df.copy()
    out["min_source_col"] = None
    out["max_source_col"] = None
    out["min_raw"] = None
    out["max_raw"] = None
    out["min_age"] = np.nan
    out["max_age"] = np.nan
    out["rule_min"] = None
    out["rule_max"] = None

    for i, row in out.iterrows():
        vmin, cmin = first_non_null(row, min_cols)
        vmax, cmax = first_non_null(row, max_cols)
        vage, cage = first_non_null(row, age_cols)

        out.at[i, "min_source_col"] = cmin
        out.at[i, "max_source_col"] = cmax
        out.at[i, "min_raw"] = vmin
        out.at[i, "max_raw"] = vmax

        # We'll merge results from any source that yields both bounds.
        mn_out = np.nan
        mx_out = np.nan
        rule_min = None
        rule_max = None

        # MIN side text
        if vmin is not None and not _is_pure_numeric(vmin):
            mn, mx, rule = parse_age_expression(vmin)
            if mn is not None:
                mn_out = mn
                rule_min = rule
            if mx is not None:
                mx_out = mx
                # if min text implied a max (e.g., "100+"), capture rule
                rule_max = rule

        # MAX side text
        if vmax is not None and not _is_pure_numeric(vmax):
            mn, mx, rule = parse_age_expression(vmax)
            if mx is not None:
                mx_out = mx
                rule_max = rule if rule_max is None else rule_max
            if mn is not None and np.isnan(mn_out):
                # If max text implies a minimum (e.g., a range like "10-20" mistakenly in max),
                # fill it to meet the user's examples where both should show.
                mn_out = mn
                rule_min = rule if rule_min is None else rule_min

        # Fallback generic age text if both still NaN
        if (np.isnan(mn_out) and np.isnan(mx_out)) and (vage is not None) and (not _is_pure_numeric(vage)):
            mn, mx, rule = parse_age_expression(vage)
            if mn is not None:
                mn_out = mn
                rule_min = f"age_{rule}" if rule_min is None else rule_min
            if mx is not None:
                mx_out = mx
                rule_max = f"age_{rule}" if rule_max is None else rule_max

        # Assign to output while preserving "numeric => blank" rule
        out.at[i, "min_age"] = mn_out
        out.at[i, "max_age"] = mx_out
        out.at[i, "rule_min"] = rule_min
        out.at[i, "rule_max"] = rule_max

    diag = pd.DataFrame({
        "detected_min_columns": [", ".join(min_cols) if min_cols else "<none>"],
        "detected_max_columns": [", ".join(max_cols) if max_cols else "<none>"],
        "detected_age_columns": [", ".join(age_cols) if age_cols else "<none>"],
        "note": [
            "Patterns like '100+' or 'newborn and 10' now populate BOTH min_age & max_age even if found in a Min/Max column; "
            "pure numeric entries still leave min_age/max_age blank."
        ],
        "source_file_used": [SOURCE_PATH],
        "output_file_created": [OUT_PATH],
    })
    return out, diag

# Run
created_path = None
if os.path.exists(SOURCE_PATH):
    try:
        df_src = pd.read_excel(SOURCE_PATH)
    except Exception:
        df_src = pd.read_excel(SOURCE_PATH, engine="openpyxl")
    out_df, diag_df = transform_v3(df_src)
    with pd.ExcelWriter(OUT_PATH, engine="xlsxwriter") as xw:
        out_df.to_excel(xw, index=False, sheet_name="result")
        diag_df.to_excel(xw, index=False, sheet_name="diagnostics")
    created_path = OUT_PATH
    try:
        display_dataframe_to_user("Preview — Book2221_minmax_textual_only.xlsx (both-bounds from any column)", out_df.head(50))
    except Exception as e:
        print("Preview display failed:", e)

created_path
