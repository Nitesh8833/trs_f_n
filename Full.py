import re
import pandas as pd
import difflib

# ========================
# CONFIG (EDIT THESE)
# ========================
INPUT_XLSX = r"C:\path\to\your_input.xlsx"
SHEET_NAME = None  # None => first sheet
OUTPUT_XLSX = r"C:\path\to\parsed_output.xlsx"

# ========================
# Lists
# ========================
org_keywords = [
    "health","medcare","medicine","clinic","hospital",
    "medical","university","wakemed","center","centre","practice"
]

degree_list = [
    "MD","M.D.","DO","D.O.","PA","PA-C","APRN","DNP","PhD","Ph.D.","M.P.H.","MPH",
    "MS","M.S.","M.S.Ed","MS.Ed","Ed.D","EdD","CRNA","DPT","RN","OTR","BCBA",
    "RD","LD","FACOG","MBA","MSEd","III","II","IV"
]

degree_list_sorted = sorted(set(degree_list), key=lambda x: -len(x))
degree_pattern = r'\b(?:' + '|'.join(re.escape(d) for d in degree_list_sorted) + r')\b\.?'

canonical = {
    "M.D.": "MD", "MD": "MD",
    "D.O.": "DO", "DO": "DO",
    "Ph.D.": "PhD", "PhD": "PhD",
    "M.P.H.": "MPH", "MPH": "MPH",
    "M.S.": "MS", "MS": "MS",
    "M.S.Ed": "MSEd", "MS.Ed": "MSEd",
    "Ed.D": "EdD", "EdD": "EdD",
    "DNP": "DNP", "CRNA": "CRNA",
    "PA-C": "PA-C", "PA": "PA",
    "DPT": "DPT", "RN": "RN",
    "BCBA": "BCBA", "RD": "RD", "LD": "LD",
    "MBA": "MBA"
}

suffix_tokens = {"Jr", "Jr.", "Sr", "Sr.", "II", "III", "IV"}

# target logical column name we want to find
TARGET_COLUMN = "header_col_value"

# ========================
# Utility: normalize column names
# ========================
def normalize_colname(c):
    if c is None:
        return ""
    # strip, lower, replace whitespace and non-alphanumeric with underscore
    c = str(c).strip().lower()
    c = re.sub(r'[\s\-\./\\]+', '_', c)          # spaces, dashes, dots -> underscore
    c = re.sub(r'[^0-9a-z_]', '', c)             # remove other punctuation
    c = re.sub(r'_+', '_', c)                    # collapse multiple underscores
    c = c.strip('_')
    return c

# ========================
# Column autodetect
# ========================
def autodetect_header_column(df_columns, target=TARGET_COLUMN):
    # produce mapping normalized -> original
    norm_map = {normalize_colname(c): c for c in df_columns}
    norm_list = list(norm_map.keys())

    target_norm = normalize_colname(target)

    # 1) exact normalized match
    if target_norm in norm_map:
        chosen = norm_map[target_norm]
        print(f"Autodetect: exact normalized match -> '{chosen}'")
        return chosen

    # 2) substring/keyword heuristics: look for cols containing both 'header' and 'value' or 'header' & 'col'
    for norm_c, orig in norm_map.items():
        if ('header' in norm_c and 'value' in norm_c) or ('header' in norm_c and 'col' in norm_c):
            print(f"Autodetect: heuristic substring match -> '{orig}' (normalized='{norm_c}')")
            return orig

    # 3) substring: contains 'header' or contains 'value' (prefer 'header')
    header_candidates = [orig for norm_c, orig in norm_map.items() if 'header' in norm_c]
    value_candidates = [orig for norm_c, orig in norm_map.items() if 'value' in norm_c]
    col_candidates = [orig for norm_c, orig in norm_map.items() if 'col' in norm_c]

    if header_candidates:
        print(f"Autodetect: choosing first header-like column -> '{header_candidates[0]}'")
        return header_candidates[0]
    if value_candidates:
        print(f"Autodetect: choosing first value-like column -> '{value_candidates[0]}'")
        return value_candidates[0]
    if col_candidates:
        print(f"Autodetect: choosing first col-like column -> '{col_candidates[0]}'")
        return col_candidates[0]

    # 4) fuzzy match with difflib
    # try to match against the raw original names too (use both)
    raw_cols = list(df_columns)
    attempt_space = target.replace('_', ' ')
    candidates = difflib.get_close_matches(attempt_space, raw_cols, n=3, cutoff=0.6)
    if candidates:
        print(f"Autodetect: fuzzy match against original names -> candidates: {candidates}")
        print(f"Autodetect: choosing '{candidates[0]}'")
        return candidates[0]

    # try normalized fuzzy
    candidates_norm = difflib.get_close_matches(target_norm, norm_list, n=3, cutoff=0.6)
    if candidates_norm:
        chosen = norm_map[candidates_norm[0]]
        print(f"Autodetect: fuzzy match on normalized names -> '{chosen}'")
        return chosen

    # final fallback: nothing found
    raise KeyError(
        f"Could not autodetect a column similar to '{target}'. Available columns: {list(df_columns)}"
    )

# ========================
# Parsing function (same as before)
# ========================
def parse_header_value(s_raw):
    out = {"First_Name": "", "Middle_Name": "", "Last_Name": "", "Degree": "", "Organization": ""}

    if pd.isna(s_raw):
        return out

    s = str(s_raw).strip()
    s = re.sub(r'\s+', ' ', s)
    s = re.sub(r',\s*', ',', s)
    s = s.strip(' ,')

    # Organization detection (use normalized lowercase substring match)
    s_lower = s.lower()
    for kw in org_keywords:
        if kw.lower() in s_lower:
            out["Organization"] = s
            return out

    # Extract degrees (may be multiple)
    raw_degrees = [m.group(0).strip().strip('.') for m in re.finditer(degree_pattern, s, flags=re.IGNORECASE)]
    degrees_found, seen = [], set()
    for d in raw_degrees:
        key = canonical.get(d.upper().replace('.', ''), d.upper().replace('.', ''))
        if key not in seen:
            seen.add(key)
            pretty = canonical.get(d, canonical.get(d.upper().replace('.', ''), d.replace('.', '')))
            degrees_found.append(pretty)

    if degrees_found:
        # remove degree tokens and trailing punctuation/commas
        s = re.sub(degree_pattern + r'(?:(?:\s*[/,&]\s*)|\s+|[.,])?', '', s, flags=re.IGNORECASE)
        s = s.strip(' ,')

    # Parse name content
    if ',' in s:
        parts = [p.strip() for p in s.split(',') if p.strip()]
        filtered_parts = [p for p in parts if not re.search(degree_pattern, p, flags=re.IGNORECASE)]
        parts = filtered_parts

        if len(parts) == 1:
            tokens = parts[0].split()
            if len(tokens) == 1:
                out["Last_Name"] = tokens[0]
            elif len(tokens) == 2:
                out["First_Name"], out["Last_Name"] = tokens
            else:
                out["First_Name"] = tokens[0]
                out["Middle_Name"] = " ".join(tokens[1:-1])
                out["Last_Name"] = tokens[-1]
        else:
            out["Last_Name"] = parts[0]
            remainder = " ".join(parts[1:])
            rem_tokens = remainder.split()
            if len(rem_tokens) == 1:
                out["First_Name"] = rem_tokens[0]
            elif len(rem_tokens) >= 2:
                out["First_Name"] = rem_tokens[0]
                out["Middle_Name"] = " ".join(rem_tokens[1:])
    else:
        tokens = s.split()
        if len(tokens) == 1:
            out["Last_Name"] = tokens[0]
        elif len(tokens) == 2:
            out["First_Name"], out["Last_Name"] = tokens
        else:
            out["First_Name"] = tokens[0]
            if tokens[-1].replace('.', '') in suffix_tokens:
                out["Last_Name"] = " ".join(tokens[-2:])
                out["Middle_Name"] = " ".join(tokens[1:-2])
            else:
                out["Last_Name"] = tokens[-1]
                out["Middle_Name"] = " ".join(tokens[1:-1])

    for k in ("First_Name", "Middle_Name", "Last_Name"):
        out[k] = out[k].strip(" .,")

    if degrees_found:
        out["Degree"] = ", ".join(degrees_found)

    if not (out["First_Name"] or out["Last_Name"] or out["Middle_Name"]) and not out["Organization"]:
        out["Organization"] = s_raw

    return out

# ========================
# Process Excel
# ========================
def process_excel(input_xlsx, output_xlsx, sheet_name=None):
    # Read sheet(s)
    # Using sheet_name=None returns dict of DataFrames; handle both cases
    data = pd.read_excel(input_xlsx, sheet_name=sheet_name, dtype=str)

    # If pandas returned a dict (multiple sheets), pick the requested sheet or the first one
    if isinstance(data, dict):
        if sheet_name is None:
            # take the first sheet
            first_sheet = list(data.keys())[0]
            df = data[first_sheet]
            print(f"Loaded first sheet: '{first_sheet}'")
        else:
            if sheet_name in data:
                df = data[sheet_name]
                print(f"Loaded sheet: '{sheet_name}'")
            else:
                raise KeyError(f"Sheet '{sheet_name}' not found. Available sheets: {list(data.keys())}")
    elif isinstance(data, pd.DataFrame):
        df = data
    else:
        raise TypeError("Could not read the Excel file into a DataFrame.")

    # Save original columns for debugging
    original_columns = list(df.columns)
    print("Original columns found:", original_columns)

    # Standardize column names
    norm_to_orig = {normalize_colname(c): c for c in original_columns}
    df.columns = [normalize_colname(c) for c in original_columns]

    # Try to find the correct column automatically
    try:
        chosen_orig = autodetect_header_column(original_columns, target=TARGET_COLUMN)
        # map chosen_orig to the normalized name used in df (since df.columns are normalized)
        chosen_norm = normalize_colname(chosen_orig)
        print(f"Using column: '{chosen_orig}' (normalized as '{chosen_norm}') for parsing")
    except KeyError as e:
        # Provide helpful debug info
        print(str(e))
        print("Normalized available columns:", list(df.columns))
        raise

    # now parse values from the chosen normalized column
    parsed_rows = [parse_header_value(val) for val in df[chosen_norm]]
    parsed_df = pd.DataFrame(parsed_rows, index=df.index)

    # to preserve original dataframe's columns (unnormalized), restore them in result
    df_result = df.copy()
    # rename df_result columns back to original names for neatness
    df_result.columns = original_columns

    # append parsed columns
    result = pd.concat([df_result, parsed_df], axis=1)

    result.to_excel(output_xlsx, index=False)
    print(f"✅ Parsed results saved to: {output_xlsx}")
    return result

# ========================
# RUN
# ========================
if __name__ == "__main__":
    process_excel(INPUT_XLSX, OUTPUT_XLSX, sheet_name=SHEET_NAME)
