import re
import pandas as pd

# ========================
# CONFIG (EDIT THESE)
# ========================
INPUT_XLSX = r"C:\path\to\your_input.xlsx"
SHEET_NAME = None  # use None for the first sheet
OUTPUT_XLSX = r"C:\path\to\parsed_output.xlsx"

# ========================
# Lists
# ========================
org_keywords = [
    "health","MedCare","Medicine","Clinic","Hospital",
    "Medical","University","WakeMed","Center","Centre","Practice"
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

# ========================
# Parsing function
# ========================
def parse_header_value(s_raw):
    out = {"First_Name": "", "Middle_Name": "", "Last_Name": "", "Degree": "", "Organization": ""}

    if pd.isna(s_raw):
        return out

    s = str(s_raw).strip()
    s = re.sub(r'\s+', ' ', s)
    s = re.sub(r',\s*', ',', s)
    s = s.strip(' ,')

    # Organization detection
    s_lower = s.lower()
    for kw in org_keywords:
        if kw.lower() in s_lower:
            out["Organization"] = s
            return out

    # Extract degrees
    raw_degrees = [m.group(0).strip().strip('.') for m in re.finditer(degree_pattern, s, flags=re.IGNORECASE)]
    degrees_found, seen = [], set()
    for d in raw_degrees:
        key = canonical.get(d.upper().replace('.', ''), d.upper().replace('.', ''))
        if key not in seen:
            seen.add(key)
            pretty = canonical.get(d, canonical.get(d.upper().replace('.', ''), d.replace('.', '')))
            degrees_found.append(pretty)

    if degrees_found:
        s = re.sub(degree_pattern + r'(?:(?:\s*[/,&]\s*)|\s+|[.,])?', '', s, flags=re.IGNORECASE)
        s = s.strip(' ,')

    # Parse name
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
    df = pd.read_excel(input_xlsx, sheet_name=sheet_name, dtype=str)

    # ✅ FIX for "dict object has no attribute 'columns'"
    if not isinstance(df, pd.DataFrame):
        raise TypeError("The input file could not be read as a DataFrame. Check file and sheet name.")

    if 'header_col_value' not in df.columns:
        raise KeyError("Input Excel does not contain column 'header_col_value'")

    parsed_rows = [parse_header_value(val) for val in df['header_col_value']]
    parsed_df = pd.DataFrame(parsed_rows)

    result = pd.concat([df, parsed_df], axis=1)
    result.to_excel(output_xlsx, index=False)
    print(f"✅ Parsed results saved to: {output_xlsx}")
    return result

# ========================
# RUN
# ========================
if __name__ == "__main__":
    process_excel(INPUT_XLSX, OUTPUT_XLSX, sheet_name=SHEET_NAME)
