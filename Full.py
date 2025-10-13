import re
import pandas as pd

# -----------------------------
# CONFIG - change these paths
# -----------------------------
INPUT_XLSX = r"C:\path\to\your_input.xlsx"
SHEET_NAME = None  # None = first sheet
OUTPUT_XLSX = r"C:\path\to\parsed_output.xlsx"

# -----------------------------
# Helper lists
# -----------------------------
org_keywords = [
    "health", "MedCare", "Medicine", "Clinic", "Hospital", 
    "Medical", "University", "WakeMed", "Center", "Centre", "Practice"
]

degree_list = [
    "MD", "M.D.", "DO", "D.O.", "PA", "PA-C", "APRN", "DNP", "PhD", "Ph.D.",
    "M.P.H.", "MPH", "MS", "M.S.", "M.S.Ed", "MS.Ed", "Ed.D", "EdD", "CRNA",
    "DPT", "RN", "OTR", "BCBA", "RD", "LD", "FACOG", "MBA", "BSc", "MBBS",
    "III", "II", "IV"
]

# Normalize degree tokens for regex
degree_list_sorted = sorted(set(degree_list), key=lambda x: -len(x))
degree_pattern = r'\b(?:' + '|'.join(re.escape(d) for d in degree_list_sorted) + r')\b\.?'

suffix_tokens = {"Jr", "Jr.", "Sr", "Sr.", "II", "III", "IV"}

# ----------------------------------------
# Parsing function (core logic)
# ----------------------------------------
def parse_header_value(s_raw: str):
    out = {"First_Name": "", "Middle_Name": "", "Last_Name": "", "Degree": "", "Organization": ""}

    if pd.isna(s_raw):
        return out

    s = str(s_raw).strip()
    s = re.sub(r'\s+', ' ', s)
    s = re.sub(r',\s*', ',', s)
    s = s.strip(' ,')

    # Detect organizations
    s_lower = s.lower()
    for kw in org_keywords:
        if kw.lower() in s_lower:
            out["Organization"] = s
            return out

    # Extract degrees (multiple allowed)
    degrees_found = []
    for m in re.finditer(degree_pattern, s, flags=re.IGNORECASE):
        degrees_found.append(m.group(0).strip().strip('.'))

    if degrees_found:
        # Remove degree tokens from string
        s = re.sub(degree_pattern + r'[.,\s]*', '', s, flags=re.IGNORECASE).strip(' ,')

    # Parse names
    if ',' in s:
        parts = [p.strip() for p in s.split(',') if p.strip()]
        filtered_parts = [p for p in parts if not re.search(degree_pattern, p, flags=re.IGNORECASE)]
        parts = filtered_parts

        if len(parts) == 1:
            tokens = parts[0].split()
            if len(tokens) == 1:
                out["Last_Name"] = tokens[0]
            elif len(tokens) == 2:
                out["First_Name"], out["Last_Name"] = tokens[0], tokens[1]
            else:
                out["First_Name"] = tokens[0]
                out["Last_Name"] = tokens[-1]
                out["Middle_Name"] = " ".join(tokens[1:-1])
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
            out["First_Name"], out["Last_Name"] = tokens[0], tokens[1]
        else:
            out["First_Name"] = tokens[0]
            if tokens[-1].replace('.', '') in suffix_tokens:
                out["Last_Name"] = " ".join(tokens[-2:])
                out["Middle_Name"] = " ".join(tokens[1:-2]) if len(tokens) > 3 else ""
            else:
                out["Last_Name"] = tokens[-1]
                if len(tokens) > 2:
                    out["Middle_Name"] = " ".join(tokens[1:-1])

    # Clean punctuation
    for k in ("First_Name", "Middle_Name", "Last_Name"):
        out[k] = out[k].strip(" .,")

    # Handle multiple degrees (comma-separated)
    if degrees_found:
        cleaned = []
        for d in degrees_found:
            dd = d.strip().replace('.', '')
            if dd.upper() not in (x.upper().replace('.', '') for x in cleaned):
                cleaned.append(d.strip().strip('.'))
        out["Degree"] = ", ".join(cleaned)

    # If no name detected, fallback to Organization
    if not (out["First_Name"] or out["Last_Name"] or out["Middle_Name"]) and not out["Organization"]:
        out["Organization"] = s_raw

    return out

# ----------------------------------------
# Process Excel file
# ----------------------------------------
def process_excel(input_xlsx, output_xlsx, sheet_name=None):
    df = pd.read_excel(input_xlsx, sheet_name=sheet_name, dtype=str)
    if 'header_col_value' not in df.columns:
        raise KeyError("Input Excel does not contain column 'header_col_value'")

    parsed_rows = []
    for idx, val in df['header_col_value'].items():
        parsed_rows.append(parse_header_value(val))

    parsed_df = pd.DataFrame(parsed_rows, index=df.index)
    result = pd.concat([df, parsed_df], axis=1)

    result.to_excel(output_xlsx, index=False)
    print(f"✅ Parsed output saved to: {output_xlsx}")
    return result

# Run main
if __name__ == "__main__":
    out_df = process_excel(INPUT_XLSX, OUTPUT_XLSX, sheet_name=SHEET_NAME)
