import pandas as pd
import re

# =========================
# USER CONFIG
# =========================
input_path = r"C:\Users\nitesh.kumar_spicemo\Downloads\09-30-2023 ECUP Active Roster.xlsx"  # path to source Excel
output_path = r"C:\Users\nitesh.kumar_spicemo\Downloads\09-30-2023 ECUP Active Roster_CLEANED.xlsx"
# =========================

def detect_header_row(df_no_header: pd.DataFrame, max_rows_to_check: int = 20) -> int:
    non_null_counts = df_no_header.iloc[:max_rows_to_check].notna().sum(axis=1)
    return int(non_null_counts.idxmax())

def make_standard_name(original: str) -> str:
    if original is None:
        return ""
    o = str(original).strip().lower()
    if "primary" in o and "special" in o:
        return "Speciality 1"
    if "secondary" in o and "special" in o:
        return "Speciality 2"
    if "third" in o or "3rd" in o:
        return "Speciality 3"
    if "name" in o and not "group" in o:
        return "Name"
    if "dob" in o:
        return "DOB"
    if "dea" in o:
        return "DEA #"
    if "license" in o:
        return "License"
    if "board" in o and "cert" in o:
        return "Board Certification"
    cleaned = re.sub(r"[^\w\s]", " ", str(original)).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.title()

def process_sheet(df_raw: pd.DataFrame):
    header_idx = detect_header_row(df_raw)
    header_row = df_raw.iloc[header_idx].fillna("").astype(str).str.strip().tolist()
    data_df = df_raw.iloc[header_idx + 1 :].reset_index(drop=True)
    data_df.columns = header_row
    data_df = data_df.dropna(how="all")

    for c in data_df.columns:
        if data_df[c].dtype == object:
            data_df[c] = data_df[c].astype(str).str.strip().replace({"nan": pd.NA})

    grouped = (
        data_df.groupby(list(data_df.columns), dropna=False, as_index=False)
        .size()
        .rename(columns={"size": "duplicate_count"})
    )
    grouped["header_row_number"] = header_idx + 1
    return grouped, header_idx + 1

def produce_two_row_header_columns(original_columns):
    top = [make_standard_name(c) for c in original_columns]
    bottom = [("" if (c is None) else str(c)) for c in original_columns]
    return top, bottom

xls = pd.read_excel(input_path, sheet_name=None, header=None, engine="openpyxl")
writer = pd.ExcelWriter(output_path, engine="openpyxl")

for sheet_name, df_raw in xls.items():
    print(f"Processing sheet: {sheet_name}")
    processed_df, header_row_num = process_sheet(df_raw)

    if "duplicate_count" not in processed_df.columns:
        processed_df["duplicate_count"] = 1
    if "header_row_number" not in processed_df.columns:
        processed_df["header_row_number"] = header_row_num

    cols = [c for c in processed_df.columns if c not in ("duplicate_count", "header_row_number")]
    processed_df = processed_df[cols + ["duplicate_count", "header_row_number"]]

    orig_cols = list(processed_df.columns)
    top_headers, bottom_headers = produce_two_row_header_columns(orig_cols)
    multi_cols = pd.MultiIndex.from_tuples(list(zip(top_headers, bottom_headers)))
    processed_df.columns = multi_cols

    processed_df.to_excel(writer, sheet_name=str(sheet_name), index=False)

writer.close()
print(f"✅ Output written to: {output_path}")
