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
*************************************************
import pandas as pd
import re

# =========================
# USER CONFIG
# =========================
input_path = r"C:\Users\nitesh.kumar_spicemo\Downloads\09-30-2023 ECUP Active Roster.xlsx"
output_path = r"C:\Users\nitesh.kumar_spicemo\Downloads\09-30-2023 ECUP Active Roster_COMBINED_CLEANED.xlsx"
# =========================

def detect_header_row(df_no_header: pd.DataFrame, max_rows_to_check: int = 20) -> int:
    """Detect the header row based on the highest number of non-null cells."""
    rows_to_check = min(max_rows_to_check, len(df_no_header))
    non_null_counts = df_no_header.iloc[:rows_to_check].notna().sum(axis=1)
    return int(non_null_counts.idxmax())

def make_standard_name(original: str) -> str:
    """Map messy or inconsistent column names to clean standardized names."""
    if original is None:
        return ""
    o = str(original).strip().lower()
    if "primary" in o and ("special" in o or "specialty" in o):
        return "Speciality"
    if "secondary" in o and ("special" in o or "specialty" in o):
        return "Speciality"
    if re.search(r"\b(3rd|third|tertiary)\b", o):
        return "Speciality"
    if "first name" in o or (("name" in o) and ("first" in o or "given" in o)):
        return "Name"
    if "last name" in o or ("last" in o and "name" in o):
        return "Name"
    if "middle" in o:
        return "Name"
    if "name" in o and not ("group" in o):
        return "Name"
    if "dob" in o or "date of birth" in o:
        return "DOB"
    if "dea" in o:
        return "DEA"
    if "license" in o or "lic" in o:
        return "License"
    if "board" in o and "cert" in o:
        return "Board Certification"
    cleaned = re.sub(r"[^\w\s]", " ", str(original)).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.title()

def process_single_sheet(df_raw: pd.DataFrame, sheet_name: str):
    """Detect header row and clean a single sheet."""
    header_idx = detect_header_row(df_raw, max_rows_to_check=20)
    header_row = df_raw.iloc[header_idx].fillna("").astype(str).str.strip().tolist()
    data_df = df_raw.iloc[header_idx + 1 :].reset_index(drop=True)
    data_df.columns = header_row
    data_df = data_df.dropna(how="all")

    for c in list(data_df.columns):
        if data_df[c].dtype == object:
            data_df[c] = data_df[c].astype(str).str.strip().replace({"nan": pd.NA})

    # Rename blank columns
    new_cols = []
    for i, col in enumerate(data_df.columns):
        if col is None or str(col).strip() == "":
            new_cols.append(f"Column_{i+1}")
        else:
            new_cols.append(col)
    data_df.columns = new_cols

    data_df["header_row_number"] = header_idx + 1
    data_df["source_sheet"] = sheet_name
    return data_df

def make_unique_standardized_names(standardized_names):
    """
    Ensure standardized column names are unique:
    e.g. ["Name", "Name", "Name"] -> ["Name 1", "Name 2", "Name 3"]
    """
    counts = {}
    unique_names = []
    for name in standardized_names:
        if name not in counts:
            counts[name] = 1
            unique_names.append(name + " 1")
        else:
            counts[name] += 1
            unique_names.append(f"{name} {counts[name]}")
    return unique_names

def produce_two_row_header_columns(original_columns):
    """Build standardized + unique header and original header."""
    top = [make_standard_name(c) for c in original_columns]
    top = make_unique_standardized_names(top)  # ensure unique
    bottom = [("" if (c is None) else str(c)) for c in original_columns]
    return top, bottom

def main_combine_all_sheets(input_path: str, output_path: str):
    """Combine all sheets, clean data, remove duplicates, and standardize columns."""
    xls = pd.read_excel(input_path, sheet_name=None, header=None, engine="openpyxl")
    processed_frames = []

    for sheet_name, df_raw in xls.items():
        print(f"Processing sheet: {sheet_name} (raw shape {df_raw.shape})")
        try:
            df_clean = process_single_sheet(df_raw, sheet_name)
            if df_clean.shape[0] == 0:
                print(f"  -> sheet {sheet_name} has no data after header; skipped")
                continue
            processed_frames.append(df_clean)
        except Exception as e:
            print(f"  ! Error processing sheet {sheet_name}: {e}")
            continue

    if not processed_frames:
        raise SystemExit("No data found in any sheet to process.")

    combined = pd.concat(processed_frames, ignore_index=True, sort=False)
    for c in combined.columns:
        if combined[c].dtype == object:
            combined[c] = combined[c].astype(str).str.strip().replace({"nan": pd.NA})

    combined = combined.dropna(how="all", subset=[c for c in combined.columns if c not in ("header_row_number", "source_sheet")])

    tracking_cols = {"header_row_number", "source_sheet"}
    data_cols = [c for c in combined.columns if c not in tracking_cols]

    if not data_cols:
        combined["duplicate_count"] = 1
        combined["source_sheets"] = combined["source_sheet"]
        out_df = combined.drop(columns=["source_sheet"])
    else:
        grouped = combined.groupby(data_cols, dropna=False, as_index=False).agg(
            duplicate_count=("header_row_number", "size"),
            header_row_number=("header_row_number", "min"),
            source_sheets=("source_sheet", lambda s: ",".join(sorted(set(s.dropna().astype(str)))))
        )
        out_df = grouped

    if "duplicate_count" not in out_df.columns:
        out_df["duplicate_count"] = 1
    if "header_row_number" not in out_df.columns:
        out_df["header_row_number"] = pd.NA
    if "source_sheets" not in out_df.columns:
        out_df["source_sheets"] = ""

    final_cols = [c for c in data_cols] + ["duplicate_count", "header_row_number", "source_sheets"]
    out_df = out_df.loc[:, final_cols]

    # Build two-row unique headers
    orig_cols = list(out_df.columns)
    top_headers, bottom_headers = produce_two_row_header_columns(orig_cols)
    multi_cols = pd.MultiIndex.from_tuples(list(zip(top_headers, bottom_headers)))
    out_df.columns = multi_cols

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        out_df.to_excel(writer, sheet_name="Combined", index=False)

    print(f"✅ Combined output written to: {output_path}")

if __name__ == "__main__":
    main_combine_all_sheets(input_path, output_path)
