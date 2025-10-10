import pandas as pd
import re
from collections import OrderedDict, defaultdict

# =========================
# USER CONFIG - edit paths
# =========================
input_path = r"C:\Users\nitesh.kumar_spicemo\Downloads\09-30-2023 ECUP Active Roster.xlsx"
output_path = r"C:\Users\nitesh.kumar_spicemo\Downloads\09-30-2023 ECUP Active Roster_COMBINED_CLEANED.xlsx"
# =========================

def detect_header_row(df_no_header: pd.DataFrame, max_rows_to_check: int = 20) -> int:
    rows_to_check = min(max_rows_to_check, len(df_no_header))
    non_null_counts = df_no_header.iloc[:rows_to_check].notna().sum(axis=1)
    return int(non_null_counts.idxmax())

def make_standard_name(original: str) -> str:
    """Better-ordered mapping: check specific tokens first (fax/phone/email) then generic rules."""
    if original is None:
        return ""
    o = str(original).strip().lower()

    # specific contact telecom mappings (high priority)
    if re.search(r"\bfax\b", o) or re.search(r"\bfx\b", o):
        return "Fax"
    if re.search(r"\bphone\b", o) or re.search(r"\btelephone\b", o) or re.search(r"\btel\b", o) or re.search(r"\bcontact\b", o):
        return "Phone"
    if re.search(r"\bemail\b", o) or re.search(r"\bmail\b", o) or re.search(r"\be-mail\b", o):
        return "Email"

    # more explicit mappings for specialties, names, ids
    if "primary" in o and ("special" in o or "specialty" in o):
        return "Speciality"
    if "secondary" in o and ("special" in o or "specialty" in o):
        return "Speciality"
    if re.search(r"\b(3rd|third|tertiary)\b", o):
        # Only map to Speciality if the context contains 'special' or 'specialty'
        if "special" in o or "specialty" in o:
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
    if "board" in o and ("cert" in o or "certif" in o):
        return "Board Certification"

    cleaned = re.sub(r"[^\w\s]", " ", str(original)).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.title()

def process_single_sheet(df_raw: pd.DataFrame, sheet_name: str):
    header_idx = detect_header_row(df_raw, max_rows_to_check=20)
    header_row = df_raw.iloc[header_idx].fillna("").astype(str).str.strip().tolist()
    data_df = df_raw.iloc[header_idx + 1 :].reset_index(drop=True)
    data_df.columns = header_row
    data_df = data_df.dropna(how="all")

    # normalize text cells
    for c in list(data_df.columns):
        if data_df[c].dtype == object:
            data_df[c] = data_df[c].astype(str).str.strip().replace({"nan": pd.NA})

    # Rename blank column names
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
    counts = {}
    unique_names = []
    for name in standardized_names:
        # treat empty base name as "Column"
        base = name if name and name.strip() else "Column"
        if base not in counts:
            counts[base] = 1
            unique_names.append(f"{base} 1")
        else:
            counts[base] += 1
            unique_names.append(f"{base} {counts[base]}")
    return unique_names

def build_standardized_frame(combined_df):
    """
    Given the combined dataframe with many original columns (possibly duplicates representing same logical field),
    build a new dataframe whose columns are standardized unique names like "Name 1", "Name 2", ...
    The mapping respects the left-to-right order of first occurrence among columns.
    Returns:
      - standardized_df: DataFrame with standardized unique column names (plus tracking cols)
      - map_standard_to_originals: OrderedDict mapping standardized_col -> list(original_col_that_fed_it)
    """
    # Preserve tracking columns
    tracking = ["header_row_number", "source_sheet"]
    orig_cols_order = [c for c in combined_df.columns if c not in tracking]

    # Map each original column to a base standardized name (without numbering)
    orig_to_base = OrderedDict()
    base_first_seen_order = []  # to preserve order of base names as they appear
    for col in orig_cols_order:
        base = make_standard_name(col)
        if base is None or str(base).strip() == "":
            base = "Column"
        orig_to_base[col] = base
        if base not in base_first_seen_order:
            base_first_seen_order.append(base)

    # For each base, gather list of original columns that mapped to it (preserve first-seen ordering)
    base_to_originals = OrderedDict()
    for col, base in orig_to_base.items():
        base_to_originals.setdefault(base, []).append(col)

    # Build standardized column names unique numbering using the *appearance order* of original columns
    standardized_columns = []
    mapping_standard_to_originals = OrderedDict()
    for base, originals in base_to_originals.items():
        # For each original column that maps to this base, create a standardized column base + " n"
        for idx, orig_col in enumerate(originals, start=1):
            std_name = f"{base} {idx}"
            standardized_columns.append(std_name)
            mapping_standard_to_originals[std_name] = [orig_col]  # keep single origin per standardized column

    # Now create the standardized dataframe by taking values from original columns for each standardized column
    std_df = pd.DataFrame(index=combined_df.index)  # empty then fill
    for std_name, originals in mapping_standard_to_originals.items():
        orig = originals[0]  # the original column that fills this standardized slot
        # copy column if exists, else fill NaN
        if orig in combined_df.columns:
            std_df[std_name] = combined_df[orig]
        else:
            std_df[std_name] = pd.NA

    # Append tracking columns
    std_df["header_row_number"] = combined_df["header_row_number"].values
    std_df["source_sheet"] = combined_df["source_sheet"].values

    return std_df, mapping_standard_to_originals

def dedupe_and_audit(std_df):
    """
    Deduplicate across the standardized data columns (excluding tracking columns).
    Returns:
      - out_df: deduped aggregated DataFrame (one row per unique data combination) with
                duplicate_count, header_row_number (min), source_sheets aggregated
      - duplicates_removed_df: DataFrame containing rows that were removed (every occurrence except the kept one)
    """
    tracking_cols = {"header_row_number", "source_sheet"}
    data_cols = [c for c in std_df.columns if c not in tracking_cols]

    if not data_cols:
        # no data columns => trivial
        std_df["duplicate_count"] = 1
        std_df["source_sheets"] = std_df["source_sheet"]
        out_df = std_df.drop(columns=["source_sheet"])
        duplicates_removed_df = pd.DataFrame(columns=list(out_df.columns) + ["removed_reason"])
        return out_df, duplicates_removed_df

    # build group key
    std_df["_group_key"] = std_df[data_cols].apply(lambda r: "||".join([("" if pd.isna(v) else str(v)) for v in r]), axis=1)
    std_df["_occurrence_idx"] = std_df.groupby("_group_key").cumcount() + 1  # 1-based

    # rows to be removed -> occurrence_idx > 1
    duplicates_removed_df = std_df[std_df["_occurrence_idx"] > 1].copy()
    if not duplicates_removed_df.empty:
        duplicates_removed_df["removed_reason"] = "duplicate_of_group"

    # aggregate to produce final unique rows (keep first occurrence implicitly)
    grouped = (
        std_df.groupby(data_cols, dropna=False, as_index=False)
        .agg(
            duplicate_count=("header_row_number", "size"),
            header_row_number=("header_row_number", "min"),
            source_sheets=("source_sheet", lambda s: ",".join(sorted(set(s.dropna().astype(str)))))
        )
    )

    # cleanup helper cols in audit
    if "_group_key" in duplicates_removed_df.columns:
        duplicates_removed_df = duplicates_removed_df.rename(columns={"_group_key": "group_key", "_occurrence_idx": "occurrence_index"})
        # keep only data cols + metadata for audit (to keep audit readable)
        audit_cols = data_cols + ["source_sheet", "header_row_number", "group_key", "occurrence_index", "removed_reason"]
        # ensure columns exist
        audit_cols = [c for c in audit_cols if c in duplicates_removed_df.columns]
        duplicates_removed_df = duplicates_removed_df.loc[:, audit_cols]

    return grouped, duplicates_removed_df

def write_excel_with_two_header_rows(out_df, mapping_standard_to_originals, output_path):
    """
    Write the final out_df to Excel with two header rows:
      - Row 1: standardized unique names (out_df columns order)
      - Row 2: original column names (for each standardized column, we show the original column name that fed it)
    We avoid pandas MultiIndex to_excel issues by manually writing header rows, then writing data starting at row 3.
    Also replace pd.NA with empty strings before writing.
    """
    # solidify out_df (makes sure no pd.NA in values)
    out_df = out_df.copy()
    out_df = out_df.fillna("")

    # Build header rows lists
    std_cols = [c for c in out_df.columns if c not in ("duplicate_count", "header_row_number", "source_sheets")]
    # standardized header row (top)
    top_row = []
    bottom_row = []
    for col in std_cols:
        top_row.append(col)
        originals = mapping_standard_to_originals.get(col, [])
        bottom_row.append(", ".join(originals) if originals else "")

    # append the audit/tracking column names at end
    top_row += ["duplicate_count", "header_row_number", "source_sheets"]
    bottom_row += ["duplicate_count", "header_row_number", "source_sheets"]

    # Prepare a flattened DataFrame for writing (values only)
    write_df = out_df.loc[:, std_cols + ["duplicate_count", "header_row_number", "source_sheets"]].copy()
    write_df = write_df.fillna("")

    # Write using ExcelWriter; write header rows manually then dataframe starting row 3 (startrow=2)
    with pd.ExcelWriter(output_path, engine="openpyxl", mode="w") as writer:
        # Create an empty DataFrame to write to create the sheet
        empty_df = pd.DataFrame()
        empty_df.to_excel(writer, sheet_name="Combined", index=False, header=False, startrow=0)

        workbook = writer.book
        worksheet = writer.sheets["Combined"]

        # write the two header rows
        for col_idx, val in enumerate(top_row):
            worksheet.cell(row=1, column=col_idx + 1, value=val)
        for col_idx, val in enumerate(bottom_row):
            worksheet.cell(row=2, column=col_idx + 1, value=val)

        # Now write the data starting at row 3 using pandas (header=False, index=False, startrow=2)
        write_df.to_excel(writer, sheet_name="Combined", index=False, header=False, startrow=2)

        # Optionally, set column widths (light attempt)
        for i, col in enumerate(write_df.columns):
            maxlen = max(write_df[col].astype(str).map(len).max(), len(str(top_row[i])))
            worksheet.column_dimensions[worksheet.cell(row=1, column=i + 1).column_letter].width = min(maxlen + 2, 50)

        # Save workbook after Combined sheet; the writer context manager will save at the end

def main():
    # 1) Read all sheets (no header)
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

    # 2) Combine all sheets
    combined = pd.concat(processed_frames, ignore_index=True, sort=False)

    # 3) normalize string columns (again)
    for c in combined.columns:
        if combined[c].dtype == object:
            combined[c] = combined[c].astype(str).str.strip().replace({"nan": pd.NA})

    # drop fully empty rows (excluding tracking cols)
    combined = combined.dropna(how="all", subset=[c for c in combined.columns if c not in ("header_row_number", "source_sheet")])

    # 4) Build standardized dataframe (columns like "Name 1","Name 2"...)
    std_df, mapping_standard_to_originals = build_standardized_frame(combined)

    # 5) Deduplicate on standardized columns and create audit
    out_df, duplicates_removed_df = dedupe_and_audit(std_df)

    # 6) Fill missing metadata columns if not present
    if "source_sheets" not in out_df.columns:
        out_df["source_sheets"] = ""
    if "duplicate_count" not in out_df.columns:
        out_df["duplicate_count"] = 1
    if "header_row_number" not in out_df.columns:
        out_df["header_row_number"] = pd.NA

    # 7) Final ordering: data cols first, then duplicate_count, header_row_number, source_sheets
    tracking_cols = ["duplicate_count", "header_row_number", "source_sheets"]
    data_cols = [c for c in out_df.columns if c not in tracking_cols]
    final_cols = data_cols + tracking_cols
    out_df = out_df.loc[:, final_cols]

    # 8) Replace pd.NA with empty strings for writing
    out_df = out_df.fillna("")

    # 9) Write Combined sheet with two header rows and write Audit sheet
    # We'll write Combined first, then Duplicates_Removed (audit) as a normal sheet
    # For writing Combined we need mapping_standard_to_originals that maps each standardized col to its original source
    write_excel_with_two_header_rows(out_df, mapping_standard_to_originals, output_path)

    # Now append audit sheet (open the file and append)
    with pd.ExcelWriter(output_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        if not duplicates_removed_df.empty:
            # sanitize and replace pd.NA
            duplicates_removed_df = duplicates_removed_df.fillna("")
            duplicates_removed_df.to_excel(writer, sheet_name="Duplicates_Removed", index=False)
        else:
            pd.DataFrame({"note": ["No duplicates removed"]}).to_excel(writer, sheet_name="Duplicates_Removed", index=False)

    print(f"✅ Combined output written to: {output_path}")
    print(f"✅ Audit sheet 'Duplicates_Removed' saved (contains removed duplicate occurrences)")

if __name__ == "__main__":
    main()
