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
***********************************************************************************
import pandas as pd
import re
from collections import OrderedDict

# =========================
# USER CONFIG - edit paths
# =========================
input_path = r"C:\Users\nitesh.kumar_spicemo\Downloads\09-30-2023 ECUP Active Roster.xlsx"
output_path = r"C:\Users\nitesh.kumar_spicemo\Downloads\09-30-2023 ECUP Active Roster_DEGREE_SPEC_TAXONOMY_CLEANED.xlsx"
# =========================

# The three logical bases we care about
DESIRED_BASES = ["Degree", "Speciality", "Taxonomy"]

def detect_header_row(df_no_header: pd.DataFrame, max_rows_to_check: int = 20) -> int:
    rows_to_check = min(max_rows_to_check, len(df_no_header))
    non_null_counts = df_no_header.iloc[:rows_to_check].notna().sum(axis=1)
    return int(non_null_counts.idxmax())

def make_standard_name(original: str) -> str:
    """
    Map original column name to a base standardized name.
    Prioritize Degree, Speciality, Taxonomy detection.
    """
    if original is None:
        return ""
    o = str(original).strip().lower()

    # Degree / credentials detection (common tokens)
    if re.search(r"\b(degree|degree(s)?|credential|credentials|deg\b|md\b|m\.d\b|do\b|d\.o\b|phd\b|dr\b|dmd\b|dds\b|mbbs\b|mbchb\b|pa-c\b|pa\b|np\b|rn\b|bsc\b|msc\b|ms\b|mph\b|mds\b)\b", o, flags=re.I):
        return "Degree"

    # Taxonomy detection
    if re.search(r"\b(taxonomy|taxonomy code|tax code|tax)\b", o, flags=re.I):
        return "Taxonomy"

    # Speciality detection
    if ("special" in o) or ("specialty" in o) or ("specialisation" in o) or ("specialisation" in o):
        return "Speciality"

    # fall-back: return cleaned title (but we won't use fallbacks; only DESIRED_BASES are extracted)
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

def build_standardized_frame_for_three_fields(combined_df):
    """
    Build standardized DataFrame that contains only the desired bases:
      - Degree 1, Degree 2, ...
      - Speciality 1, Speciality 2, ...
      - Taxonomy 1, Taxonomy 2, ...
    The numbering respects the left-to-right first-seen order of original columns.
    Returns std_df and mapping_standard_to_originals (OrderedDict).
    """
    tracking = ["header_row_number", "source_sheet"]
    orig_cols_order = [c for c in combined_df.columns if c not in tracking]

    # Map each original column to a base standardized name
    orig_to_base = OrderedDict()
    for col in orig_cols_order:
        base = make_standard_name(col)
        orig_to_base[col] = base

    # For the desired bases, gather original columns that mapped to them
    base_to_originals = OrderedDict((b, []) for b in DESIRED_BASES)
    for col, base in orig_to_base.items():
        if base in DESIRED_BASES:
            base_to_originals[base].append(col)

    # Build standardized mapping: for each desired base, create numbered standardized columns
    mapping_standard_to_originals = OrderedDict()
    for base in DESIRED_BASES:
        originals = base_to_originals.get(base, [])
        for idx, orig_col in enumerate(originals, start=1):
            std_name = f"{base} {idx}"
            mapping_standard_to_originals[std_name] = [orig_col]

    # Create std_df by pulling values from the mapped original columns
    std_df = pd.DataFrame(index=combined_df.index)
    for std_name, originals in mapping_standard_to_originals.items():
        orig = originals[0]
        if orig in combined_df.columns:
            std_df[std_name] = combined_df[orig]
        else:
            std_df[std_name] = pd.NA

    # Ensure that if no columns found for a base we still provide at least one column (optional)
    # (Commented out by default; if you want Degree 1 present even if none found, uncomment)
    # for base in DESIRED_BASES:
    #     if not any(k.startswith(base + " ") for k in mapping_standard_to_originals):
    #         std_df[f"{base} 1"] = pd.NA
    #         mapping_standard_to_originals[f"{base} 1"] = []

    # Append tracking columns
    std_df["header_row_number"] = combined_df["header_row_number"].values
    std_df["source_sheet"] = combined_df["source_sheet"].values

    return std_df, mapping_standard_to_originals

def dedupe_and_audit(std_df):
    tracking_cols = {"header_row_number", "source_sheet"}
    data_cols = [c for c in std_df.columns if c not in tracking_cols]

    if not data_cols:
        std_df["duplicate_count"] = 1
        std_df["source_sheets"] = std_df["source_sheet"]
        out_df = std_df.drop(columns=["source_sheet"])
        duplicates_removed_df = pd.DataFrame(columns=list(out_df.columns) + ["removed_reason"])
        return out_df, duplicates_removed_df

    std_df["_group_key"] = std_df[data_cols].apply(lambda r: "||".join([("" if pd.isna(v) else str(v)) for v in r]), axis=1)
    std_df["_occurrence_idx"] = std_df.groupby("_group_key").cumcount() + 1

    duplicates_removed_df = std_df[std_df["_occurrence_idx"] > 1].copy()
    if not duplicates_removed_df.empty:
        duplicates_removed_df["removed_reason"] = "duplicate_of_group"

    grouped = (
        std_df.groupby(data_cols, dropna=False, as_index=False)
        .agg(
            duplicate_count=("header_row_number", "size"),
            header_row_number=("header_row_number", "min"),
            source_sheets=("source_sheet", lambda s: ",".join(sorted(set(s.dropna().astype(str)))))
        )
    )

    if "_group_key" in duplicates_removed_df.columns:
        duplicates_removed_df = duplicates_removed_df.rename(columns={"_group_key": "group_key", "_occurrence_idx": "occurrence_index"})
        audit_cols = data_cols + ["source_sheet", "header_row_number", "group_key", "occurrence_index", "removed_reason"]
        audit_cols = [c for c in audit_cols if c in duplicates_removed_df.columns]
        duplicates_removed_df = duplicates_removed_df.loc[:, audit_cols]

    return grouped, duplicates_removed_df

def write_excel_with_two_header_rows(out_df, mapping_standard_to_originals, output_path):
    out_df = out_df.copy().fillna("")

    # Determine std data cols (exclude tracking columns)
    std_cols = [c for c in out_df.columns if c not in ("duplicate_count", "header_row_number", "source_sheets")]

    top_row = []
    bottom_row = []
    for col in std_cols:
        top_row.append(col)
        originals = mapping_standard_to_originals.get(col, [])
        bottom_row.append(", ".join(originals) if originals else "")

    top_row += ["duplicate_count", "header_row_number", "source_sheets"]
    bottom_row += ["duplicate_count", "header_row_number", "source_sheets"]

    write_df = out_df.loc[:, std_cols + ["duplicate_count", "header_row_number", "source_sheets"]].copy().fillna("")

    with pd.ExcelWriter(output_path, engine="openpyxl", mode="w") as writer:
        empty_df = pd.DataFrame()
        empty_df.to_excel(writer, sheet_name="Combined", index=False, header=False, startrow=0)
        workbook = writer.book
        worksheet = writer.sheets["Combined"]

        for col_idx, val in enumerate(top_row):
            worksheet.cell(row=1, column=col_idx + 1, value=val)
        for col_idx, val in enumerate(bottom_row):
            worksheet.cell(row=2, column=col_idx + 1, value=val)

        write_df.to_excel(writer, sheet_name="Combined", index=False, header=False, startrow=2)

        # set modest column widths
        for i, col in enumerate(write_df.columns):
            try:
                maxlen = max(write_df[col].astype(str).map(len).max(), len(str(top_row[i])))
            except Exception:
                maxlen = len(str(top_row[i]))
            worksheet.column_dimensions[worksheet.cell(row=1, column=i + 1).column_letter].width = min(maxlen + 2, 50)

def main():
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

    # Build standardized frame containing only Degree / Speciality / Taxonomy columns
    std_df, mapping_standard_to_originals = build_standardized_frame_for_three_fields(combined)

    # Deduplicate on those standardized columns
    out_df, duplicates_removed_df = dedupe_and_audit(std_df)

    # Ensure metadata columns
    if "source_sheets" not in out_df.columns:
        out_df["source_sheets"] = ""
    if "duplicate_count" not in out_df.columns:
        out_df["duplicate_count"] = 1
    if "header_row_number" not in out_df.columns:
        out_df["header_row_number"] = pd.NA

    # Order: data cols then tracking
    tracking_cols = ["duplicate_count", "header_row_number", "source_sheets"]
    data_cols = [c for c in out_df.columns if c not in tracking_cols]
    final_cols = data_cols + tracking_cols
    out_df = out_df.loc[:, final_cols].fillna("")

    # Write Combined + Audit
    write_excel_with_two_header_rows(out_df, mapping_standard_to_originals, output_path)

    with pd.ExcelWriter(output_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        if not duplicates_removed_df.empty:
            duplicates_removed_df = duplicates_removed_df.fillna("")
            duplicates_removed_df.to_excel(writer, sheet_name="Duplicates_Removed", index=False)
        else:
            pd.DataFrame({"note": ["No duplicates removed"]}).to_excel(writer, sheet_name="Duplicates_Removed", index=False)

    print(f"✅ Combined (Degree/Speciality/Taxonomy) output written to: {output_path}")
    print("✅ Audit sheet 'Duplicates_Removed' saved (contains removed duplicate occurrences)")

if __name__ == "__main__":
    main()
************************************************************************
import re
import pandas as pd

# ----------------------------
# CONFIG
# ----------------------------
INPUT_XLSX = r"C:\path\to\input.xlsx"      # change to your file
SHEET_NAME = None                          # None => first sheet
OUTPUT_XLSX = r"C:\path\to\output_with_formats.xlsx"
SOURCE_COL = "header_col_value"
# ----------------------------

# Known degree/designation abbreviations (expand as needed)
DEGREES = [
    "MD", "MBBS", "DO", "PhD", "MSc", "MS", "MBA", "RN", "R\.?N\.?", "PA", "DVM",
    "OTR", "CHT", "DDS", "DMD", "BSc", "BPharm", "FRCS", "FRCP", "LPC", "CPA", "Esq"
]
# compile as alternation for regex (word boundaries)
DEGREE_RE = re.compile(r"\b(?:" + "|".join(DEGREES) + r")\b", flags=re.IGNORECASE)

# Organization / facility keywords (expand as needed)
ORG_KEYWORDS = [
    "hospital", "clinic", "center", "centre", "institute", "school", "college",
    "laboratory", "laboratories", "labs", "trust", "clinic", "pharmacy", "care",
    "health", "healthcare", "medical", "association", "hospitality", "lab", "university"
]
ORG_RE = re.compile(r"\b(?:" + "|".join(ORG_KEYWORDS) + r")\b", flags=re.IGNORECASE)

# Helper regexes for common formats
COMMA_FORMAT_RE = re.compile(r"^\s*([^,]+)\s*,\s*(.+)$")  # "Last, First ..." or "Org, Location"
PARENS_RE = re.compile(r"(.+?)\s*\((.+)\)\s*$")          # "Name (Affiliation)"
MULTIPLE_SPACES_RE = re.compile(r"\s+")
INITIAL_RE = re.compile(r"^[A-Z]\.?$", flags=re.IGNORECASE)  # single initial like "J." or "J"

# common tokens that strongly indicate organization rather than person (word-level)
ORG_STRONG_TOKENS = {"ltd", "pvt", "inc", "company", "co", "llc", "corp", "clinic", "hospital", "institute", "college", "university", "trust"}

def is_organization(text: str) -> bool:
    # quick heuristics for organisation:
    t = text.strip()
    # contains org keywords
    if ORG_RE.search(t):
        return True
    # contains company suffixes (Ltd, Inc etc.)
    words = re.findall(r"[A-Za-z0-9]+", t.lower())
    if any(w in ORG_STRONG_TOKENS for w in words):
        return True
    # many uppercase words (like "ABC HOSPITAL") or digits in name
    if sum(1 for ch in t if ch.isdigit()) >= 2:
        return True
    # if entire string ALL CAPS and longer than 2 words, likely org
    if t.isupper() and len(t.split()) > 1:
        return True
    return False

def extract_degrees(text: str):
    # returns list of degree tokens and string with them removed
    found = DEGREE_RE.findall(text)
    cleaned = DEGREE_RE.sub("", text).strip()
    # normalize found (uppercase, remove dots)
    found_norm = [re.sub(r"\.", "", f).upper() for f in found]
    return list(dict.fromkeys(found_norm)), MULTIPLE_SPACES_RE.sub(" ", cleaned)

def detect_format(raw: str):
    """Return a dict with detected_format and parsed components (best-effort)."""
    if raw is None:
        return {"detected_format": "empty", "first_name": None, "middle_name": None, "last_name": None, "designation": None, "organization": None}

    s = str(raw).strip()
    if s == "":
        return {"detected_format": "empty", "first_name": None, "middle_name": None, "last_name": None, "designation": None, "organization": None}

    # remove redundant whitespace
    s = MULTIPLE_SPACES_RE.sub(" ", s)

    # Extract degrees / designations
    degrees, s_no_deg = extract_degrees(s)

    # If the whole string looks like an organization, label it
    if is_organization(s_no_deg):
        return {
            "detected_format": "organization",
            "first_name": None,
            "middle_name": None,
            "last_name": None,
            "designation": ", ".join(degrees) if degrees else None,
            "organization": s_no_deg
        }

    # 1) Comma formats: "Last, First [Middle] [Degree]" or "Org, Location"
    m = COMMA_FORMAT_RE.match(s_no_deg)
    if m:
        left = m.group(1).strip()
        right = m.group(2).strip()
        # If left looks like last name (single token, maybe with Jr/Sr)
        left_tokens = left.split()
        right_tokens = right.split()
        # Heuristic: if right begins with a capitalized name token, assume "Last, First ..."
        # Also check that left is not organization-like
        if not is_organization(left):
            # right could be "First Middle"
            first = right_tokens[0] if right_tokens else None
            middle = " ".join(right_tokens[1:]) if len(right_tokens) > 1 else None
            return {
                "detected_format": "last_name,first_name" + (",degree" if degrees else ""),
                "first_name": first,
                "middle_name": middle,
                "last_name": left,
                "designation": ", ".join(degrees) if degrees else None,
                "organization": None
            }
        else:
            # left is an org
            return {
                "detected_format": "organization",
                "first_name": None,
                "middle_name": None,
                "last_name": None,
                "designation": ", ".join(degrees) if degrees else None,
                "organization": s_no_deg
            }

    # 2) Parentheses maybe indicate affiliation: "Name (Hospital ABC)" or "Org (City)"
    m2 = PARENS_RE.match(s_no_deg)
    if m2:
        before = m2.group(1).strip()
        inside = m2.group(2).strip()
        # if inside contains org keywords, label as person with org
        if ORG_RE.search(inside) or is_organization(inside):
            # parse person name in 'before' if possible
            parts = before.split()
            if len(parts) == 1:
                first = parts[0]
                last = None
            elif len(parts) == 2:
                first, last = parts
            else:
                first = parts[0]
                last = parts[-1]
                middle = " ".join(parts[1:-1]) if len(parts) > 2 else None
                return {
                    "detected_format": "first_name last_name (organization)",
                    "first_name": first,
                    "middle_name": middle,
                    "last_name": last,
                    "designation": ", ".join(degrees) if degrees else None,
                    "organization": inside
                }
            return {
                "detected_format": "first_name last_name (organization)",
                "first_name": first,
                "middle_name": None,
                "last_name": last,
                "designation": ", ".join(degrees) if degrees else None,
                "organization": inside
            }
        else:
            # parentheses but not an org -> treat as "Name (extra)"
            return {
                "detected_format": "unknown_with_parenthesis",
                "first_name": None,
                "middle_name": None,
                "last_name": None,
                "designation": ", ".join(degrees) if degrees else None,
                "organization": s_no_deg
            }

    # 3) Plain tokens separated by spaces -> assume "First Middle Last" or single token (organization or single name)
    tokens = s_no_deg.split()
    # Catch single token -> either single name or org
    if len(tokens) == 1:
        # if token contains words like hospital etc, organization else single name
        if is_organization(tokens[0]):
            return {
                "detected_format": "organization",
                "first_name": None, "middle_name": None, "last_name": None,
                "designation": ", ".join(degrees) if degrees else None,
                "organization": tokens[0]
            }
        else:
            return {
                "detected_format": "single_name",
                "first_name": tokens[0], "middle_name": None, "last_name": None,
                "designation": ", ".join(degrees) if degrees else None,
                "organization": None
            }

    # 4) Two tokens -> most likely "First Last"
    if len(tokens) == 2:
        return {
            "detected_format": "first_name last_name" + (",degree" if degrees else ""),
            "first_name": tokens[0],
            "middle_name": None,
            "last_name": tokens[1],
            "designation": ", ".join(degrees) if degrees else None,
            "organization": None
        }

    # 5) More than two tokens -> assume first, middle(s), last
    if len(tokens) >= 3:
        first = tokens[0]
        last = tokens[-1]
        middle = " ".join(tokens[1:-1])
        # edge: if tokens contain Jr/Sr etc at end, handle that (optional)
        suffixes = {"jr", "sr", "ii", "iii", "iv"}
        if last.lower().strip(".") in suffixes and len(tokens) >= 4:
            # shift suffix out
            suffix = last
            last = tokens[-2]
            middle = " ".join(tokens[1:-2]) if len(tokens) > 3 else None
            return {
                "detected_format": "first middle last, suffix" + (",degree" if degrees else ""),
                "first_name": first,
                "middle_name": middle,
                "last_name": last + " " + suffix,
                "designation": ", ".join(degrees) if degrees else None,
                "organization": None
            }
        return {
            "detected_format": "first_name middle_name last_name" + (",degree" if degrees else ""),
            "first_name": first,
            "middle_name": middle,
            "last_name": last,
            "designation": ", ".join(degrees) if degrees else None,
            "organization": None
        }

    # default fallback
    return {
        "detected_format": "other",
        "first_name": None,
        "middle_name": None,
        "last_name": None,
        "designation": ", ".join(degrees) if degrees else None,
        "organization": None
    }

def process_dataframe(df: pd.DataFrame, source_col: str = SOURCE_COL) -> pd.DataFrame:
    if source_col not in df.columns:
        raise ValueError(f"Column '{source_col}' not found in DataFrame")

    # Prepare lists for new columns
    detected = []
    firsts = []
    middles = []
    lasts = []
    designations = []
    orgs = []

    for val in df[source_col].tolist():
        res = detect_format(val)
        detected.append(res["detected_format"])
        firsts.append(res.get("first_name"))
        middles.append(res.get("middle_name"))
        lasts.append(res.get("last_name"))
        designations.append(res.get("designation"))
        orgs.append(res.get("organization"))

    # add columns
    df_out = df.copy()
    df_out["detected_format"] = detected
    df_out["first_name_parsed"] = firsts
    df_out["middle_name_parsed"] = middles
    df_out["last_name_parsed"] = lasts
    df_out["designation_parsed"] = designations
    df_out["organization_parsed"] = orgs
    return df_out

def main():
    print("Loading", INPUT_XLSX)
    df = pd.read_excel(INPUT_XLSX, sheet_name=SHEET_NAME, dtype=str)
    print(f"Rows read: {len(df)}")
    df_processed = process_dataframe(df, source_col=SOURCE_COL)
    print("Saving to", OUTPUT_XLSX)
    df_processed.to_excel(OUTPUT_XLSX, index=False)
    print("Done.")

if __name__ == "__main__":
    main()
