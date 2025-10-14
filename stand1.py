import pandas as pd
import re
from collections import OrderedDict

# =========================
# USER CONFIG - edit paths
# =========================
input_path = r"C:\Users\nitesh.kumar_spicemo\Downloads\09-30-2023 ECUP Active Roster.xlsx"
output_path = r"C:\Users\nitesh.kumar_spicemo\Downloads\09-30-2023 ECUP Active Roster_DEGREE_SPEC_TAXONOMY_ROLE_CLEANED.xlsx"
# =========================

# The four logical bases we care about (Role added)
DESIRED_BASES = ["Degree", "Speciality", "Taxonomy", "Role"]

def detect_header_row(df_no_header: pd.DataFrame, max_rows_to_check: int = 20) -> int:
    rows_to_check = min(max_rows_to_check, len(df_no_header))
    non_null_counts = df_no_header.iloc[:rows_to_check].notna().sum(axis=1)
    return int(non_null_counts.idxmax())

def header_word_count(original: str) -> int:
    if original is None:
        return 0
    # count word-like tokens (letters/digits/underscore)
    tokens = re.findall(r"\w+", str(original).strip())
    return len(tokens)

def make_standard_name(original: str) -> str:
    """
    Map original column name to a base standardized name.
    Priority:
      1) Degree
      2) Role  (columns with ROLE, SPEC, PCP, primary care, etc.)
      3) Taxonomy (must contain 'taxonomy' or 'taxonomy code' / 'tax code' — NOT 'tax')
      4) Speciality (only if header word-count <= 3 when header contains speciality token)
    """
    if original is None:
        return ""
    o = str(original).strip().lower()

    # Degree / credentials detection (common tokens)
    if re.search(r"\b(degree|degrees|credential|credentials|deg\b|md\b|m\.d\b|do\b|d\.o\b|phd\b|dr\b|dmd\b|dds\b|mbbs\b|mbchb\b|pa-c\b|pa\b|np\b|rn\b|bsc\b|msc\b|ms\b|mph\b|mds\b)\b", o, flags=re.I):
        return "Degree"

    # Role detection BEFORE Speciality: include ROLE, SPEC (short token), PCP, primary care tokens
    if re.search(r"\b(role|spec\b|pcp\b|primary care|primarycare|primary-care)\b", o, flags=re.I):
        # header-level rule: if header has more than 3 words, skip mapping to Role
        if header_word_count(original) <= 3:
            return "Role"
        else:
            return ""  # don't map if header is too long

    # Taxonomy detection: require 'taxonomy' (or explicit 'taxonomy code', 'tax code') — do NOT match plain 'tax'
    if re.search(r"\b(taxonomy|taxonomy code|tax code)\b", o, flags=re.I):
        return "Taxonomy"

    # Speciality detection (fall back after Role so 'spec' mapped to Role as requested)
    if re.search(r"\b(special|specialty|speciality|specialisation|specialization)\b", o, flags=re.I):
        # header-level rule: if header has more than 3 words, skip mapping to Speciality
        if header_word_count(original) <= 3:
            return "Speciality"
        else:
            return ""  # don't map if header is too long

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
      - Role 1, Role 2, ...
    Numbering respects left-to-right first-seen order of original columns.
    Applies header- and value-level rules:
      - Headers with >3 words are excluded from mapping to Speciality or Role even if they contain the token.
      - For Speciality and Role: when populating values, discard any cell with >3 words (set to pd.NA).
    Returns std_df and mapping_standard_to_originals (OrderedDict).
    """
    tracking = ["header_row_number", "source_sheet"]
    orig_cols_order = [c for c in combined_df.columns if c not in tracking]

    # Map each original column to a base standardized name (respecting header-level word-count rules within make_standard_name)
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

        # Apply value-level rules:
        base = std_name.split()[0]  # 'Speciality', 'Role', etc.
        if base in ("Speciality", "Role"):
            def _filter_val(v):
                if pd.isna(v) or str(v).strip() == "":
                    return pd.NA
                # count words (split on whitespace). If >3 words, discard (NA)
                # use regex to count word tokens to avoid counting punctuation
                tokens = re.findall(r"\w+", str(v).strip())
                return v if len(tokens) <= 3 else pd.NA
            std_df[std_name] = std_df[std_name].apply(_filter_val)

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

    # Build standardized frame containing Degree / Speciality / Taxonomy / Role columns
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

    print(f"✅ Combined (Degree/Speciality/Taxonomy/Role) output written to: {output_path}")
    print("✅ Audit sheet 'Duplicates_Removed' saved (contains removed duplicate occurrences)")

if __name__ == "__main__":
    main()
    ***************************************************
    import pandas as pd
import re
from collections import OrderedDict

# =========================
# USER CONFIG - edit paths
# =========================
input_path = r"C:\Users\nitesh.kumar_spicemo\Downloads\09-30-2023 ECUP Active Roster.xlsx"
output_path = r"C:\Users\nitesh.kumar_spicemo\Downloads\09-30-2023 ECUP Active Roster_DEGREE_SPEC_TAXONOMY_ROLE_CLEANED.xlsx"
# =========================

# The four logical bases we care about
DESIRED_BASES = ["Degree", "Speciality", "Taxonomy", "Role"]

# --------------------------------------------------------------------
# MANUAL INCLUDE / EXCLUDE PATTERNS (edit these to force mapping behavior)
# Patterns are regular expressions (case-insensitive). Examples:
#   - To include exact header "DEGREE_EARNED.TRAINING_PROG_DEGREE" for Degree:
#       INCLUDE_HEADER_PATTERNS["Degree"] = [r"^DEGREE_EARNED\.TRAINING_PROG_DEGREE$"]
#   - To exclude any header containing the word "legacy" from Speciality:
#       EXCLUDE_HEADER_PATTERNS["Speciality"] = [r"legacy"]
# Leave lists empty to use automatic detection only.
# --------------------------------------------------------------------
INCLUDE_HEADER_PATTERNS = {
    "Degree": [
        # Add regex strings here to force mapping to Degree
        # r"^DEGREE_EARNED\.TRAINING_PROG_DEGREE$"
    ],
    "Speciality": [
        # e.g. r"^some header$"
    ],
    "Taxonomy": [
        # e.g. r"^taxonomy_override$"
    ],
    "Role": [
        # e.g. r"^(role|pcp)$"
    ],
}

EXCLUDE_HEADER_PATTERNS = {
    "Degree": [
        # Add regex strings here to prevent mapping to Degree
    ],
    "Speciality": [
        # e.g. r"ignore_for_speciality"
    ],
    "Taxonomy": [
        # e.g. r"not_taxonomy"
    ],
    "Role": [
        # e.g. r"ignore_role"
    ],
}


def _matches_any_pattern(patterns, header):
    """Return True if header matches any regex pattern in patterns (case-insensitive)."""
    if not patterns:
        return False
    if header is None:
        return False
    for p in patterns:
        try:
            if re.search(p, str(header), flags=re.I):
                return True
        except re.error:
            # if user provided invalid regex, also try literal equality fallback
            if str(header).strip().lower() == str(p).strip().lower():
                return True
    return False


def detect_header_row(df_no_header: pd.DataFrame, max_rows_to_check: int = 20) -> int:
    rows_to_check = min(max_rows_to_check, len(df_no_header))
    non_null_counts = df_no_header.iloc[:rows_to_check].notna().sum(axis=1)
    return int(non_null_counts.idxmax())


def header_word_count(original: str) -> int:
    if original is None:
        return 0
    tokens = re.findall(r"\w+", str(original).strip())
    return len(tokens)


def make_standard_name(original: str) -> str:
    """
    Map original column name to a base standardized name.

    Rules:
      - If header matches INCLUDE_HEADER_PATTERNS[base] -> map to that base immediately (override).
      - If header matches EXCLUDE_HEADER_PATTERNS[base] -> do not map to that base (skip).
      - Otherwise run automatic detection:
         * Degree detection (unless excluded)
         * Role detection (unless excluded)
         * Taxonomy detection (must contain 'taxonomy' or 'taxonomy code' / 'tax code')
         * Speciality detection (unless excluded) with header-word-count <= 3 rule
    """
    if original is None:
        return ""

    o_raw = str(original)
    o = o_raw.strip().lower()

    # --- 1) Manual includes (highest priority) ---
    for base in DESIRED_BASES:
        pats = INCLUDE_HEADER_PATTERNS.get(base, [])
        if _matches_any_pattern(pats, o_raw):
            return base

    # Helper: is this header explicitly excluded for a base?
    def _is_excluded(base_name):
        return _matches_any_pattern(EXCLUDE_HEADER_PATTERNS.get(base_name, []), o_raw)

    # --- 2) Degree detection (skip if excluded) ---
    if not _is_excluded("Degree"):
        if re.search(
            r"\b(degree|degrees|credential|credentials|deg\b|md\b|m\.d\b|do\b|d\.o\b|phd\b|dr\b|dmd\b|dds\b|mbbs\b|mbchb\b|pa-c\b|pa\b|np\b|rn\b|bsc\b|msc\b|ms\b|mph\b|mds\b)\b",
            o,
            flags=re.I,
        ):
            return "Degree"

    # --- 3) Role detection (skip if excluded) ---
    if not _is_excluded("Role"):
        if re.search(r"\b(role|spec\b|pcp\b|primary care|primarycare|primary-care)\b", o, flags=re.I):
            # header-level rule: if header has more than 3 words, skip mapping to Role
            if header_word_count(o_raw) <= 3:
                return "Role"
            # else: intentionally fall-through to other tests (or no mapping)

    # --- 4) Taxonomy detection (skip if excluded) ---
    if not _is_excluded("Taxonomy"):
        if re.search(r"\b(taxonomy|taxonomy code|tax code)\b", o, flags=re.I):
            return "Taxonomy"

    # --- 5) Speciality detection (skip if excluded) ---
    if not _is_excluded("Speciality"):
        if re.search(r"\b(special|specialty|speciality|specialisation|specialization)\b", o, flags=re.I):
            # header-level rule: if header has more than 3 words, skip mapping to Speciality
            if header_word_count(o_raw) <= 3:
                return "Speciality"

    # fall-back: cleaned title (unused for mapping to DESIRED_BASES)
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
      - Role 1, Role 2, ...
    Applies header- and value-level rules:
      - Headers with >3 words are excluded from mapping to Speciality or Role (unless manually INCLUDED).
      - For Speciality and Role: when populating values, discard any cell with >3 word-like tokens (set to pd.NA).
      - Manual INCLUDE patterns override everything; EXCLUDE patterns prevent mapping to that base.
    Returns std_df and mapping_standard_to_originals (OrderedDict).
    """
    tracking = ["header_row_number", "source_sheet"]
    orig_cols_order = [c for c in combined_df.columns if c not in tracking]

    # Map each original column to a base standardized name (respecting includes/excludes)
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

        # Apply value-level rules:
        base = std_name.split()[0]  # 'Speciality', 'Role', etc.
        if base in ("Speciality", "Role"):
            def _filter_val(v):
                if pd.isna(v) or str(v).strip() == "":
                    return pd.NA
                tokens = re.findall(r"\w+", str(v).strip())
                return v if len(tokens) <= 3 else pd.NA
            std_df[std_name] = std_df[std_name].apply(_filter_val)

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

    # Build standardized frame containing Degree / Speciality / Taxonomy / Role columns
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

    print(f"✅ Combined (Degree/Speciality/Taxonomy/Role) output written to: {output_path}")
    print("✅ Audit sheet 'Duplicates_Removed' saved (contains removed duplicate occurrences)")


if __name__ == "__main__":
    main()

