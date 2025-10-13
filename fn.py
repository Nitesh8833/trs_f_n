import os
import sys
import argparse
import pandas as pd
import re
import json
from typing import Tuple, List

# ---------------- CONFIG ---------------- #
# Update DEFAULT_INPUT_BASENAME to your default Excel name/path if desired
DEFAULT_INPUT_BASENAME = r"C:\Users\n299141\Downloads\fullName.xlsx"
AUTO_DISCOVER_DEPTH = 3  # how many directory levels downward to search if not found

# Suffixes (lowercase) that are name suffixes (not designations)
SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v", "phd", "md", "dds", "esq"}

MULTIWORD_LAST_PREFIXES = {"de", "del", "de la", "da", "di", "van", "von", "der", "du", "la", "le", "st", "st.", "mac"}
UPPER_DESIGNATION_MIN_ALPHA = 2  # need at least 2 alpha chars total

COMMON_DESIGNATION_TOKENS = {
    "CEO", "CTO", "CFO", "COO", "VP", "SVP", "AVP", "MD", "HR", "QA", "QA LEAD", "TEAM LEAD", "LEAD",
    "ENGINEER", "SR ENGINEER", "SENIOR ENGINEER", "MANAGER", "SR MANAGER", "SENIOR MANAGER", "DIRECTOR",
    "SR DIRECTOR", "SENIOR DIRECTOR", "PRESIDENT", "CHAIRMAN", "CHAIRWOMAN", "CHIEF", "ARCHITECT",
    "ANALYST", "PA", "DO", "OT", "PT", "NP", "FNP", "ARNP", "APRN", "RN", "LPN", "MBA", "CPA", "FNP-C",
    "PA-C", "RN BSN", "BSN", "DNP", "OT"
}

# files to exclude entirely (lowercased). If you want to EXCLUDE everything except the Roster file,
# configure this set appropriately. We'll treat the special roster file separately (see ROSTER_FILENAME).
EXCLUDE_FILE_NAMES = {
    # other filenames you want to exclude entirely (lowercase trimmed)
    # e.g. "some_old_file.xlsx"
}

# Special file: we DO NOT exclude this; instead we tag it into the organization column.
ROSTER_FILENAME = "Roster Files - top 20 entities_desensitized.xlsx"

# ---------------- HELPERS ---------------- #

def is_designation_string(segment: str) -> bool:
    """
    Heuristic: is this segment a designation / credential string?
    We require at least UPPER_DESIGNATION_MIN_ALPHA alpha chars total and that alpha chunks are uppercase.
    Also accept direct matches in COMMON_DESIGNATION_TOKENS.
    """
    if not segment or not isinstance(segment, str):
        return False
    seg = segment.strip().strip('.')
    if not seg:
        return False

    if seg.upper() in COMMON_DESIGNATION_TOKENS:
        return True

    # split on whitespace and punctuation
    tokens = [t for t in re.split(r"[,\s]+", seg) if t]
    if not tokens:
        return False

    alpha_count = sum(sum(ch.isalpha() for ch in t) for t in tokens)
    if alpha_count < UPPER_DESIGNATION_MIN_ALPHA:
        return False

    # require alpha parts to be uppercase (ignore digits/punct)
    for t in tokens:
        alpha = ''.join(ch for ch in t if ch.isalpha())
        if alpha and alpha != alpha.upper():
            return False

    # single token special-case: allow short uppercase credentials (2-4 chars)
    if len(tokens) == 1:
        single = tokens[0]
        if single.upper() != single:
            return False
        if single.upper() in COMMON_DESIGNATION_TOKENS:
            return True
        if 2 <= len(single) <= 4:
            return True
        return False

    return True


def normalize_designation_segment(seg: str) -> List[str]:
    """
    Given a raw segment such as "PT, OCS" or "DO PhD" return a list of normalized designation tokens:
    e.g. ["PT", "OCS"] or ["DO", "PhD"] (preserve casing for tokens like PhD).
    We try to split on commas and whitespace, then keep tokens that look like credentials.
    """
    if not seg or not isinstance(seg, str):
        return []

    # split by comma first, then by whitespace
    parts = []
    for c in re.split(r",", seg):
        for p in re.split(r"\s+", c.strip()):
            if p:
                parts.append(p.strip().strip('.'))

    kept = []
    for p in parts:
        if not p:
            continue
        # Accept if is_designation_string on this token OR token upper == token and has 2+ letters
        if is_designation_string(p) or (any(ch.isalpha() for ch in p) and p.upper() == p and sum(ch.isalpha() for ch in p) >= 2):
            kept.append(p)
        else:
            # Some tokens like "PhD" may fail is_designation_string due to lowercase detection; include known pattern:
            if re.match(r"^[A-Za-z]{2,4}$", p):
                kept.append(p)
    # remove duplicates preserving order
    seen = set()
    out = []
    for k in kept:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out


def clean_token(tok: str) -> str:
    return tok.strip().strip('.').strip()


def _split_tokens(s: str) -> List[str]:
    return [t.strip().strip('.,') for t in re.split(r'\s+', s) if t.strip()]


def remove_designation_tokens_from_part(part: str, designation_list: List[str]) -> str:
    """
    Remove any tokens from a name part that match tokens in designation_list or look like uppercase credentials.
    Returns the cleaned part (joined by single spaces).
    """
    if not part:
        return ""
    tokens = [t for t in re.split(r'\s+', part) if t.strip()]
    keep = []
    # build a lowercase set of normalized designation tokens for quick compare
    desig_norm = {d.strip().strip('.').lower() for d in designation_list if isinstance(d, str) and d}
    for tok in tokens:
        tok_clean = tok.strip().strip('.,')
        if not tok_clean:
            continue
        lower = tok_clean.lower()
        # remove if equal to any designation token
        if lower in desig_norm:
            continue
        # if token looks like credential (all letters and uppercase length 2-4), drop it
        if re.fullmatch(r'[A-Z]{2,4}', tok_clean):
            continue
        # also remove tokens that are known COMMON_DESIGNATION_TOKENS (case-insensitive)
        if tok_clean.upper() in COMMON_DESIGNATION_TOKENS:
            continue
        keep.append(tok_clean)
    return " ".join(keep)


def parse_full_name(full_name: str) -> Tuple[str, str, str, List[str]]:
    """
    Parse full_name into (first, middle, last, designation_list).
    Returns designation_list as a list (not string).
    """
    if not isinstance(full_name, str):
        return "", "", "", []
    name = full_name.strip()
    if not name:
        return "", "", "", []

    # normalize whitespace
    name = re.sub(r"\s+", " ", name).strip()
    designation_list: List[str] = []
    comma_mode = False

    # If commas are present, try to interpret trailing designation chunk(s)
    if ',' in name:
        parts_all = [p.strip() for p in name.split(',')]
        # If last part looks like designation(s), move them to designation_list and treat remainder as name_core
        if len(parts_all) >= 2:
            last_chunk = parts_all[-1]
            if is_designation_string(last_chunk):
                designation_list = normalize_designation_segment(last_chunk)
                name_core = ','.join(parts_all[:-1])
                idx = len(parts_all) - 2
                while idx >= 0 and is_designation_string(parts_all[idx]):
                    extra = normalize_designation_segment(parts_all[idx])
                    designation_list = extra + designation_list
                    idx -= 1
                comma_mode = (idx < len(parts_all) - 1)
            else:
                name_core = name
                comma_mode = True if len(parts_all) == 2 else True
        else:
            name_core = name
            comma_mode = True
    else:
        name_core = name

    if comma_mode and ',' in name_core:
        parts = [p.strip() for p in name_core.split(',', 1)]
        last_part = parts[0]
        rest_part = parts[1]
        given_tokens = _split_tokens(rest_part)
        last_tokens = _split_tokens(last_part)
    else:
        tokens = _split_tokens(name_core)
        given_tokens = tokens
        last_tokens = []

    # CASE: No explicit last_tokens (no Last, First)
    if not last_tokens:
        tokens = list(given_tokens)
        if not tokens:
            return "", "", "", designation_list

        # Capture trailing designation tokens (free-form)
        trailing_designations = []
        while tokens and is_designation_string(tokens[-1]):
            trailing_designations = normalize_designation_segment(tokens[-1]) + trailing_designations
            tokens = tokens[:-1]
        if trailing_designations:
            designation_list = designation_list + trailing_designations

        # Remove suffix tokens (jr, sr, etc) that are not designation
        while tokens and tokens[-1].lower().rstrip('.') in SUFFIXES and not is_designation_string(tokens[-1]):
            tokens = tokens[:-1]

        if not tokens:
            return "", "", "", designation_list

        if len(tokens) == 1:
            return tokens[0], "", "", designation_list
        if len(tokens) == 2:
            return tokens[0], "", tokens[1], designation_list

        # For 3+ tokens, try to detect multi-word last-name prefixes
        last_candidate = [tokens[-1]]
        idx = len(tokens) - 2
        while idx >= 0:
            probe = tokens[idx].lower().strip('.').strip()
            if probe in MULTIWORD_LAST_PREFIXES:
                last_candidate.insert(0, tokens[idx])
                idx -= 1
            else:
                break
        first = tokens[0]
        last = ' '.join(last_candidate)
        middle = ' '.join(tokens[1:idx + 1]) if idx >= 1 else ""
        return first, middle, last, designation_list

    else:
        # CASE: classic Last, First (or Last, First + credentials on the right side)
        trailing_designations = []
        while given_tokens and is_designation_string(given_tokens[-1]):
            trailing_designations = normalize_designation_segment(given_tokens[-1]) + trailing_designations
            given_tokens = given_tokens[:-1]
        if trailing_designations:
            designation_list = designation_list + trailing_designations

        while given_tokens and given_tokens[-1].lower().rstrip('.') in SUFFIXES and not is_designation_string(given_tokens[-1]):
            given_tokens = given_tokens[:-1]

        if not given_tokens:
            return "", "", " ".join(last_tokens), designation_list

        if len(given_tokens) == 1:
            return given_tokens[0], "", " ".join(last_tokens), designation_list

        first = given_tokens[0]
        middle = " ".join(given_tokens[1:])
        last = " ".join(last_tokens)
        return first, middle, last, designation_list


# ---------------- MAIN ---------------- #

def main():
    no_args = len(sys.argv) == 1

    if no_args:
        class Args:
            input = None
            sheet = None
            output = None
            value_col = 'header_col_value'
        args = Args()
    else:
        parser = argparse.ArgumentParser(description="Split full names in header_col_value into first_name, middle_name, last_name, designation.")
        parser.add_argument('--input', default=None, help='Path to input Excel (default: fullName.xlsx beside script or discovered).')
        parser.add_argument('--sheet', default=None, help='Sheet name (default: first sheet).')
        parser.add_argument('--output', default=None, help='Path to output Excel (default: <input> with _parsed suffix).')
        parser.add_argument('--value-col', default='header_col_value', help='Column containing the full name.')
        args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Discover input if not provided
    if not args.input:
        candidates = []
        p1 = os.path.join(script_dir, DEFAULT_INPUT_BASENAME)
        if os.path.isfile(p1):
            candidates.append(p1)
        p2 = os.path.join(script_dir, 'test', DEFAULT_INPUT_BASENAME)
        if os.path.isfile(p2):
            candidates.append(p2)
        for root, dirs, files in os.walk(script_dir):
            depth = root[len(script_dir):].count(os.sep)
            if depth > AUTO_DISCOVER_DEPTH:
                dirs[:] = []
                continue
            if DEFAULT_INPUT_BASENAME in files:
                candidates.append(os.path.join(root, DEFAULT_INPUT_BASENAME))

        # de-duplicate preserving order
        uniq = []
        seen = set()
        for c in candidates:
            if c not in seen:
                seen.add(c)
                uniq.append(c)

        if uniq:
            args.input = uniq[0]
        else:
            print(f"Input file '{DEFAULT_INPUT_BASENAME}' not found. Place it next to this script or provide --input.", file=sys.stderr)
            return

    if not os.path.isfile(args.input):
        print(f"Input file does not exist: {args.input}", file=sys.stderr)
        return

    # Read Excel
    try:
        if args.sheet:
            df = pd.read_excel(args.input, sheet_name=args.sheet)
        else:
            df = pd.read_excel(args.input)
    except Exception as e:
        print(f"Failed to read input Excel: {e}", file=sys.stderr)
        return

    # Ensure file_name column exists for filtering
    if 'file_name' not in df.columns:
        df['file_name'] = ''

    # NEW: populate organization column only for rows from ROSTER_FILENAME (do not exclude that file)
    df['organization'] = df['file_name'].apply(lambda v: v if isinstance(v, str) and v.strip() == ROSTER_FILENAME else "")

    # Apply exclusion filter on file_name (case-insensitive) - BUT make sure roster file is NOT excluded
    exclude_lower = {n.lower().strip() for n in EXCLUDE_FILE_NAMES}
    before_rows = len(df)

    def _should_exclude(fname):
        if not isinstance(fname, str):
            return False
        fname_l = fname.strip().lower()
        if fname_l == ROSTER_FILENAME.lower():
            return False
        return fname_l in exclude_lower

    df = df[~df['file_name'].fillna('').astype(str).apply(lambda x: _should_exclude(x))].copy()
    excluded_rows = before_rows - len(df)

    # Normalize column name (case-insensitive) to requested value_col and preserve original source value
    col_lc_map = {c.lower(): c for c in df.columns}
    value_col_lc = args.value_col.lower()
    if value_col_lc in col_lc_map:
        # preserve the original source column into 'header_col_value' (left-most target column)
        orig_col = col_lc_map[value_col_lc]
        # create a canonical column name header_col_value (overwrite if present)
        df['header_col_value'] = df[orig_col].astype(str).where(df[orig_col].notna(), "")
        # ensure args.value_col is the canonical name we use from here
        args.value_col = 'header_col_value'
    else:
        # column not found (case-insensitive)
        print(f"Column '{args.value_col}' not found. Available: {list(df.columns)}", file=sys.stderr)
        return

    # Parse names
    parsed = [parse_full_name(v) for v in df[args.value_col]]
    first_names = []
    middle_names = []
    last_names = []
    designations_all = []

    for p in parsed:
        first, middle, last, desigs = p
        # ensure desigs is a list
        if desigs is None:
            desigs = []
        # remove any stray designation tokens from name parts
        first_clean = remove_designation_tokens_from_part(first, desigs)
        middle_clean = remove_designation_tokens_from_part(middle, desigs)
        last_clean = remove_designation_tokens_from_part(last, desigs)
        # Final fallback: if cleaning removed everything from first but middle exists, promote middle
        if not first_clean and middle_clean:
            parts = middle_clean.split()
            if parts:
                first_clean = parts[0]
                middle_clean = " ".join(parts[1:]) if len(parts) > 1 else ""
        first_names.append(first_clean)
        middle_names.append(middle_clean)
        last_names.append(last_clean)
        designations_all.append(desigs)

    df['first_name'] = first_names
    df['middle_name'] = middle_names
    df['last_name'] = last_names
    # store designation as comma-separated string without spaces after comma
    df['designation'] = [",".join([s.strip() for s in d if s]) if d else "" for d in designations_all]

    # Reorder columns: original header_col_value at left, then first/middle/last/designation then others
    primary = [args.value_col, 'first_name', 'middle_name', 'last_name', 'designation', 'organization']
    other_cols = [c for c in df.columns if c not in primary]
    cols_final = [c for c in primary if c in df.columns] + [c for c in other_cols if c in df.columns]
    df = df[cols_final]

    # Determine output filename
    if not args.output:
        base, ext = os.path.splitext(args.input)
        args.output = base + '_parsed.xlsx'

    # Write Excel (try with openpyxl engine first)
    try:
        with pd.ExcelWriter(args.output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='names')
    except ModuleNotFoundError:
        with pd.ExcelWriter(args.output) as writer:
            df.to_excel(writer, index=False, sheet_name='names')

    # Also write JSON (records)
    json_output = os.path.splitext(args.output)[0] + '_parsed.json'
    try:
        # orient records: list of dicts
        df.to_json(json_output, orient='records', force_ascii=False, date_format='iso')
    except Exception:
        # fallback to Python json dump to preserve readability
        records = df.to_dict(orient='records')
        with open(json_output, 'w', encoding='utf-8') as fh:
            json.dump(records, fh, ensure_ascii=False, indent=2)

    # Summary prints
    print('Done.')
    print(f'Input: {args.input}')
    print(f'Output Excel: {args.output}')
    print(f'Output JSON: {json_output}')
    print(f'Excluded rows by file_name filter (excluding roster file): {excluded_rows}')
    print('Added columns: header_col_value, first_name, middle_name, last_name, designation, organization')
    if no_args:
        print('(Ran in no-argument mode)')


if __name__ == "__main__":
    main()
