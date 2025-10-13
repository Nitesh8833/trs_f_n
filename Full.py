#!/usr/bin/env python3
"""
Comprehensive healthcare provider name parser.

Inputs:
 - Reads an Excel file with a column (default name: header_col_value) OR runs demo on built-in sample list.
Outputs:
 - Excel with columns:
   header_col_value, First_Name, Middle_Name, Last_Name, Degree, Organization, Provider_Type, Confidence, Parsing_Notes

Designed to follow the rules & examples you provided; extend token lists to improve behavior.
"""

import re
import os
import sys
import argparse
import pandas as pd
from typing import List, Tuple, Dict, Optional

# ---------------- CONFIG ---------------- #
DEFAULT_INPUT_BASENAME = r"C:\Users\n299141\Downloads\fullName.xlsx"
AUTO_DISCOVER_DEPTH = 3

# Organization keywords (case-insensitive substring match). Extend as needed.
ORG_KEYWORDS = [
    "health","medcare","medicine","clinic","hospital","medical","university","wakemed",
    "group","practice","associates","center","centre","institute","partners","llc","inc","corp","p.a.","p.c."
]

# Degrees / credentials (add values you frequently see)
DEGREE_TOKENS = {
    "MD","M.D.","DO","D.O.","PA","PA-C","PA-C.","RN","BSN","PhD","Ph.D.","MBA","M.D","MBBS","DNP",
    "CRNA","NP","APRN","FNP","DPT","D.P.T.","DDS","OD","Esq","CPA","MPH","M.P.H.","FACOG","III","II","Jr","Sr",
    "RD","LD","OT","OTR","DVM","DMD","MS","MS.Ed","MSEd","MSN","RN-BSN","BCBA","BCBA-D","FNP-C", "PA-C"
}

# Honorifics / prefixes to strip (and optionally store)
PREFIX_TOKENS = {"dr","dr.","mr","mr.","mrs","mrs.","ms","ms.","prof","prof.","professor","drs","sir","dame"}

# Name suffixes (generational) that are not degrees
SUFFIXES = {"jr","jr.","sr","sr.","ii","iii","iv","v","iii.","ii.","iv.","v."}

# Particles that belong to last names (multiword)
MULTIWORD_LAST_PREFIXES = {"de","del","de la","da","di","van","von","der","du","la","le","st","st.","mac","mc","o'"}

# sample demo list you provided
sample_value = [
 "Andujar Vazquez, Gabriela M., MD", "Abdou, Rami Y.", "Adam Harold", "Albrecht, Morgan A, PA-C",
 "Booth, Aaron A., MD", "Angela E. Scheuerle, M.D.", "Changhee Kim, MD PhD", "Nell Divincenzo",
 "Abadir, Joseph M, DPT", "Mary B. Morgan, APRN", "George R. Parkerson, III, MD",
 "Andrew T. Day, M.D., M.P.H.", "Benesky, Jr., William T.", "Jennifer A. Black, DNP, CRNA",
 "Dominique St. Croix Williams, MD", "O. Howard Frazier, MD", "Mohammad I. H. Hirzallah, MD",
 "Ignatia B. Van den Veyver, MD", "Daniel Bral, DO, MPH, MS", "Heather S. Davis, PhD, BCBA",
 "WakeMed (NC)", "Phillips, J. Duncan", "Luis O. Rustveld, PhD, RD, LD",
 "Wonedwossen N. Abebe Goshu, DNP, CRNA", "Lourdes L. Lizarraga, M.S.Ed",
 "Helen S. Cohen, Ed.D, OTR", "Cecilia V. Morales, MD", "Prucha, Jr., Ronald J",
 # plus some of the string examples you mentioned earlier
 "Galanopoulos, MD, Alison, C.", "Fred Muhletaler-Maggiolo , MD", "BOTTY VAN DEN BRUELE, ASTRID MARIEROSE",
 "Aaron F. Brafman, MD", "ALLEN, PETER J", "Alfidi, Mary, MD", "Ashley Pinckney, DO",
 "Aida J. Lopez , APRN", "W. Michael Haney , MD", "Emilio P. Araujo Mino , MD",
 "Alfred J. Bilik Jr., MD", "Carla V. Justo , PA", "Salituro,sofia J Selbka MD", "Cohen, Wayne A.,MD, FACOG",
 "Grabios, B. M.,MD", "Eduardo B. Fernandez-Vicioso, MD, PhD", "PASBJERG, LAURENE. CRNA OBHG",
 "Grant I. S. Disick , MD", "Bitto Jr. MD, Donald D.", "Michael R. Denardis, MD"
]

# ---------------- HELPERS ---------------- #

def normalize_token(tok: str) -> str:
    if tok is None:
        return ""
    return tok.strip().strip('.,').replace('\xa0',' ').strip()

def split_commas_keep_empties(s: str) -> List[str]:
    return [normalize_token(p) for p in re.split(r',', s)]

def contains_org_keyword(s: str) -> Optional[str]:
    if not s:
        return None
    low = s.lower()
    for kw in ORG_KEYWORDS:
        if kw.lower() in low:
            return kw
    return None

def token_looks_degree(t: str) -> bool:
    if not t:
        return False
    tclean = t.strip().strip('.').strip(',').upper()
    if tclean in {d.upper().strip('.') for d in DEGREE_TOKENS}:
        return True
    # pattern like MD, PT, RN, PA-C etc.
    if re.fullmatch(r"[A-Z]{1,4}(-[A-Z]{1,4})?", tclean):
        return True
    # mixed-case academic like PhD, EdD
    if re.fullmatch(r"[A-Za-z]{2,6}\.?", t):
        return True
    return False

def strip_prefixes(s: str) -> Tuple[str, List[str]]:
    tokens = re.split(r'\s+', s)
    prefixes = []
    while tokens and tokens[0].strip().lower().strip('.') in PREFIX_TOKENS:
        prefixes.append(tokens.pop(0))
    return " ".join(tokens), prefixes

def strip_suffixes_tokens(tokens: List[str]) -> Tuple[List[str], List[str]]:
    suffixes_found = []
    while tokens and tokens[-1].strip().rstrip('.').lower() in SUFFIXES:
        suffixes_found.insert(0, tokens.pop(-1))
    return tokens, suffixes_found

def join_if_hyphenated(parts: List[str]) -> str:
    return " ".join(parts)

def parse_tokens_name(tokens: List[str]) -> Tuple[str,str,str]:
    """
    tokens: list of tokens for name (no degrees)
    heuristics:
      - 1 token: first
      - 2 tokens: first,last
      - 3 tokens: first, middle, last
      - >3: first, middle(all but first/last), last
      - handle 'Last First' ambiguous cases outside (we call this when aware of ordering)
    """
    if not tokens:
        return "","",""
    t = [normalize_token(x) for x in tokens if normalize_token(x)]
    if not t:
        return "","",""
    if len(t) == 1:
        return t[0], "", ""
    if len(t) == 2:
        return t[0], "", t[1]
    # 3 or more
    first = t[0]
    last = t[-1]
    middle = " ".join(t[1:-1])
    return first, middle, last

def parse_last_comma_first_format(segments: List[str]) -> Tuple[str,str,str,List[str]]:
    """
    segments is the comma-split pieces (normalized).
    heuristics for common "Last, First Middle, Degree, Degree" patterns
    returns (first,middle,last,degrees)
    """
    degrees = []
    # remove empty segments
    segs = [s for s in segments if s]
    if not segs:
        return "","","",[]

    # If first seg is likely organization & contains keyword -> classify later
    # Usually segs[0] = Last (maybe with suffix), segs[1] = given tokens, segs[2..] are degrees/titles
    last_seg = segs[0]
    given_seg = segs[1] if len(segs) > 1 else ""
    trailing = segs[2:] if len(segs) > 2 else []

    # If last_seg actually contains multiple words and looks like 'LAST FIRST' it's ambiguous; fallback later.
    # Extract degrees from trailing segments (and also from given_seg tail tokens if present)
    trailing_degs = []
    kept_given = given_seg
    # from trailing
    for t in trailing:
        toks = [normalize_token(x) for x in re.split(r'\s+', t) if x.strip()]
        found = [x for x in toks if token_looks_degree(x)]
        if found:
            trailing_degs.extend(found)
        else:
            # sometimes trailing contains roles like 'FACOG' etc -> still degree-like
            # if segment has any uppercase tokens >1 len treat as degree
            ups = [x for x in toks if re.search(r'[A-Z]{2,}', x)]
            if ups:
                trailing_degs.extend(ups)
            else:
                # maybe it's an extra name piece -> merge into given section
                kept_given = kept_given + " " + t if kept_given else t

    # Also strip degree-like tokens that are appended to given_seg
    given_toks = [normalize_token(x) for x in re.split(r'\s+', given_seg) if x.strip()]
    give_keep = []
    give_degs = []
    while given_toks and token_looks_degree(given_toks[-1]):
        give_degs.insert(0, given_toks.pop(-1))
    give_keep = given_toks
    if give_keep:
        kept_given = " ".join(give_keep)
    else:
        kept_given = ""

    degrees = trailing_degs + give_degs

    # last_seg may include suffix like "Benesky, Jr." or "Prucha, Jr., Ronald J" cases:
    last_tokens = [t for t in re.split(r'\s+', last_seg) if t.strip()]
    # if last_tokens include Jr, keep it with last name by joining
    last_clean = " ".join(last_tokens)

    # parse given tokens into first/middle
    first, middle, last_x = "","",""
    if kept_given:
        first, middle, _ = parse_tokens_name([x for x in re.split(r'\s+', kept_given) if x.strip()])
    # for last: prefer last_clean unless last_clean looks like it contains both last and first
    last = last_clean

    # handle cases "Benesky, Jr., William T." -> segments could be ['Benesky', 'Jr.', 'William T.'] depending on splitting.
    # If you see the second segment equals suffix (jr...) and third segment is given, adjust:
    if len(segs) >= 3 and segs[1].rstrip('.').lower() in SUFFIXES:
        last = segs[0] + " " + segs[1]
        # third seg is given
        first, middle, _ = parse_tokens_name([x for x in re.split(r'\s+', segs[2]) if x.strip()])

    # final cleanup: strip degree-like tokens from name parts (safety)
    def remove_degree_like_from_part(part: str) -> str:
        if not part:
            return ""
        toks = [t for t in re.split(r'\s+', part) if t.strip()]
        toks = [t for t in toks if not token_looks_degree(t)]
        return " ".join(toks)

    first = remove_degree_like_from_part(first)
    middle = remove_degree_like_from_part(middle)
    last = remove_degree_like_from_part(last)

    return first, middle, last, degrees

def parse_freeform_no_commas(s: str) -> Tuple[str,str,str,List[str]]:
    """
    When no comma: typical "First Middle Last, Degree" or "First Last" or "Organization"
    We'll extract degree tokens at end if present.
    """
    degrees = []
    s_norm = s.strip()
    # remove parentheses parts when they look like location (but preserve for org detection)
    # Example: "WakeMed (NC)" - handled earlier as org
    toks = [normalize_token(x) for x in re.split(r'\s+', s_norm) if x.strip()]
    # collect trailing degree tokens
    trailing_deg = []
    while toks and token_looks_degree(toks[-1]):
        trailing_deg.insert(0, toks.pop(-1))
    # also if last is like 'MD,PhD' handled earlier by comma splitting
    degrees = trailing_deg

    # remove honorific prefix if present
    joined = " ".join(toks)
    joined, prefixes = strip_prefixes(joined)
    toks = [normalize_token(x) for x in re.split(r'\s+', joined) if x.strip()]

    # remove suffixes like Jr from end
    toks, suffs = strip_suffixes_tokens(toks)

    # If remaining tokens <= 2 => standard first/last
    if len(toks) == 0:
        return "","","", degrees
    if len(toks) == 1:
        return toks[0], "", "", degrees
    if len(toks) == 2:
        return toks[0], "", toks[1], degrees
    # else >=3 => heuristic: first token first, last token last, middle rest
    first = toks[0]
    last = toks[-1]
    middle = " ".join(toks[1:-1])
    return first, middle, last, degrees

def build_confidence(parsed: Dict[str, str], provider_type: str) -> float:
    """
    Simple confidence heuristics:
     - ORGANIZATION: 0.9 if org found
     - INDIVIDUAL: high (0.9) if first+last present; 0.75 if first or last present; else 0.5
     - reduce if degrees empty but expected, etc.
    """
    if provider_type == "ORGANIZATION":
        return 0.95 if parsed.get("Organization") else 0.75
    first = parsed.get("First_Name","")
    last = parsed.get("Last_Name","")
    deg = parsed.get("Degree","")
    if first and last:
        conf = 0.95
    elif first or last:
        conf = 0.75
    else:
        conf = 0.45
    # bump if degrees found and they look valid
    if deg:
        conf = min(0.98, conf + 0.02)
    return conf

# ---------------- CORE parse function ---------------- #

def parse_header_value(value: str) -> Dict[str,str]:
    """
    Returns dict with keys:
    First_Name, Middle_Name, Last_Name, Degree (comma-separated), Organization, Provider_Type, Confidence, Parsing_Notes
    """
    out = {
        "First_Name": "",
        "Middle_Name": "",
        "Last_Name": "",
        "Degree": "",
        "Organization": "",
        "Provider_Type": "",
        "Confidence": 0.0,
        "Parsing_Notes": ""
    }
    if value is None:
        value = ""
    original = str(value).strip()
    if not original:
        out["Parsing_Notes"] = "Empty input"
        out["Confidence"] = 0.0
        return out

    # Detect org early: if contains known org keywords -> treat as organization (but still try to parse person if ambiguous)
    orgkw = contains_org_keyword(original)
    # If the string is short and contains org keyword or contains "clinic"/"hospital" etc => organization
    if orgkw and (len(original.split()) <= 6 or re.search(r'\b(clinic|hospital|center|centre|group|institute|university|practice)\b', original, flags=re.I)):
        out["Organization"] = original
        out["Provider_Type"] = "ORGANIZATION"
        out["Parsing_Notes"] = f"Detected organization keyword '{orgkw}'"
        out["Confidence"] = 0.98
        return out

    # else proceed to parse person formats
    s = original

    # Normalize certain punctuation spacing
    s = re.sub(r'\s+', ' ', s).strip()
    s = re.sub(r'\s*,\s*', ',', s)  # strip spaces around commas

    # If there are commas: try last, first parsing heuristics
    if ',' in s:
        segments = split_commas_keep_empties(s)
        # Quick check: if first segment contains org keyword -> organization
        if contains_org_keyword(segments[0]):
            out["Organization"] = original
            out["Provider_Type"] = "ORGANIZATION"
            out["Parsing_Notes"] = "First segment contains org keyword"
            out["Confidence"] = 0.95
            return out

        # Try Last, First pattern when segments[0] looks like a last name (no spaces or has uppercase)
        # handle cases like "Last, First Middle, DEG1, DEG2" etc.
        first, middle, last, degrees = parse_last_comma_first_format(segments)
        # if parser returned empty names then fallback to other strategies
        name_found = bool(first or last or middle)
        if name_found:
            out["First_Name"] = first
            out["Middle_Name"] = middle
            out["Last_Name"] = last
            out["Degree"] = ",".join([normalize_token(d) for d in degrees]) if degrees else ""
            out["Provider_Type"] = "INDIVIDUAL"
            out["Parsing_Notes"] = "Parsed using Last, First comma heuristic"
            out["Confidence"] = build_confidence(out, out["Provider_Type"])
            return out

        # else fallback: maybe format is "Last, MD, First ..." (weird ordering)
        # We'll attempt to collect degree-like segments and name-like segments
        segs_clean = [seg for seg in segments if seg]
        degs = []
        name_segs = []
        for seg in segs_clean:
            toks = [normalize_token(x) for x in re.split(r'\s+', seg) if x.strip()]
            if any(token_looks_degree(t) for t in toks) and len(toks) <= 4:
                degs.append(" ".join([t for t in toks if token_looks_degree(t)]))
            else:
                name_segs.append(seg)
        if name_segs:
            # join name_segs into an approximate name and parse freeform
            joined = " ".join(name_segs)
            f,m,l,deg2 = parse_freeform_no_commas(joined)
            deg_all = degs + deg2
            out["First_Name"] = f
            out["Middle_Name"] = m
            out["Last_Name"] = l
            out["Degree"] = ",".join([normalize_token(d) for d in deg_all if d])
            out["Provider_Type"] = "INDIVIDUAL"
            out["Parsing_Notes"] = "Fallback: combined comma segments into name"
            out["Confidence"] = build_confidence(out, out["Provider_Type"])
            return out

    else:
        # No commas: typical freeform "First Middle Last [Degree]"
        # First check if it's likely an organization in different style (contains 'hospital' etc)
        if contains_org_keyword(s):
            out["Organization"] = original
            out["Provider_Type"] = "ORGANIZATION"
            out["Parsing_Notes"] = "Detected organization keyword in freeform"
            out["Confidence"] = 0.95
            return out

        f,m,l,degs = parse_freeform_no_commas(s)
        out["First_Name"] = f
        out["Middle_Name"] = m
        out["Last_Name"] = l
        out["Degree"] = ",".join([normalize_token(d) for d in degs]) if degs else ""
        out["Provider_Type"] = "INDIVIDUAL"
        out["Parsing_Notes"] = "Parsed using no-comma freeform heuristic"
        out["Confidence"] = build_confidence(out, out["Provider_Type"])
        return out

    # final fallback (shouldn't normally reach)
    out["Parsing_Notes"] = "Could not parse; fallback applied"
    out["Confidence"] = 0.25
    return out

# ---------------- Dataframe processing ---------------- #

def process_dataframe(df: pd.DataFrame, value_col: str = "header_col_value") -> pd.DataFrame:
    # normalize column name lookup (case-insensitive)
    col_map = {c.lower(): c for c in df.columns}
    if value_col.lower() in col_map:
        canonical = col_map[value_col.lower()]
    else:
        raise ValueError(f"Column '{value_col}' not found. Available columns: {list(df.columns)}")

    # preserve original as header_col_value (leftmost canonical)
    df['header_col_value'] = df[canonical].astype(str).where(df[canonical].notna(), "")

    parsed_rows = [parse_header_value(v) for v in df['header_col_value']]

    # expand into columns
    out = df.copy()
    out['First_Name'] = [p['First_Name'] for p in parsed_rows]
    out['Middle_Name'] = [p['Middle_Name'] for p in parsed_rows]
    out['Last_Name'] = [p['Last_Name'] for p in parsed_rows]
    out['Degree'] = [p['Degree'] for p in parsed_rows]
    out['Organization'] = [p['Organization'] for p in parsed_rows]
    out['Provider_Type'] = [p['Provider_Type'] for p in parsed_rows]
    out['Confidence'] = [p['Confidence'] for p in parsed_rows]
    out['Parsing_Notes'] = [p['Parsing_Notes'] for p in parsed_rows]

    # reorder columns: header_col_value then requested fields then rest
    desired = ['header_col_value','First_Name','Middle_Name','Last_Name','Degree','Organization','Provider_Type','Confidence','Parsing_Notes']
    other_cols = [c for c in out.columns if c not in desired]
    out = out[desired + other_cols]
    return out

# ---------------- CLI / demo ---------------- #

def main():
    parser = argparse.ArgumentParser(description="Comprehensive healthcare provider name parser.")
    parser.add_argument('--input', help='Path to input Excel file (.xlsx). If omitted, runs demo using built-in sample list.')
    parser.add_argument('--sheet', help='Sheet name when reading Excel (default: first)', default=None)
    parser.add_argument('--output', help='Output path for parsed Excel (default: <input>_parsed.xlsx or parsed_demo.xlsx).')
    parser.add_argument('--value-col', help='Input column name (default header_col_value)', default='header_col_value')
    args = parser.parse_args()

    if not args.input:
        # demo
        df_demo = pd.DataFrame({ 'header_col_value': sample_value })
        out = process_dataframe(df_demo, value_col='header_col_value')
        print(out.to_string(index=False))
        demo_path = args.output or "parsed_demo.xlsx"
        try:
            with pd.ExcelWriter(demo_path, engine='openpyxl') as writer:
                out.to_excel(writer, index=False, sheet_name='names')
            print("Wrote demo output to", demo_path)
        except Exception:
            out.to_excel(demo_path, index=False, sheet_name='names')
            print("Wrote demo output to", demo_path)
        return

    # read provided Excel
    if not os.path.isfile(args.input):
        print(f"Input file not found: {args.input}", file=sys.stderr)
        return

    try:
        if args.sheet:
            df = pd.read_excel(args.input, sheet_name=args.sheet)
        else:
            df = pd.read_excel(args.input)
    except Exception as e:
        print("Failed to read Excel:", e, file=sys.stderr)
        return

    try:
        out_df = process_dataframe(df, value_col=args.value_col)
    except Exception as e:
        print("Processing error:", e, file=sys.stderr)
        return

    out_path = args.output or os.path.splitext(args.input)[0] + "_parsed.xlsx"
    try:
        with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
            out_df.to_excel(writer, index=False, sheet_name='names')
    except Exception:
        out_df.to_excel(out_path, index=False, sheet_name='names')

    print("Parsed and wrote:", out_path)

if __name__ == "__main__":
    main()
