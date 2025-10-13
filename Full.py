#!/usr/bin/env python3
import json
import re
import pandas as pd

INPUT_XLSX = r"H:\Transformation\2025\oct\Week2\file_Name_new\Provider_name_analysis.xlsx"
OUTPUT_XLSX = r"H:\Transformation\2025\oct\Week2\file_Name_new\fullName_Parsed.xlsx"
OUTPUT_JSONL = r"H:\Transformation\2025\oct\Week2\file_Name_new\fullName_parsed_v6_1.jsonl"

# Candidate name column headers (case-insensitive)
NAME_COL_CANDIDATES = {
    'header_col_value', 'full name', 'full_name',
    'orig full name', 'provider_full_name'
}

# Organization keywords (case-insensitive). Edit or extend this list as needed.
ORG_KEYWORDS = [
    "health", "MedCare", "Medicine", "Clinic", "Hospital", "Medical",
    "University", "WakeMed"
]

# Normalize ORG keywords for regex (create an alternation, case-insensitive)
ORG_RE = re.compile(r"\b(" + r"|".join(re.escape(k) for k in ORG_KEYWORDS) + r")\b", flags=re.IGNORECASE)

RAW_DEGREE_ALIASES = {
    'm.d.': 'MD', 'md': 'MD', 'MD': 'MD',
    'ph.d.': 'PhD', 'phd': 'PhD', 'PhD': 'PhD',
    'd.o.': 'DO', 'do': 'DO', 'DO': 'DO',
    'r.n.': 'RN', 'rn': 'RN', 'RN': 'RN',
    'd.d.s.': 'DDS', 'dds': 'DDS', 'DDS': 'DDS',
    'd.c.': 'DC', 'dc': 'DC', 'DC': 'DC',
    'm.b.b.s.': 'MBBS', 'mbbs': 'MBBS', 'MBBS': 'MBBS',
    'd.m.': 'DM', 'dm': 'DM', 'DM': 'DM',

    'pa': 'PA', 'pa-c': 'PA-C', 'pa c': 'PA-C', 'pa-c.': 'PA-C',
    'aprn': 'APRN', 'crna': 'CRNA', 'np': 'NP', 'np-c': 'NP-C', 'NP-C': 'NP-C',

    'dpt': 'DPT', 'd.p.t.': 'DPT',
    'dpm': 'DPM', 'd.p.m.': 'DPM',
    'pt': 'PT', 'otr': 'OTR', 'otr/l': 'OTR/L',

    'aud': 'AUD', 'msw': 'MSW', 'rnfa': 'RNFA',
    'dnp': 'DNP', 'fnp': 'FNP', 'cnm': 'CNM', 'cns': 'CNS',

    'rt': 'RT', 'rtt': 'RTT', 'rd': 'RD', 'rdn': 'RDN',
    'rdms': 'RDMS', 'rdcs': 'RDCS',

    'msc': 'MSC', 'ms': 'MS', 'ma': 'MA', 'mba': 'MBA', 'mph': 'MPH',

    # Explicit include for uppercase specialty token:
    'obgyn': 'OBGYN'
}

SUFFIXES = {'jr', 'sr', 'ii', 'iii', 'iv', 'v', 'vi'}
ROMAN_NUMERALS = {'I', 'II', 'III', 'IV', 'V', 'VI'}
TITLES = {'dr', 'mr', 'mrs', 'ms', 'prof', 'sir', 'madam', 'miss'}

SPLIT_NO_HYPHEN = re.compile(r"[\s,]+")
UPPERCASE_DEG_RE = re.compile(r"[A-Z][A-Z\-]+$")
DASH_ONLY = {'-', '–', '—'}

def normalize_token(tok: str) -> str:
    return re.sub(r"[\s\-.]", '', str(tok)).lower()

DEGREE_MAP = {normalize_token(k): v for k, v in RAW_DEGREE_ALIASES.items()}
DEGREE_KEYS = set(DEGREE_MAP.keys())

def strip_leading_titles(text: str) -> str:
    t = text.strip()
    while True:
        m = re.match(r"^(?P<title>[A-Za-z]+)\.?\s+(.*)$", t)
        if m and m.group('title').lower() in TITLES:
            t = m.group(2)
            continue
        break
    return t.strip()

def smart_title(s: str) -> str:
    def fix_token(tok: str) -> str:
        if tok.upper() in {'II', 'III', 'IV', 'VI'}:
            return tok.upper()
        if re.match(r"^[A-Za-z]\.$", tok):  # initials like J.
            return tok.upper()
        if re.match(r"^O[Oo]+", tok):
            return "O" + tok[2:].capitalize()
        if '-' in tok:
            return '-'.join(fix_token(part) for part in tok.split('-'))
        return tok.capitalize()
    return ' '.join(fix_token(t) for t in s.split())

def extract_degrees_from_part(part: str) -> list:
    """
    Return a list of normalized degrees found in a comma/space separated part.
    Recognizes canonical degrees and fully UPPERCASE tokens (except Roman numerals).
    """
    if not isinstance(part, str) or not part.strip():
        return []
    tokens = SPLIT_NO_HYPHEN.split(part.strip())
    out = []
    for t in tokens:
        if not t or t in DASH_ONLY:
            continue
        key = normalize_token(t)
        if key in DEGREE_MAP:
            out.append(DEGREE_MAP[key])
        else:
            # Treat fully UPPERCASE tokens as degrees (exclude Roman numerals)
            if UPPERCASE_DEG_RE.match(t) and t.upper() not in ROMAN_NUMERALS and len(t) >= 2:
                out.append(t.upper())
    return out

def parse_with_degree(raw: str):
    """Returns (firstname, lastname, middlename, medical_degree)."""
    if pd.isna(raw):
        return '', '', '', 'NA'
    s = str(raw).strip().replace(';', "")
    if not s:
        return '', '', '', 'NA'
    parts = [p.strip() for p in re.split(r"\s*,\s*", s) if p.strip()]
    # 1) Collect degrees from trailing comma-separated parts
    degrees = []
    for p in parts[1:]:
        degrees.extend(extract_degrees_from_part(p))
    # 2) Remove pure-degree trailing parts so they don't affect name parsing
    while parts:
        cand = parts[-1]
        toks = [t for t in SPLIT_NO_HYPHEN.split(cand) if t and t not in DASH_ONLY]
        if toks and all(
            (normalize_token(t) in DEGREE_KEYS)
            or (UPPERCASE_DEG_RE.match(t) and t.upper() not in ROMAN_NUMERALS and len(t) >= 2)
            for t in toks
        ):
            parts.pop()
        else:
            break
    # 3) Suffix handling (Jr, Sr, II, III, IV...)
    suffix = ''
    if len(parts) >= 3 and normalize_token(parts[-1]) in SUFFIXES:
        suffix = parts.pop()
    # 4) Parse based on comma format or space format
    if len(parts) >= 2 and ',' in s:
        last_base = parts[0]
        right = strip_leading_titles(parts[1])
        right_tokens = right.split()
        kept_tokens = []
        for t in right_tokens:
            if t in DASH_ONLY:
                continue
            key = normalize_token(t)
            if key in DEGREE_MAP:
                degrees.append(DEGREE_MAP[key])
            elif UPPERCASE_DEG_RE.match(t) and t.upper() not in ROMAN_NUMERALS and len(t) >= 2:
                degrees.append(t.upper())
            else:
                kept_tokens.append(t)
        if len(kept_tokens) == 0:
            first, middle = '', ''
        elif len(kept_tokens) == 1:
            first, middle = kept_tokens[0], ''
        else:
            first, middle = kept_tokens[0], ' '.join(kept_tokens[1:])
        last = last_base if not suffix else f"{last_base} {suffix.upper()}"
    else:
        # "Firstname Middlename Lastname [Degrees]" (no comma)
        name2 = strip_leading_titles(' '.join(parts))
        tokens = name2.split()
        kept = []
        for t in tokens:
            if t in DASH_ONLY:
                continue
            key = normalize_token(t)
            if key in DEGREE_MAP:
                degrees.append(DEGREE_MAP[key])
            elif UPPERCASE_DEG_RE.match(t) and t.upper() not in ROMAN_NUMERALS and len(t) >= 2:
                degrees.append(t.upper())
            else:
                kept.append(t)
        if len(kept) == 0:
            medical_degree = ', '.join(dict.fromkeys(degrees)) if degrees else 'NA'
            return '', '', '', medical_degree
        if len(kept) == 1:
            first, last, middle = kept[0], '', ''
        elif len(kept) == 2:
            first, last, middle = kept[0], kept[1], ''
        else:
            first, last = kept[0], kept[-1]
            middle = ' '.join(kept[1:-1])
    # 5) Normalize degree list: de-duplicate preserving order, join with comma+space
    if degrees:
        seen = set()
        deg_norm = []
        for d in degrees:
            if d not in seen:
                seen.add(d)
                deg_norm.append(d)
        medical_degree = ', '.join(deg_norm)
    else:
        medical_degree = 'NA'
    # 6) Titlecase name parts
    return smart_title(first), smart_title(last), smart_title(middle), medical_degree

def is_organization_entry(s: str) -> bool:
    """Return True if string s appears to be an organization based on ORG_RE."""
    if not isinstance(s, str) or not s.strip():
        return False
    return ORG_RE.search(s) is not None

def main():
    # Load input xlsx
    try:
        df = pd.read_excel(INPUT_XLSX, engine='openpyxl')
    except Exception:
        df = pd.read_excel(INPUT_XLSX, engine='xlrd')

    # Determine which column contains the name strings
    lower_map = {c: str(c).strip().lower() for c in df.columns}
    name_col = None
    for c in df.columns:
        if lower_map[c] in NAME_COL_CANDIDATES:
            name_col = c
            break
    if name_col is None:
        name_col = df.columns[0]

    # Parse all rows with existing logic
    parsed = df[name_col].apply(parse_with_degree)
    res = pd.DataFrame(parsed.tolist(), columns=['firstname', 'lastname', 'middlename', 'medical_degree'])
    res.insert(0, 'input_fullname', df[name_col].fillna('').astype(str))

    # NEW: organization detection (dynamic)
    # If input_fullname contains any organization keyword, set 'organization' to the raw string
    # and blank out the name fields and medical_degree for that row.
    res['organization'] = ''
    for idx, raw in res['input_fullname'].items():
        if is_organization_entry(raw):
            res.at[idx, 'organization'] = raw
            # keep columns blank when organization detected
            res.at[idx, 'firstname'] = ''
            res.at[idx, 'lastname'] = ''
            res.at[idx, 'middlename'] = ''
            res.at[idx, 'medical_degree'] = ''

    # Save Excel
    res.to_excel(OUTPUT_XLSX, index=False)

    # Save JSONL including organization
    with open(OUTPUT_JSONL, 'w', encoding='utf-8') as f:
        for _, r in res.iterrows():
            obj = {
                'firstname': r['firstname'] if r['firstname'] else '',
                'lastname': r['lastname'] if r['lastname'] else '',
                'middlename': r['middlename'] if r['middlename'] else '',
                'medical_degree': r['medical_degree'] if r['medical_degree'] else '',
                'organization': r['organization'] if r['organization'] else ''
            }
            f.write(json.dumps(obj, ensure_ascii=False) + '\n')

if __name__ == '__main__':
    main()
