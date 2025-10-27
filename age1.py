import re, math
import pandas as pd
from src.utils.rulesengine import resolve_candidate_column


def transform_age_ranges(
    df: pd.DataFrame,
    value_col_candidates: list[str],
    out_col: str = "age",
) -> pd.DataFrame:
    """
    Pure function: parses human-readable age ranges in a source column and writes
    TWO output columns:
        - f"min_{out_col}"  (e.g., 'min_age')
        - f"max_{out_col}"  (e.g., 'max_age')

    Args:
        df: Input DataFrame (NOT modified in-place).
        value_col_candidates: Candidate column names that contain the age text
                              (e.g., ["header_col_value", "headercolvalue", "Age"]).
        out_col: Name prefix for output columns (default "age").

    Returns:
        A copy of df with the 2 columns added/overwritten.
    """
    def norm_col(s: str) -> str:
        return re.sub(r'[^a-z0-9]+', '', str(s).lower())

    NUM_RE = re.compile(r'(\d+(?:\.\d+)?)')

    def extract_first_number(text: str):
        m = NUM_RE.search(text or '')
        return float(m.group(1)) if m else None

    def count_numbers(text: str) -> int:
        return len(re.findall(r'\d+(?:\.\d+)?', text or ''))

    PHRASE_RULES = [
        (re.compile(r'\b(newborn|neonate|neonatal)\b', re.I),
         lambda t: (0.0, 0.08, "lex_newborn")),
        (re.compile(r'\badult\s*only\b', re.I),
         lambda t: (18.0, 150.0, "lex_adult_only")),
        (re.compile(r'\badults?\b(?!\s*to)', re.I),
         lambda t: (18.0, 150.0, "lex_adult")),
        (re.compile(r'\badolescents?\s*to\s*adult(s)?\b', re.I),
         lambda t: (10.0, 150.0, "lex_adol_to_adult")),
        (re.compile(r'\bnewborn\b.*\bto\b.*\b(17|seventeen)\b', re.I),
         lambda t: (0.0, 17.0, "lex_newborn_to_17")),
        (re.compile(r'\bnewborn\b.*\bto\b.*\bage\s*17\b', re.I),
         lambda t: (0.0, 17.0, "lex_newborn_to_17")),
    ]

    def fuzzy_and_up(text: str):
        if text is None:
            return None
        t = re.sub(r'\s+', ' ', str(text).strip())
        m = re.search(r'^\s*(\d+(?:\.\d+)?)\s+(a\w*d)\s+(u\w*p)\b', t, flags=re.I)
        if m:
            return float(m.group(1))
        m2 = re.search(r'^\s*(\d+(?:\.\d+)?)\s+a\s*n\s*d\s+u\s+p\b', t, flags=re.I)
        if m2:
            return float(m2.group(1))
        return None

    def parse_months_to_years(text: str):
        m = re.search(r'(\d+(?:\.\d+)?)\s*(?:month|months)\b', text or '', re.I)
        return float(m.group(1)) / 12.0 if m else None

    def parse_age_phrase(value: str):
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return (None, None, "empty")
        t_raw = str(value).strip()
        tl = t_raw.lower()

        # Lexicon first
        for pat, fn in PHRASE_RULES:
            if pat.search(t_raw):
                return fn(t_raw)

        # "+/*" min-only: "18+", "18 *"
        m_plus_strict = re.search(r'^\s*(\d+(?:\.\d+)?)\s*[\+\*]+\s*$', t_raw)
        if m_plus_strict:
            return (float(m_plus_strict.group(1)), 150.0, "min_plus_or_star")

        # Inline variants: ">=18+", "age 18+ adults"
        m_plus_inline = re.search(r'(\d+(?:\.\d+)?)\s*[\+\*]+', t_raw)
        if m_plus_inline:
            return (float(m_plus_inline.group(1)), 150.0, "min_plus_or_star_inline")

        # Fuzzy "and up"
        up_num = fuzzy_and_up(t_raw)
        if up_num is not None:
            return (float(up_num), 150.0, "and_up")

        # Normalize hyphens and "to"
        tl_norm = tl.replace('—', '-').replace('–', '-').replace(' to ', '-')

        # No restriction
        if any(x in tl_norm for x in ['none', 'no restriction', 'all ages']):
            return (0.0, 150.0, "no_restriction")

        # "and older" / "& older" / "above" / "over"
        if re.search(r'(\d+(?:\.\d+)?)\s*(?:\+|\*|(?:and|&)\s*older|(?:and\s+above)|above|over)\b', tl_norm):
            val = extract_first_number(tl_norm)
            return (float(val) if val is not None else None, 150.0, "min_only_older")

        # Months explicit
        months_years = parse_months_to_years(t_raw)
        if months_years is not None:
            if any(x in tl_norm for x in ['and older', 'older', '+', '& older']):
                return (months_years, 150.0, "months_plus")
            if any(x in tl_norm for x in ['and younger', 'younger', 'under', 'below', 'less than', 'up to']):
                return (0.0, months_years, "months_max")
            return (months_years, 150.0, "months_as_min")

        # Range "a-b"
        m_rng = re.search(r'(\d+(?:\.\d+)?)\s*[-~]\s*(\d+(?:\.\d+)?)', tl_norm)
        if m_rng:
            lo = float(m_rng.group(1)); hi = float(m_rng.group(2))
            if lo > hi:
                lo, hi = hi, lo
            return (lo, hi, "range")

        # Max-only
        if re.search(r'(under|below|less than|up to)\s*(\d+(?:\.\d+)?)', tl_norm) or \
           re.search(r'(\d+(?:\.\d+)?)\s*(and younger|younger)', tl_norm):
            nums = [float(x) for x in re.findall(r'\d+(?:\.\d+)?', tl_norm)]
            maxv = nums[-1] if nums else None
            return (0.0, maxv, "max_only")

        # Single number -> ambiguous
        if len(re.findall(r'\d+(?:\.\d+)?', tl_norm)) == 1:
            return (None, None, "single_ambiguous")

        return (None, None, "unparsed")

    def is_min_header(h: str):
        hl = str(h).lower()
        return any(k in hl for k in [' min', 'minimum', 'min ', '(min', '>=', 'lower bound']) or \
               hl.endswith(' min') or hl.startswith('min')

    def is_max_header(h: str):
        hl = str(h).lower()
        return any(k in hl for k in [' max', 'maximum', 'max ', '(max', '<=', 'upper bound']) or \
               hl.endswith(' max') or hl.startswith('max')

    out = df.copy()
    cols = [c for c in out.columns]

    # Source value column from candidates
    val_col = resolve_candidate_column(cols, value_col_candidates)
    if val_col is None:
        return out

    # Optional header column for min/max disambiguation
    header_candidates_internal = [
        "header_col_name", "headercolname", "header_colName",
        "header", "column_name"
    ]
    header_col = resolve_candidate_column(cols, header_candidates_internal)

    MIN_DEFAULT, MAX_DEFAULT = 0.0, 150.0
    min_col = f"min_{out_col}"
    max_col = f"max_{out_col}"

    min_list, max_list = [], []

    headers_iter = out[header_col].astype(str) if header_col in out.columns else [""] * len(out)
    for h, v in zip(headers_iter, out[val_col]):
        pmin, pmax, note = parse_age_phrase(v)
        v_text = "" if (v is None or (isinstance(v, float) and math.isnan(v))) else str(v)

        # Ambiguous single number defaults
        if note == "single_ambiguous" and count_numbers(v_text) == 1 and not is_min_header(h):
            only_num = extract_first_number(v_text)
            pmin, pmax, note = (0.0, only_num, "assumed_single_as_max")

        # Header hints
        if is_min_header(h) and (pmin is None):
            pmin = extract_first_number(v_text)
        if is_max_header(h) and (pmax is None):
            pmax = extract_first_number(v_text)

        # Defaults
        if pmin is None: pmin = MIN_DEFAULT
        if pmax is None: pmax = MAX_DEFAULT

        min_list.append(round(float(pmin), 2))
        max_list.append(round(float(pmax), 2))

    out[min_col] = min_list
    out[max_col] = max_list

    return out
