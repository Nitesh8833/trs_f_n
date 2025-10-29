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
    """
    # ---------------------------- helpers ----------------------------
    def norm_col(s: str) -> str:
        return re.sub(r'[^a-z0-9]+', '', str(s).lower())

    NUM_RE = re.compile(r'(\d+(?:\.\d+)?)')

    def extract_first_number(text: str):
        m = NUM_RE.search(text or '')
        return float(m.group(1)) if m else None

    def count_numbers(text: str) -> int:
        return len(re.findall(r'\d+(?:\.\d+)?', text or ''))

    def parse_months_to_years(text: str):
        """
        Accept broad month synonyms: month(s), mon(s), mo(s), mth(s), m.
        """
        m = re.search(
            r'(\d+(?:\.\d+)?)\s*(?:month|months|mon|mons|mo|mos|mth|mths|m)\b',
            text or '',
            re.I,
        )
        return float(m.group(1)) / 12.0 if m else None

    # Phrase lexicon (kept from your version)
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

    # ---- NEW: robust "and up / over / older / above" detector (with typos/collapses) ----
    # Handles: 18 and up, 18 & up, 18 or up, 18 and over/older/above,
    #          18and up, 18andUp, 18 a nd Up, 18 years and older, etc.
    def is_min_only_up_over(text: str):
        if text is None:
            return None

        t = str(text)
        tl = t.lower()

        # a) Special-cased: allow up to 3 words (e.g., "years") between the number and the keyword
        m = re.search(
            r'(\d+(?:\.\d+)?)\s*(?:[a-z/]+\s*){0,3}?(?:'
            r'\+|\*|'
            r'(?:and|&|or)\s*(?:up|over|older|above)|'
            r'up|over|older|above'
            r')\b',
            tl,
            re.I,
        )
        if m:
            return float(m.group(1))

        # b) Collapsed/typo forms (strip non-alnum except +, *)
        collapsed = re.sub(r'[^a-z0-9+*]', '', tl)
        m2 = re.search(r'^(\d+(?:\.\d+)?)((years?|yrs?|yr|yo|y/o)?)and(?:up|over|older|above)\b', collapsed, re.I)
        if m2:
            return float(m2.group(1))

        # c) "a n d up" spaced letters (already handled largely by (a), but keep a tolerant fallback)
        m3 = re.search(r'(\d+(?:\.\d+)?)\s*a\s*n\s*d\s+u\s*p\b', tl, re.I)
        if m3:
            return float(m3.group(1))

        return None

    def parse_age_phrase(value: str):
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return (None, None, "empty")

        t_raw = str(value).strip()
        tl = t_raw.lower()

        # Lexicon first
        for pat, fn in PHRASE_RULES:
            if pat.search(t_raw):
                return fn(t_raw)

        # Normalize some punctuation/phrasing for easier matching
        tl_norm = tl.replace('—', '-').replace('–', '-').replace(' to ', '-')

        # ---- NEW: Special case "range upper has +/*" like "0 - 99+ years" => min=left, max=150
        m_rng_plus = re.search(
            r'(\d+(?:\.\d+)?)\s*[-~]\s*(\d+(?:\.\d+)?)\s*[\+\*]\b',
            tl_norm
        )
        if m_rng_plus:
            lo = float(m_rng_plus.group(1))
            hi = float(m_rng_plus.group(2))
            if lo > hi:
                lo, hi = hi, lo
            return (lo, 150.0, "range_upper_plus_as_150")

        # "+/*" min-only: "18+", "18 *" (strict lone or inline) — keep but AFTER the range+ case above
        m_plus_strict = re.search(r'^\s*(\d+(?:\.\d+)?)\s*[\+\*]+\s*$', t_raw)
        if m_plus_strict:
            return (float(m_plus_strict.group(1)), 150.0, "min_plus_or_star")

        m_plus_inline = re.search(r'(\d+(?:\.\d+)?)\s*[\+\*]+', t_raw)
        if m_plus_inline:
            return (float(m_plus_inline.group(1)), 150.0, "min_plus_or_star_inline")

        # ---- NEW: Unified "and up/over/older/above" (incl. messy forms) => min=number, max=150
        up_over_num = is_min_only_up_over(t_raw)
        if up_over_num is not None:
            return (up_over_num, 150.0, "min_only_up_over")

        # Months explicit (with extended synonyms)
        months_years = parse_months_to_years(t_raw)
        if months_years is not None:
            # Treat "and up/over/older/above" and +/* as min-only
            if re.search(r'(?:\+|\*|(?:and|&|or)\s*(?:up|over|older|above)|up|over|older|above)\b', tl):
                return (months_years, 150.0, "months_plus_or_up")
            # Max-only variants
            if re.search(r'(and\s+younger|younger|under|below|less than|up to)\b', tl):
                return (0.0, months_years, "months_max")
            # Otherwise treat as min (conservative)
            return (months_years, 150.0, "months_as_min")

        # Range "a-b"
        m_rng = re.search(r'(\d+(?:\.\d+)?)\s*[-~]\s*(\d+(?:\.\d+)?)', tl_norm)
        if m_rng:
            lo = float(m_rng.group(1)); hi = float(m_rng.group(2))
            if lo > hi:
                lo, hi = hi, lo
            return (lo, hi, "range")

        # Max-only (under/below/less than/up to ... or "X and younger")
        if re.search(r'(under|below|less than|up to)\s*(\d+(?:\.\d+)?)', tl_norm) or \
           re.search(r'(\d+(?:\.\d+)?)\s*(and younger|younger)', tl_norm):
            nums = [float(x) for x in re.findall(r'\d+(?:\.\d+)?', tl_norm)]
            maxv = nums[-1] if nums else None
            return (0.0, maxv, "max_only")

        # Single number -> ambiguous (we'll resolve via header rules later)
        if len(re.findall(r'\d+(?:\.\d+)?', tl_norm)) == 1:
            return (None, None, "single_ambiguous")

        return (None, None, "unparsed")

    # ---- Header helpers (enhanced "From") ----
    def is_min_header(h: str):
        hl = str(h).lower()
        return (
            any(k in hl for k in [' min', 'minimum', 'min ', '(min', '>=', 'lower bound']) or
            hl.endswith(' min') or hl.startswith('min') or
            re.search(r'\bfrom\b', hl) is not None  # NEW: treat "From" as min header
        )

    def is_max_header(h: str):
        hl = str(h).lower()
        return (
            any(k in hl for k in [' max', 'maximum', 'max ', '(max', '<=', 'upper bound']) or
            hl.endswith(' max') or hl.startswith('max')
            # (Optional) If you ever want "To" as max header, add: or re.search(r'\bto\b', hl)
        )

    # ---------------------------- main ----------------------------
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

        # If header is a "From" style min header, prefer treating a single bare number as MIN
        # (the next block won't trigger because we check "not is_min_header(h)")
        if note == "single_ambiguous" and count_numbers(v_text) == 1 and not is_min_header(h):
            only_num = extract_first_number(v_text)
            pmin, pmax, note = (0.0, only_num, "assumed_single_as_max")

        # Header hints (these run regardless of above)
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
