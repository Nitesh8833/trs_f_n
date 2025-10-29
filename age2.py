import re, math
import pandas as pd
from src.utils.rulesengine import resolve_candidate_column


def transform_age_ranges(
    df: pd.DataFrame,
    value_col_candidates: list[str],
    out_col: str = "age",
) -> pd.DataFrame:
    """
    Parses human-readable age ranges and writes TWO columns:
      - f"min_{out_col}", f"max_{out_col}"

    Header semantics added:
      - If header contains "min", "minimum", or "from" (e.g., "Accepts Minimum Patient Age",
        "Minimum Age", "Min Age", "Age Range From") and the cell has a single number,
        force min=<that number>, max=150 for that row.
    Also:
      - "0 - 99+ years" (or similar with +/* on the upper bound) -> min=0, max=150.
      - Robust "and up/over/older/above" (incl. messy/collapsed forms) -> min=number, max=150.
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

    # Phrase lexicon
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

    # Robust "and up / over / older / above" detector (typos/collapses tolerated)
    def is_min_only_up_over(text: str):
        if text is None:
            return None

        t = str(text)
        tl = t.lower()

        # Allow up to 3 words between number and keyword (e.g., "18 years and older")
        m = re.search(
            r'(\d+(?:\.\d+)?)\s*(?:[a-z/]+\s*){0,3}?(?:'
            r'\+|\*|'
            r'(?:and|&|or)\s*(?:up|over|older|above)|'
            r'up|over|older|above'
            r')\b',
            tl, re.I
        )
        if m:
            return float(m.group(1))

        # Collapsed/typo forms (e.g., "18andUp", "18a ndUp")
        collapsed = re.sub(r'[^a-z0-9+*]', '', tl)
        m2 = re.search(r'^(\d+(?:\.\d+)?)((years?|yrs?|yr|yo|y/o)?)and(?:up|over|older|above)\b', collapsed, re.I)
        if m2:
            return float(m2.group(1))

        # Spelled-out spaced letters: "a n d up"
        m3 = re.search(r'(\d+(?:\.\d+)?)\s*a\s*n\s*d\s+u\s*p\b', tl, re.I)
        if m3:
            return float(m3.group(1))

        return None

    def parse_age_phrase(value: str):
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return (None, None, "empty")

        t_raw = str(value).strip()
        tl = t_raw.lower()
        tl_norm = tl.replace('—', '-').replace('–', '-').replace(' to ', '-')

        # Lexicon first
        for pat, fn in PHRASE_RULES:
            if pat.search(t_raw):
                return fn(t_raw)

        # SPECIAL: range upper has +/*  => left as min, max=150   e.g., "0 - 99+ years"
        m_rng_plus = re.search(
            r'(\d+(?:\.\d+)?)\s*[-~]\s*(\d+(?:\.\d+)?)\s*[\+\*]\b', tl_norm
        )
        if m_rng_plus:
            lo = float(m_rng_plus.group(1))
            hi = float(m_rng_plus.group(2))
            if lo > hi:
                lo, hi = hi, lo
            return (lo, 150.0, "range_upper_plus_as_150")

        # "+/*" min-only (either strict or inline)
        m_plus_strict = re.search(r'^\s*(\d+(?:\.\d+)?)\s*[\+\*]+\s*$', t_raw)
        if m_plus_strict:
            return (float(m_plus_strict.group(1)), 150.0, "min_plus_or_star")

        m_plus_inline = re.search(r'(\d+(?:\.\d+)?)\s*[\+\*]+', t_raw)
        if m_plus_inline:
            return (float(m_plus_inline.group(1)), 150.0, "min_plus_or_star_inline")

        # Unified "and up/over/older/above" => min=number, max=150
        up_over_num = is_min_only_up_over(t_raw)
        if up_over_num is not None:
            return (up_over_num, 150.0, "min_only_up_over")

        # Months explicit (with extended synonyms)
        months_years = parse_months_to_years(t_raw)
        if months_years is not None:
            # Treat up/over/older/above and +/* as min-only
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

        # Max-only (under/below/less than/up to ... OR "X and younger")
        if re.search(r'(under|below|less than|up to)\s*(\d+(?:\.\d+)?)', tl_norm) or \
           re.search(r'(\d+(?:\.\d+)?)\s*(and younger|younger)', tl_norm):
            nums = [float(x) for x in re.findall(r'\d+(?:\.\d+)?', tl_norm)]
            maxv = nums[-1] if nums else None
            return (0.0, maxv, "max_only")

        # Single number -> ambiguous (resolve via header rules later)
        if len(re.findall(r'\d+(?:\.\d+)?', tl_norm)) == 1:
            return (None, None, "single_ambiguous")

        return (None, None, "unparsed")

    # ---- Header helpers (explicit MIN/FROM semantics) ----
    def is_min_header(h: str):
        hl = str(h).lower()
        # Covers: "Accepts Minimum Patient Age", "Minimum Age", "Min Age", "Age Range From", etc.
        return bool(
            re.search(r'\b(min|minimum|from)\b', hl) or
            hl.endswith(' min') or hl.startswith('min') or
            any(k in hl for k in [' min', 'min ', '(min', '>=', 'lower bound'])
        )

    def is_max_header(h: str):
        hl = str(h).lower()
        return bool(
            hl.endswith(' max') or hl.startswith('max') or
            any(k in hl for k in [' max', 'maximum', 'max ', '(max', '<=', 'upper bound'])
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

        # If NOT a "min header" and it's a single bare number, assume it's a MAX (0..N)
        if note == "single_ambiguous" and count_numbers(v_text) == 1 and not is_min_header(h):
            only_num = extract_first_number(v_text)
            pmin, pmax, note = (0.0, only_num, "assumed_single_as_max")

        # ---- HEADER OVERRIDES ----
        # If it's a MIN header ("min", "minimum", "from"), force MIN semantics for a single number
        if is_min_header(h):
            num = extract_first_number(v_text)
            if num is not None:
                # If parser gave max_only/single_ambiguous/unparsed, override to min=num, max=150
                if note in ("single_ambiguous", "max_only", "unparsed"):
                    pmin, pmax, note = (num, 150.0, "header_forced_min")
                # If min wasn't set by parser, set it
                if pmin is None:
                    pmin = num
                # If only a single numeric token present and max missing, set max=150
                if (pmax is None) and (count_numbers(v_text) == 1):
                    pmax = 150.0

        # Max-header hint (if desired; does not override strong parses)
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
