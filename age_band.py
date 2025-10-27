import re
from typing import Optional, Tuple, List
import pandas as pd
from src.utils.rulesengine import resolve_candidate_column


def transform_min_max_and_sub_category(
    df: pd.DataFrame,
    first_name_candidates: list[str],
    out_col: str = "age",
) -> pd.DataFrame:
    """
    Pure function (no I/O):
    - Resolves the source column (holding messy age strings) from `first_name_candidates`.
    - Parses into f"Min_{out_col}" and f"Max_{out_col}".
    - If `header_colName` exists, fills/overwrites a `sub-category` column that points
      to the corresponding min/max column name for that header.

    Parameters mirror the example schema you provided:
      df: input DataFrame (NOT modified in place)
      first_name_candidates: candidate column names that may contain the age text
      out_col: base name used to create Min_{out_col} / Max_{out_col}
    """
    out = df.copy()
    cols: List[str] = [c for c in out.columns]

    # Resolve the age text column using your helper (same pattern as the template).
    src = resolve_candidate_column(cols, first_name_candidates)
    if src is None:
        # No source column found → behave like the template and return unchanged
        return out

    DEFAULT_MAX_AGE = 150.0

    # Header → kind mapping ("min" / "max"). Extend as needed.
    HEADER_TO_KIND = {
        # Common spellings
        "AGE": "min",
        "Age": "min",
        "Ages": "min",
        "AGE RESTRICTIONS": "min",
        "Age Range From": "min",
        "Age Ranges": "min",
        "Age Ranges (0-99)": "min",
        "Minimum Age": "min",
        "Min Age": "min",
        "Max Age": "max",
        "Maximum Age": "max",
        "MAX Age": "max",
        "MAXIMUM PATIENT AGE": "max",
        "PATIENT MIN AGE": "min",
        "PATIENT MAX AGE": "max",
        "Practice limitations: Min Age": "min",
        "Practice limitations: Max Age": "max",
        "Age Restrictions (required for all providers)": "min",
        # Long variants from screenshots
        "Address/Service Location Age Restrictions (EX: None, 18 & Younger, 19& older) [No Blanks]": "min",
        "Address/Service Location Age Restrictions (EX: None, 18 & Younger, 19 & Older) [No Blanks]": "min",
        # Accepts-Min-Age style
        "Accepts Minimum Patient Age": "min",
    }

    def _norm_header_key(s: str) -> str:
        return re.sub(r"\s+", " ", str(s).strip()).lower()

    NORMALIZED_HEADER_TO_KIND = { _norm_header_key(k): v for k, v in HEADER_TO_KIND.items() }

    # Regexes for parsing
    NUMBER_RE        = re.compile(r"(\d+(?:\.\d+)?)")
    NUMBER_ONLY_RE   = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*$", re.IGNORECASE)
    RANGE_RE         = re.compile(r"^\s*(\d+(?:\.\d+)?)[\s]*(?:-|–|to)[\s]*(\d+(?:\.\d+)?)\s*$", re.IGNORECASE)
    AND_UP_RE        = re.compile(r"^\s*(\d+(?:\.\d+)?)[\s\+&]*(\+|and\s*up|and\s*older|or\s*older|&\s*older)\s*$", re.IGNORECASE)
    UNDER_RE         = re.compile(r"^\s*(under|less\s*than)\s*(\d+(?:\.\d+)?)\s*$", re.IGNORECASE)
    YOUNGER_RE       = re.compile(r"^\s*(\d+(?:\.\d+)?)[\s]*(?:&|and)?\s*younger\s*$", re.IGNORECASE)
    MONTHS_RE        = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(month|months)\s*(?:and\s*older|and\s*up)?\s*$", re.IGNORECASE)

    LABEL_MAP = {
        "adult": (18.0, DEFAULT_MAX_AGE),
        "adults": (18.0, DEFAULT_MAX_AGE),
        "pediatric": (0.0, 17.0),
        "children": (0.0, 17.0),
        "child": (0.0, 17.0),
        "members": (0.0, DEFAULT_MAX_AGE),
        "no age restriction": (0.0, DEFAULT_MAX_AGE),
        "no age restrictions": (0.0, DEFAULT_MAX_AGE),
        "none": (0.0, DEFAULT_MAX_AGE),
    }

    def _to_float(x: str) -> Optional[float]:
        try:
            return float(x)
        except Exception:
            return None

    min_col = f"Min_{out_col}"
    max_col = f"Max_{out_col}"
    if min_col not in out.columns:
        out[min_col] = None
    if max_col not in out.columns:
        out[max_col] = None

    def _parse_age_entry(val: object) -> Tuple[Optional[float], Optional[float]]:
        if val is None:
            return (None, None)
        s = str(val).strip()
        if not s:
            return (None, None)
        s_lower = s.lower()

        # label shortcuts
        for label, rng in LABEL_MAP.items():
            if label in s_lower:
                return rng

        # months → years (0..N_years)
        m = MONTHS_RE.match(s_lower)
        if m:
            months = _to_float(m.group(1))
            if months is not None:
                return (0.0, max(0.0, months / 12.0))

        # “18+”, “18 and up”, “18 & older”
        m = AND_UP_RE.match(s_lower)
        if m:
            n = _to_float(m.group(1))
            return (n, DEFAULT_MAX_AGE)

        # “Under 18” / “Less than 18”
        m = UNDER_RE.match(s_lower)
        if m:
            n = _to_float(m.group(2))
            return (0.0, (n - 1) if n is not None else None)

        # “18 & younger”
        m = YOUNGER_RE.match(s_lower)
        if m:
            n = _to_float(m.group(1))
            return (0.0, n)

        # “13-18”, “13 to 18”
        m = RANGE_RE.match(s_lower)
        if m:
            a = _to_float(m.group(1))
            b = _to_float(m.group(2))
            if a is not None and b is not None:
                return (min(a, b), max(a, b))

        # lone number → treat as minimum
        m = NUMBER_ONLY_RE.match(s_lower)
        if m:
            n = _to_float(m.group(1))
            return (n, DEFAULT_MAX_AGE)

        # fallback: any numbers present
        nums = [ _to_float(x) for x in NUMBER_RE.findall(s_lower) ]
        nums = [x for x in nums if x is not None]
        if len(nums) >= 2:
            return (min(nums[0], nums[1]), max(nums[0], nums[1]))
        if len(nums) == 1:
            return (nums[0], DEFAULT_MAX_AGE)
        return (None, None)

    parsed = out[src].apply(_parse_age_entry)
    out[min_col] = parsed.apply(lambda t: t[0])
    out[max_col] = parsed.apply(lambda t: t[1])

    # Optional: derive `sub-category` from header_colName if present
    if "header_colName" in out.columns:
        def _label_for_header(raw_key: object) -> str:
            key_norm = _norm_header_key(raw_key)
            kind = NORMALIZED_HEADER_TO_KIND.get(key_norm)
            if kind == "min":
                return min_col
            if kind == "max":
                return max_col
            return ""
        out["sub-category"] = out["header_colName"].apply(_label_for_header)

    return out
