# ========================== PARSE ENTRY (continued) ========================== #
def parse_age_entry(raw: str) -> Tuple[Optional[float], Optional[float]]:
    if not raw:
        return (None, None)

    s_clean = str(raw).strip().lower()

    # Single numeric entry
    if NUMBER_ONLY_RE.match(s_clean):
        num = parse_numeric(s_clean)
        return (num, clamp_age(num))

    # No age restriction indicators
    if "no restriction" in s_clean or "no age restriction" in s_clean or "no age restrictions" in s_clean:
        return (0, DEFAULT_MAX_AGE)

    # Extract any numbers from the string
    nums = re.findall(r"\d+", s_clean)
    if len(nums) == 1:
        n = parse_numeric(nums[0])
        if n is not None:
            return (0, clamp_age(n)) if n == 0 else (n, clamp_age(n))
    elif len(nums) >= 2:
        a = parse_numeric(nums[0])
        b = parse_numeric(nums[1])
        if a is not None and b is not None:
            return (min(a, b), clamp_age(max(a, b)))

    return (None, None)

# ========================== NORMALIZE HEADER ========================== #
def normalize_header_col_value(series: pd.Series) -> pd.DataFrame:
    min_vals = []
    max_vals = []

    for v in series:
        mn, mx = parse_age_entry(v)
        min_vals.append(mn)
        max_vals.append(mx)

    return pd.DataFrame({"min_Age": min_vals, "max_Age": max_vals})

# ========================== MAIN PROCESS FUNCTION ========================== #
def process_excel(
    input_path: str,
    sheet_name: Optional[str] = None,
    header_col_value: Optional[str] = None,
    output_path: Optional[str] = None,
) -> str:
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    xl = pd.ExcelFile(input_path)

    if sheet_name is None:
        if len(xl.sheet_names) == 1:
            sheet_name = xl.sheet_names[0]
        else:
            raise ValueError(f"Specify sheet_name; multiple sheets found: {xl.sheet_names}")

    if sheet_name not in xl.sheet_names:
        raise ValueError(f"Sheet {sheet_name!r} not found in workbook")

    df = xl.parse(sheet_name=sheet_name)

    if header_col_value is None:
        raise ValueError("Column header_col_value not provided")

    if header_col_value not in df.columns:
        raise ValueError(f"Column {header_col_value!r} not found in {sheet_name!r}")

    # Apply parsing
    min_vals, max_vals = [], []
    for _, row in df.iterrows():
        mn, mx = apply_special_rules(row[header_col_value], header_name=header_col_value)
        if mn is None and mx is None:
            mn, mx = parse_age_entry(row[header_col_value])
        min_vals.append(mn)
        max_vals.append(mx)

    df.insert(df.columns.get_loc(header_col_value) + 1, "min_Age", min_vals)
    df.insert(df.columns.get_loc(header_col_value) + 2, "max_Age", max_vals)

    # Save output
    out_path = output_path or re.sub(r"(\.xlsx?)$", r"_output\1", input_path)
    df.to_excel(out_path, index=False)

    return out_path

# ========================== SCRIPT ENTRY POINT ========================== #
if __name__ == "__main__":
    print("💡 Direct invocation without command-line arguments.")
    print(f"Processing: {INPUT_EXCEL_PATH}")

    try:
        written = process_excel(
            input_path=INPUT_EXCEL_PATH,
            sheet_name=SHEET_NAME,
            header_col_value="Age_Band",
            output_path=OUTPUT_EXCEL_PATH,
        )
        print(f"✅ File processed successfully. Output saved to:\n{written}")
    except Exception as e:
        print(f"❌ Error: {e}")
