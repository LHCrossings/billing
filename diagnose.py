"""
diagnose.py - Check key columns for None/missing values after aggregation.
Helps catch formula caching issues before we do any real work with the data.
"""

from aggregate import load_log, LOGS_DIR, COL_LABELS

# Key columns to check (by letter)
CHECK_COLS = {
    "N": "Type",
    "P": "Gross",
    "S": "Month",
    "Y": "Billing Type",
    "AA": "Affidavit",
    "AC": "Market",
}

def main():
    log_files = sorted(LOGS_DIR.glob("*.xlsm"))

    total = 0
    nulls = {col: [] for col in CHECK_COLS}

    for path in log_files:
        rows, _ = load_log(path)
        for row in rows:
            total += 1
            for col, label in CHECK_COLS.items():
                val = row.get(col)
                if val is None or (isinstance(val, str) and not val.strip()):
                    nulls[col].append({
                        "file": path.name,
                        "bill_code": row.get("A"),
                        "start_date": row.get("B"),
                        "col": col,
                        "label": label,
                    })

    print(f"Checked {total} rows across {len(log_files)} files\n")

    any_issues = False
    for col, label in CHECK_COLS.items():
        count = len(nulls[col])
        if count == 0:
            print(f"  Col {col} ({label}): OK")
        else:
            any_issues = True
            print(f"  Col {col} ({label}): {count} blank/None values")
            for item in nulls[col][:10]:  # show first 10
                print(f"    {item['file']} | {item['bill_code']} | Start: {item['start_date']}")
            if count > 10:
                print(f"    ... and {count - 10} more")

    if not any_issues:
        print("\nAll key columns look clean — no unexpected blanks.")

if __name__ == "__main__":
    main()
