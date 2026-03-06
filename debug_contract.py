"""
debug_contract.py - Dump all raw log rows for a specific contract number.

Usage:
    uv run python debug_contract.py <contract_number> [YYYY-MM]
"""

import sys
from pathlib import Path
from aggregate import LOGS_DIR, load_log, validate_and_standardize, filter_by_month

def main():
    if len(sys.argv) < 2:
        print("Usage: uv run python debug_contract.py <contract_number> [YYYY-MM]")
        sys.exit(1)

    target_cn = str(sys.argv[1]).strip()
    year, month = None, None
    if len(sys.argv) >= 3:
        year, month = map(int, sys.argv[2].split("-"))

    log_files = sorted(LOGS_DIR.glob("*.xlsm"))
    all_rows = []
    for path in log_files:
        rows, _ = load_log(path)
        all_rows.extend(rows)

    all_rows, _ = validate_and_standardize(all_rows)
    if year and month:
        all_rows = filter_by_month(all_rows, year, month)

    matches = [r for r in all_rows if str(r.get("AB") or "").strip() == target_cn]

    if not matches:
        print(f"No rows found for contract {target_cn}" + (f" in {year}-{month:02d}" if year else ""))
        return

    print(f"Found {len(matches)} rows for contract {target_cn}:\n")
    print(f"  {'FILE':<30} {'DATE':<12} {'TYPE':<5} {'AA':<4} {'GROSS':>10}  {'MARKET':<8} {'MONTH'}")
    print("  " + "-" * 90)
    for r in matches:
        print(
            f"  {r['_source_file']:<30} {str(r.get('B') or '')[:10]:<12} "
            f"{str(r.get('N') or ''):<5} {str(r.get('AA') or ''):<4} "
            f"${(r.get('P') or 0):>9,.2f}  {str(r.get('AC') or ''):<8} {r.get('S')}"
        )

    total = sum(r.get("P") or 0 for r in matches if isinstance(r.get("P"), (int, float)))
    print(f"\n  Total gross (all rows): ${total:,.2f}")

if __name__ == "__main__":
    main()
