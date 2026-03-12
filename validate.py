"""
validate.py - Compare actual billed gross from weekly logs against expected amounts in the DB.

Loads all log files and Worldlink CSVs for a billing month, sums actual gross by
contract + market, and compares against order_monthly expected amounts in the database.

Usage:
    uv run python validate.py YYYY-MM [--logs-dir PATH] [--db PATH]

Output categories:
    MATCHED    - contract+market in both logs and DB, amounts agree (within $0.02)
    OVER       - actual exceeds expected
    UNDER      - actual is less than expected
    MISSING    - contract+market in DB but no log rows found
    UNEXPECTED - contract+market in logs but not in DB (new/unknown orders)
    WORLDLINK  - Worldlink rows (not in DB; validation skipped, listed for reference)
    NO CONTRACT - rows without a contract number (PRD, CRD, etc.)
"""

import argparse
import sys
from collections import defaultdict
from pathlib import Path

from aggregate import (
    LOGS_DIR,
    MONTH_NAMES,
    filter_by_month,
    filter_rows,
    has_contract,
    load_log,
    validate_and_standardize,
)
from orders_db import DB_PATH, get_conn, init_db
from worldlink import WORLDLINK_DIR, load_all_worldlink

TOLERANCE = 0.005  # floating-point representation epsilon only — amounts should match exactly


def load_actual_from_logs(logs_dir: Path, year: int, month: int) -> tuple[dict, list, list]:
    """
    Load all log files and aggregate billable rows for the given month.

    Returns:
        actual:       {(contract_int, market): gross}  — rows with a valid contract number
        no_contract:  list of {key, bill_code, estimate, market, gross, row_count}
        warnings:     list of strings
    """
    log_files = sorted(logs_dir.glob("*.xlsm"))
    all_rows = []
    all_warnings = []

    for path in log_files:
        rows, warnings = load_log(path)
        all_rows.extend(rows)
        all_warnings.extend(warnings)

    all_rows, issues = validate_and_standardize(all_rows)
    for issue in issues:
        all_warnings.append(
            f"Month correction: {issue['file']} | {issue['bill_code']} | {issue['issue']}"
        )

    month_rows = filter_by_month(all_rows, year, month)
    _, billable, _, _ = filter_rows(month_rows)

    actual: dict[tuple[int, str], float] = defaultdict(float)
    no_contract: dict[str, dict] = {}

    for row in billable:
        gross = row.get("P") or 0
        if not isinstance(gross, (int, float)):
            gross = 0.0
        market = str(row.get("AC") or "").strip() or "Unknown"

        if has_contract(row):
            try:
                cn = int(float(str(row["AB"]).strip()))
            except (ValueError, TypeError):
                cn_str = str(row["AB"]).strip()
                a = str(row.get("A") or "").strip()
                o = str(row.get("O") or "").strip()
                key = f"{a}|{o}" if o else a
                if key not in no_contract:
                    no_contract[key] = {
                        "key": key, "bill_code": a, "estimate": o,
                        "market": market, "gross": 0.0, "row_count": 0,
                        "note": f"non-integer contract '{cn_str}'",
                    }
                no_contract[key]["gross"] += float(gross)
                no_contract[key]["row_count"] += 1
                continue
            actual[(cn, market)] += float(gross)
        else:
            a = str(row.get("A") or "").strip()
            o = str(row.get("O") or "").strip()
            key = f"{a}|{o}" if o else a
            if key not in no_contract:
                no_contract[key] = {
                    "key": key, "bill_code": a, "estimate": o,
                    "market": market, "gross": 0.0, "row_count": 0,
                }
            no_contract[key]["gross"] += float(gross)
            no_contract[key]["row_count"] += 1

    return dict(actual), list(no_contract.values()), all_warnings


def load_expected_from_db(conn, year: int, month: int) -> dict[tuple[int, str], tuple[float, str, str]]:
    """
    Load expected monthly amounts from the DB for the given month.

    Returns:
        {(contract_number, market): (expected_gross, advertiser, agency)}
    """
    rows = conn.execute("""
        SELECT om.contract_number, om.market, om.gross,
               o.advertiser, o.client AS agency
        FROM order_monthly om
        JOIN orders o ON o.contract_number = om.contract_number
        WHERE om.year = ? AND om.month = ?
    """, (year, month)).fetchall()

    return {
        (row["contract_number"], row["market"]): (
            row["gross"],
            row["advertiser"] or "?",
            row["agency"] or "?",
        )
        for row in rows
    }


def compare(
    actual: dict[tuple[int, str], float],
    expected: dict[tuple[int, str], tuple[float, str, str]],
) -> dict[str, list]:
    """
    Compare actual vs expected gross amounts.

    Returns a dict with keys: matched, over, under, missing, unexpected.
    Each value is a list of result dicts.
    """
    results = {"matched": [], "over": [], "under": [], "missing": [], "unexpected": []}
    all_keys = set(actual) | set(expected)

    for key in sorted(all_keys):
        cn, market = key
        act = actual.get(key)
        exp_data = expected.get(key)

        if exp_data is not None:
            exp_gross, advertiser, agency = exp_data
        else:
            exp_gross, advertiser, agency = None, "?", "?"

        entry = {
            "contract": cn,
            "market": market,
            "advertiser": advertiser,
            "agency": agency,
            "actual": act,
            "expected": exp_gross,
            "diff": (act - exp_gross) if (act is not None and exp_gross is not None) else None,
        }

        if act is None:
            results["missing"].append(entry)
        elif exp_gross is None:
            results["unexpected"].append(entry)
        elif abs(act - exp_gross) <= TOLERANCE:
            results["matched"].append(entry)
        elif act > exp_gross:
            results["over"].append(entry)
        else:
            results["under"].append(entry)

    return results


def print_detail_rows(rows: list[dict], show_diff: bool = True):
    for r in rows:
        act_str = f"${r['actual']:>10,.2f}" if r["actual"] is not None else f"{'(none)':>11}"
        exp_str = f"${r['expected']:>10,.2f}" if r["expected"] is not None else f"{'(none)':>11}"
        diff_str = ""
        if show_diff and r["diff"] is not None:
            diff_str = f"  diff {'+' if r['diff'] >= 0 else ''}{r['diff']:,.2f}"
        print(
            f"  [{r['contract']}] {r['advertiser']:<28} {r['agency']:<28} "
            f"{r['market']:<8} actual {act_str}  expected {exp_str}{diff_str}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Compare actual billed gross from logs against expected amounts in the DB."
    )
    parser.add_argument("month", metavar="YYYY-MM", help="Billing month to validate")
    parser.add_argument("--logs-dir", default=str(LOGS_DIR), help="Directory containing log .xlsm files")
    parser.add_argument("--worldlink-dir", default=str(WORLDLINK_DIR), help="Directory containing Worldlink CSVs")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to billing database")
    parser.add_argument("--no-worldlink", action="store_true", help="Skip Worldlink CSV loading")
    args = parser.parse_args()

    try:
        year, month = map(int, args.month.split("-"))
    except ValueError:
        print(f"Invalid month format '{args.month}'. Use YYYY-MM.")
        sys.exit(1)

    logs_dir = Path(args.logs_dir)
    worldlink_dir = Path(args.worldlink_dir)
    db_path = Path(args.db)

    if not db_path.exists():
        print(f"Database not found: {db_path}")
        print("Run backfill.py first.")
        sys.exit(1)
    init_db(db_path)

    print(f"=== BILLING VALIDATION — {MONTH_NAMES[month]} {year} ===\n")

    # --- Load actual from logs ---
    print(f"Loading log files from {logs_dir} ...")
    actual, no_contract, log_warnings = load_actual_from_logs(logs_dir, year, month)
    print(f"  {len(actual)} contract+market groups with actual gross")
    print(f"  {len(no_contract)} no-contract groups")

    # --- Load Worldlink ---
    wl_actual: dict[tuple[int, str], float] = {}
    wl_no_contract: list[dict] = []

    if not args.no_worldlink:
        print(f"\nLoading Worldlink CSVs from {worldlink_dir} ...")
        try:
            wl_rows, wl_warnings = load_all_worldlink(
                worldlink_dir=worldlink_dir,
                billing_year=year,
                billing_month=month,
            )
            log_warnings.extend(wl_warnings)
            # Sum Worldlink actual by contract + market (col AC = accounting market)
            for row in wl_rows:
                gross = row.get("P") or 0
                if not isinstance(gross, (int, float)):
                    gross = 0.0
                market = str(row.get("AC") or "").strip() or "Unknown"
                if has_contract(row):
                    try:
                        cn = int(float(str(row["AB"]).strip()))
                        wl_actual[(cn, market)] = wl_actual.get((cn, market), 0.0) + float(gross)
                    except (ValueError, TypeError):
                        a = str(row.get("A") or "").strip()
                        o = str(row.get("O") or "").strip()
                        key = f"{a}|{o}" if o else a
                        wl_no_contract.append({
                            "key": key, "bill_code": a, "estimate": o,
                            "market": market, "gross": float(gross), "row_count": 1,
                        })
                else:
                    a = str(row.get("A") or "").strip()
                    o = str(row.get("O") or "").strip()
                    key = f"{a}|{o}" if o else a
                    wl_no_contract.append({
                        "key": key, "bill_code": a, "estimate": o,
                        "market": market, "gross": float(gross), "row_count": 1,
                    })
            print(f"  {len(wl_actual)} Worldlink contract+market groups")
        except SystemExit as e:
            print(f"  Worldlink loading halted: {e}")

    # --- Load expected from DB ---
    print(f"\nLoading expected amounts from {db_path} ...")
    with get_conn(db_path) as conn:
        expected = load_expected_from_db(conn, year, month)
        # Load all known contract numbers so we can distinguish "registered but no monthly"
        # (PRD/CRD/one-time) from truly unknown contracts
        known_contracts: set[int] = {
            row[0] for row in conn.execute("SELECT contract_number FROM orders").fetchall()
        }
    print(f"  {len(expected)} contract+market groups in DB")

    # --- Compare ---
    results = compare(actual, expected)

    # --- Summaries ---
    total_actual   = sum(actual.values())
    total_expected = sum(v[0] for v in expected.values())
    total_wl       = sum(wl_actual.values())
    total_nc       = sum(g["gross"] for g in no_contract) + sum(g["gross"] for g in wl_no_contract)

    print(f"\n--- SUMMARY ---")
    print(f"  Expected (DB):          ${total_expected:>12,.2f}  ({len(expected)} groups)")
    print(f"  Actual - regular logs:  ${total_actual:>12,.2f}  ({len(actual)} groups)")
    if wl_actual:
        print(f"  Actual - Worldlink:     ${total_wl:>12,.2f}  ({len(wl_actual)} groups)")
    if no_contract or wl_no_contract:
        print(f"  No-contract rows:       ${total_nc:>12,.2f}  ({len(no_contract) + len(wl_no_contract)} groups)")
    diff = total_actual - total_expected
    print(f"  Regular vs expected:    ${diff:>+12,.2f}")

    print(f"\n--- MATCH RESULTS ---")
    print(f"  Matched:    {len(results['matched'])} groups")
    print(f"  Over:       {len(results['over'])} groups")
    print(f"  Under:      {len(results['under'])} groups")
    print(f"  Missing:    {len(results['missing'])} groups  (in DB, no log rows)")
    print(f"  Unexpected: {len(results['unexpected'])} groups  (in logs, not in DB)")

    if results["over"]:
        print(f"\n--- OVER BUDGET ({len(results['over'])}) ---")
        print_detail_rows(results["over"])

    if results["under"]:
        print(f"\n--- UNDER BUDGET ({len(results['under'])}) ---")
        print_detail_rows(results["under"])

    if results["missing"]:
        print(f"\n--- MISSING FROM LOGS ({len(results['missing'])}) ---")
        print_detail_rows(results["missing"], show_diff=False)

    # Split unexpected into truly unknown vs. registered orders with no monthly expectation
    # (PRD, CRD, one-time charges that share a contract number with an airtime order)
    unexpected_unknown = [r for r in results["unexpected"] if r["contract"] not in known_contracts]
    unexpected_known   = [r for r in results["unexpected"] if r["contract"] in known_contracts]

    if unexpected_unknown:
        print(f"\n--- UNEXPECTED IN LOGS (not in DB) ({len(unexpected_unknown)}) ---")
        print_detail_rows(unexpected_unknown, show_diff=False)

    if unexpected_known:
        print(f"\n--- REGISTERED, NO MONTHLY EXPECTED ({len(unexpected_known)}) ---")
        print(f"  (PRD/CRD/one-time — registered in orders table but no monthly amount on file)")
        print_detail_rows(unexpected_known, show_diff=False)

    if wl_actual:
        print(f"\n--- WORLDLINK ({len(wl_actual)} groups, ${total_wl:,.2f}) ---")
        for (cn, market), gross in sorted(wl_actual.items()):
            print(f"  [{cn}] {market:<8} ${gross:,.2f}")

    if no_contract or wl_no_contract:
        all_nc = no_contract + wl_no_contract
        print(f"\n--- NO CONTRACT ({len(all_nc)} groups, ${total_nc:,.2f}) ---")
        for g in sorted(all_nc, key=lambda x: x["bill_code"]):
            wl_tag = " [Worldlink]" if g in wl_no_contract else ""
            print(
                f"  {g['bill_code']:<30} est {g['estimate'] or '(none)':<12} "
                f"{g['market']:<8} ${g['gross']:>10,.2f}  ({g['row_count']} rows){wl_tag}"
            )

    if log_warnings:
        print(f"\n--- WARNINGS ({len(log_warnings)}) ---")
        for w in log_warnings:
            print(f"  {w}")


if __name__ == "__main__":
    main()
