"""
backfill.py - Scan M:\\Clients for order files and populate the billing database.

Usage:
    uv run python backfill.py [options]

Options:
    --since YYYY-MM-DD    Only process files modified on or after this date (default: 2025-01-01)
    --clients-dir PATH    Root directory to scan for order files (default: M:\\Clients)
    --db PATH             Path to billing SQLite database (default: M:\\Accounting\\Billing\\billing.db)
    --dry-run             Parse files and print results without writing to the database
"""

import argparse
import os
from datetime import date, datetime
from pathlib import Path
from typing import Iterator

from order_parser import parse_order_file
from orders_db import DB_PATH, get_conn, init_db, upsert_monthly, upsert_order

CLIENTS_DIR = Path(r"M:\Clients")
SINCE_DATE = date(2024, 1, 1)

# Directories to skip entirely during the walk (case-insensitive match on directory name).
# - WorldLink: Run Sheets in these files are recycled templates with stale data.
#   Worldlink billing comes from Etere CSVs processed by worldlink.py, not order files.
#   FUTURE: parse Worldlink Sales Confirmations for metadata only (client, contact, market),
#   then populate order_monthly from worldlink.py output at billing time so Worldlink
#   revenue appears in DB for reporting and validation.
# - !!Archived Clients: old clients, not relevant for current billing
# - !Sample Orders: templates, not real orders
SKIP_DIRS = {"worldlink", "!!archived clients (more than 3 years old)", "!sample orders"}


def iter_order_files(clients_dir: Path, since: date) -> Iterator[Path]:
    """Walk clients_dir, yield .xlsx files modified on or after since."""
    for root, dirs, files in os.walk(clients_dir):
        dirs[:] = sorted(
            d for d in dirs
            if not d.startswith(".") and d.lower() not in SKIP_DIRS
        )
        for fname in sorted(files):
            if not fname.lower().endswith(".xlsx"):
                continue
            path = Path(root) / fname
            try:
                mtime = datetime.fromtimestamp(path.stat().st_mtime).date()
            except OSError:
                continue
            if mtime >= since:
                yield path


def main():
    parser = argparse.ArgumentParser(description="Backfill billing DB from M:\\Clients order files.")
    parser.add_argument(
        "--since", default=str(SINCE_DATE),
        help="Only process files modified on or after this date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--clients-dir", default=str(CLIENTS_DIR),
        help="Root directory to scan for order files"
    )
    parser.add_argument(
        "--db", default=str(DB_PATH),
        help="Path to billing SQLite database"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse and print without writing to the database"
    )
    args = parser.parse_args()

    since = date.fromisoformat(args.since)
    clients_dir = Path(args.clients_dir)
    db_path = Path(args.db)

    print(f"Scanning {clients_dir}")
    print(f"Modified since: {since}")
    if args.dry_run:
        print("DRY RUN — no database writes\n")
    else:
        print(f"Database: {db_path}\n")
        db_path.parent.mkdir(parents=True, exist_ok=True)
        init_db(db_path)

    total = skipped = errors = registered = 0

    def process(conn=None):
        nonlocal total, skipped, errors, registered
        for path in iter_order_files(clients_dir, since):
            total += 1
            try:
                record = parse_order_file(path)
            except Exception as e:
                print(f"  ERROR  {path.name}: {e}")
                errors += 1
                continue

            if record is None:
                skipped += 1
                continue

            monthly = record.pop("_monthly")
            cn = record["contract_number"]
            advertiser = record.get("advertiser") or record.get("client") or "?"
            market = record.get("market") or "?"
            months_str = ", ".join(
                f"{m['year']}-{m['month']:02d}=${m['gross']:,.2f}"
                for m in monthly
            )
            print(
                f"  [{cn}] {advertiser} | {market} | "
                f"{len(monthly)} month(s): {months_str or '(none)'}"
            )

            if not args.dry_run and conn is not None:
                upsert_order(conn, record)
                upsert_monthly(conn, cn, monthly)

            registered += 1

    if args.dry_run:
        process()
    else:
        with get_conn(db_path) as conn:
            process(conn)

    print(f"\n--- BACKFILL {'(DRY RUN) ' if args.dry_run else ''}COMPLETE ---")
    print(f"  Files scanned:          {total}")
    print(f"  Registered:             {registered}")
    print(f"  Skipped (not orders):   {skipped}")
    print(f"  Errors:                 {errors}")


if __name__ == "__main__":
    main()
