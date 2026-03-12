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
from orders_db import DB_PATH, get_conn, get_order, init_db, upsert_monthly, upsert_order

CLIENTS_DIR = Path(r"M:\Clients")
SINCE_DATE = date(2024, 1, 1)

# Directories to skip entirely during the walk (case-insensitive match on directory name).
# - !!Archived Clients: old clients, not relevant for current billing
# - !Sample Orders: templates, not real orders
# Note: Worldlink is NOT skipped — Sales Confirmations are parsed for metadata (contract
# number, estimate, client, market, etc.) but Run Sheets are ignored since their line data
# is stale. Monthly revenue for Worldlink comes from Etere CSVs via worldlink.py.
SKIP_DIRS = {"!!archived clients (more than 3 years old)", "!sample orders"}

# Directories whose order files should be parsed for metadata only (Run Sheet skipped).
METADATA_ONLY_DIRS = {"worldlink"}


def iter_order_files(clients_dir: Path, since: date) -> Iterator[tuple[Path, bool]]:
    """Walk clients_dir, yield (path, metadata_only) for .xlsx files modified on or after since."""
    for root, dirs, files in os.walk(clients_dir):
        dirs[:] = sorted(
            d for d in dirs
            if not d.startswith(".") and d.lower() not in SKIP_DIRS
        )
        # metadata_only if any ancestor directory matches METADATA_ONLY_DIRS
        root_parts = {p.lower() for p in Path(root).parts}
        metadata_only = bool(root_parts & METADATA_ONLY_DIRS)

        for fname in sorted(files):
            if not fname.lower().endswith(".xlsx"):
                continue
            path = Path(root) / fname
            try:
                mtime = datetime.fromtimestamp(path.stat().st_mtime).date()
            except OSError:
                continue
            if mtime >= since:
                yield path, metadata_only


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

    total = skipped = errors = registered = outdated = 0

    def process(conn=None):
        nonlocal total, skipped, errors, registered, outdated
        for path, metadata_only in iter_order_files(clients_dir, since):
            total += 1
            try:
                record = parse_order_file(path, metadata_only=metadata_only)
            except Exception as e:
                print(f"  ERROR  {path.name}: {e}")
                errors += 1
                continue

            if record is None:
                skipped += 1
                continue

            # parse_order_file returns a list (one entry per contract)
            for rec in record:
                monthly = rec.pop("_monthly")
                cn = rec["contract_number"]
                new_rev = rec.get("revision") or 0

                # Skip if a higher revision is already in the DB
                if not args.dry_run and conn is not None:
                    existing = get_order(conn, cn)
                    if existing is not None:
                        existing_rev = existing["revision"] or 0
                        if existing_rev > new_rev:
                            outdated += 1
                            print(
                                f"  [SKIP rev{new_rev}<rev{existing_rev}] "
                                f"{path.name}"
                            )
                            continue

                # Group monthly rows by contract_number — single-contract run sheets may
                # contain rows for other contracts (shared run sheet scenario).
                from collections import defaultdict
                monthly_by_cn: dict[int, list] = defaultdict(list)
                for m in monthly:
                    monthly_by_cn[m["contract_number"]].append(m)

                extra_contracts = sorted(c for c in monthly_by_cn if c != cn)

                advertiser = rec.get("advertiser") or rec.get("client") or "?"
                market = rec.get("market") or "?"
                months_str = ", ".join(
                    f"{m['year']}-{m['month']:02d}=${m['gross']:,.2f}"
                    for m in monthly_by_cn.get(cn, [])
                )
                print(
                    f"  [{cn}] {advertiser} | {market} | "
                    f"{len(monthly_by_cn.get(cn, []))} month(s): {months_str or '(none)'}"
                )
                for extra_cn in extra_contracts:
                    extra_rows = monthly_by_cn[extra_cn]
                    extra_str = ", ".join(
                        f"{m['year']}-{m['month']:02d}=${m['gross']:,.2f}" for m in extra_rows
                    )
                    print(f"    + [{extra_cn}] {len(extra_rows)} month(s): {extra_str}")

                if not args.dry_run and conn is not None:
                    upsert_order(conn, rec)
                    for group_cn, group_rows in monthly_by_cn.items():
                        if group_cn != cn:
                            exists = conn.execute(
                                "SELECT 1 FROM orders WHERE contract_number = ?", (group_cn,)
                            ).fetchone()
                            if not exists:
                                print(f"    ! [{group_cn}] not in orders table yet — monthly data skipped")
                                continue
                        upsert_monthly(conn, group_cn, group_rows)

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
    print(f"  Skipped (older rev):    {outdated}")
    print(f"  Errors:                 {errors}")


if __name__ == "__main__":
    main()
