"""
manage_db.py - View and update client flags in the billing database.

Commands:
    list                          List all clients with their flags
    list-clients                  List all known clients from the orders table
    set <client> [options]        Set flags for a client
    show-month <YYYY-MM>          Show all orders active in a billing month with flags

set options:
    --notarized / --no-notarized  Affidavit requires notarization (different template)
    --edi / --no-edi              Invoice delivered via EDI upload
    --edi-notes TEXT              Notes about EDI system or process

Examples:
    uv run python manage_db.py list
    uv run python manage_db.py list-clients
    uv run python manage_db.py set "Rodi Platcow Malin" --notarized
    uv run python manage_db.py set "Admerasia Inc." --edi --edi-notes "Mediaocean"
    uv run python manage_db.py set "H&L Partners" --no-edi
    uv run python manage_db.py show-month 2026-02
"""

import argparse
import sys
from pathlib import Path

from orders_db import (
    DB_PATH,
    get_all_client_flags,
    get_client_flags,
    get_conn,
    init_db,
    set_client_flags,
)

# Ensure schema is current whenever this script runs
def _ensure_db(db_path: Path):
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        print("Run backfill.py first to create and populate the database.")
        sys.exit(1)
    init_db(db_path)  # creates any missing tables (safe to run repeatedly)


def cmd_list(conn, args):
    rows = get_all_client_flags(conn)
    if not rows:
        print("No client flags set yet. Use 'set' to add entries.")
        return

    print(f"{'CLIENT':<40} {'NOTARIZED':<12} {'EDI':<6} EDI NOTES")
    print("-" * 80)
    for row in rows:
        notarized = "YES" if row["notarized"] else "-"
        edi = "YES" if row["edi"] else "-"
        notes = row["edi_notes"] or ""
        print(f"{row['client']:<40} {notarized:<12} {edi:<6} {notes}")


def cmd_list_clients(conn, args):
    """Show all distinct clients from the orders table, with their current flag status."""
    rows = conn.execute("""
        SELECT DISTINCT o.client,
               cf.notarized, cf.edi, cf.edi_notes
        FROM orders o
        LEFT JOIN client_flags cf ON cf.client = o.client
        ORDER BY o.client
    """).fetchall()

    if not rows:
        print("No orders in database yet.")
        return

    print(f"{'CLIENT':<40} {'NOTARIZED':<12} {'EDI':<6} EDI NOTES")
    print("-" * 80)
    for row in rows:
        notarized = "YES" if row["notarized"] else "-"
        edi = "YES" if row["edi"] else "-"
        notes = row["edi_notes"] or ""
        flag_note = " (no flags set)" if row["notarized"] is None else ""
        print(f"{row['client']:<40} {notarized:<12} {edi:<6} {notes}{flag_note}")


def cmd_set(conn, args):
    client = args.client

    # Resolve --notarized / --no-notarized
    notarized = None
    if args.notarized:
        notarized = True
    elif args.no_notarized:
        notarized = False

    # Resolve --edi / --no-edi
    edi = None
    if args.edi:
        edi = True
    elif args.no_edi:
        edi = False

    edi_notes = args.edi_notes  # None if not provided

    if notarized is None and edi is None and edi_notes is None:
        print("Nothing to update. Provide at least one flag option.")
        print("  --notarized / --no-notarized")
        print("  --edi / --no-edi")
        print("  --edi-notes TEXT")
        sys.exit(1)

    set_client_flags(conn, client, notarized=notarized, edi=edi, edi_notes=edi_notes)

    row = get_client_flags(conn, client)
    print(f"Updated: {client}")
    print(f"  Notarized: {'YES' if row['notarized'] else 'no'}")
    print(f"  EDI:       {'YES' if row['edi'] else 'no'}")
    if row["edi_notes"]:
        print(f"  EDI notes: {row['edi_notes']}")


def cmd_show_month(conn, args):
    """Show all orders with activity in a given billing month, with their flags."""
    try:
        year, month = map(int, args.month.split("-"))
    except ValueError:
        print(f"Invalid month format '{args.month}'. Use YYYY-MM.")
        sys.exit(1)

    rows = conn.execute("""
        SELECT
            o.contract_number,
            o.advertiser,
            o.client,
            o.market,
            om.market AS billed_market,
            om.gross,
            om.net,
            cf.notarized,
            cf.edi,
            cf.edi_notes
        FROM order_monthly om
        JOIN orders o ON o.contract_number = om.contract_number
        LEFT JOIN client_flags cf ON cf.client = o.client
        WHERE om.year = ? AND om.month = ?
        ORDER BY o.client, o.contract_number, om.market
    """, (year, month)).fetchall()

    if not rows:
        print(f"No orders found for {year}-{month:02d}.")
        return

    from aggregate import MONTH_NAMES
    print(f"Orders active in {MONTH_NAMES[month]} {year}:\n")
    print(f"  {'CONTRACT':<10} {'ADVERTISER':<30} {'MARKET':<8} {'GROSS':>10}  {'NOTARIZED':<10} {'EDI':<6} EDI NOTES")
    print("  " + "-" * 90)

    notarized_list = []
    edi_list = []

    for row in rows:
        notarized = "YES" if row["notarized"] else "-"
        edi = "YES" if row["edi"] else "-"
        notes = row["edi_notes"] or ""
        print(
            f"  {row['contract_number']:<10} {(row['advertiser'] or row['client'] or '?'):<30} "
            f"{row['billed_market']:<8} ${row['gross']:>9,.2f}  {notarized:<10} {edi:<6} {notes}"
        )
        if row["notarized"]:
            advertiser = row["advertiser"] or row["client"] or "?"
            if advertiser not in notarized_list:
                notarized_list.append(advertiser)
        if row["edi"]:
            advertiser = row["advertiser"] or row["client"] or "?"
            if advertiser not in edi_list:
                edi_list.append(advertiser)

    if notarized_list:
        print(f"\n  NEEDS NOTARIZATION: {', '.join(sorted(notarized_list))}")
    if edi_list:
        print(f"  EDI UPLOAD:         {', '.join(sorted(edi_list))}")


def main():
    parser = argparse.ArgumentParser(
        description="Manage client flags in the billing database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db", default=str(DB_PATH), help="Path to billing database")

    sub = parser.add_subparsers(dest="command")
    sub.required = True

    sub.add_parser("list", help="List all clients with flags set")
    sub.add_parser("list-clients", help="List all clients from orders table with flag status")

    p_set = sub.add_parser("set", help="Set flags for a client")
    p_set.add_argument("client", help="Client (agency) name, exactly as it appears in orders")
    p_set.add_argument("--notarized", action="store_true", default=False)
    p_set.add_argument("--no-notarized", action="store_true", default=False)
    p_set.add_argument("--edi", action="store_true", default=False)
    p_set.add_argument("--no-edi", action="store_true", default=False)
    p_set.add_argument("--edi-notes", default=None, metavar="TEXT",
                       help="Notes about the EDI system or process")

    p_month = sub.add_parser("show-month", help="Show orders active in a billing month with flags")
    p_month.add_argument("month", metavar="YYYY-MM")

    args = parser.parse_args()
    db_path = Path(args.db)

    _ensure_db(db_path)

    with get_conn(db_path) as conn:
        if args.command == "list":
            cmd_list(conn, args)
        elif args.command == "list-clients":
            cmd_list_clients(conn, args)
        elif args.command == "set":
            cmd_set(conn, args)
        elif args.command == "show-month":
            cmd_show_month(conn, args)


if __name__ == "__main__":
    main()
