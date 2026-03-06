"""
manage_db.py - View and update agency/advertiser flags in the billing database.

EDI is an agency-level setting (the agency has a vendor portal).
Notarization is an advertiser-level setting (the client requires a notarized affidavit,
which also drives which affidavit template is used).

Commands:
    list-agencies                         List all agencies with EDI flags
    list-advertisers                      List all advertisers with notarized flags
    set-agency   <agency>   [options]     Set EDI flags for an agency
    set-advertiser <advertiser> [options] Set notarized flag for an advertiser
    show-month   <YYYY-MM>                Show all orders active in a billing month
    remove-order <contract_number>        Remove an order and its monthly data from the DB

set-agency options:
    --edi / --no-edi        Invoice delivered via EDI upload
    --edi-notes TEXT        Notes about EDI system or process (portal, login, etc.)

set-advertiser options:
    --notarized / --no-notarized    Affidavit requires notarization (different template)

Examples:
    uv run python manage_db.py list-agencies
    uv run python manage_db.py list-advertisers
    uv run python manage_db.py set-agency "Admerasia Inc." --edi --edi-notes "Mediaocean"
    uv run python manage_db.py set-advertiser "Muckleshoot Casino" --notarized
    uv run python manage_db.py show-month 2026-02
    uv run python manage_db.py remove-order 2400
"""

import argparse
import sys
from pathlib import Path

from orders_db import (
    DB_PATH,
    get_advertiser_flags,
    get_agency_flags,
    get_all_agency_flags,
    get_conn,
    get_order,
    init_db,
    set_advertiser_flags,
    set_agency_flags,
)


def _ensure_db(db_path: Path):
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        print("Run backfill.py first to create and populate the database.")
        sys.exit(1)
    init_db(db_path)


def cmd_list_agencies(conn, args):
    """List all agencies from orders table with EDI flag status."""
    rows = conn.execute("""
        SELECT DISTINCT o.client AS agency,
               af.edi, af.edi_notes
        FROM orders o
        LEFT JOIN agency_flags af ON af.agency = o.client
        WHERE o.client IS NOT NULL AND o.client != ''
        ORDER BY o.client
    """).fetchall()

    if not rows:
        print("No orders in database.")
        return

    print(f"{'AGENCY':<45} {'NOTARIZED':<11} {'EDI':<6} EDI NOTES")
    print("-" * 85)
    for row in rows:
        notarized = "YES" if row["notarized"] else "-"
        edi = "YES" if row["edi"] else "-"
        notes = row["edi_notes"] or ""
        flag_note = " (not set)" if row["edi"] is None and row["notarized"] is None else ""
        print(f"{row['agency']:<45} {notarized:<11} {edi:<6} {notes}{flag_note}")


def cmd_list_advertisers(conn, args):
    """List all advertisers from orders table with notarized flag status."""
    rows = conn.execute("""
        SELECT DISTINCT o.advertiser,
               af.notarized
        FROM orders o
        LEFT JOIN advertiser_flags af ON af.advertiser = o.advertiser
        WHERE o.advertiser IS NOT NULL AND o.advertiser != ''
        ORDER BY o.advertiser
    """).fetchall()

    if not rows:
        print("No orders in database.")
        return

    print(f"{'ADVERTISER':<45} NOTARIZED")
    print("-" * 60)
    for row in rows:
        notarized = "YES" if row["notarized"] else "-"
        flag_note = " (not set)" if row["notarized"] is None else ""
        print(f"{row['advertiser']:<45} {notarized}{flag_note}")


def cmd_set_agency(conn, args):
    agency = args.agency
    notarized = True if args.notarized else (False if args.no_notarized else None)
    edi = True if args.edi else (False if args.no_edi else None)
    edi_notes = args.edi_notes

    if notarized is None and edi is None and edi_notes is None:
        print("Nothing to update. Provide --notarized, --edi, --no-edi, or --edi-notes TEXT.")
        sys.exit(1)

    set_agency_flags(conn, agency, notarized=notarized, edi=edi, edi_notes=edi_notes)
    row = get_agency_flags(conn, agency)
    print(f"Updated agency: {agency}")
    print(f"  Notarized: {'YES' if row['notarized'] else 'no'}")
    print(f"  EDI:       {'YES' if row['edi'] else 'no'}")
    if row["edi_notes"]:
        print(f"  EDI notes: {row['edi_notes']}")


def cmd_set_advertiser(conn, args):
    advertiser = args.advertiser
    notarized = True if args.notarized else (False if args.no_notarized else None)

    if notarized is None:
        print("Nothing to update. Provide --notarized or --no-notarized.")
        sys.exit(1)

    set_advertiser_flags(conn, advertiser, notarized=notarized)
    row = get_advertiser_flags(conn, advertiser)
    print(f"Updated advertiser: {advertiser}")
    print(f"  Notarized: {'YES' if row['notarized'] else 'no'}")


def cmd_show_month(conn, args):
    try:
        year, month = map(int, args.month.split("-"))
    except ValueError:
        print(f"Invalid month format '{args.month}'. Use YYYY-MM.")
        sys.exit(1)

    rows = conn.execute("""
        SELECT
            o.contract_number,
            o.advertiser,
            o.client       AS agency,
            om.market,
            om.gross,
            om.net,
            MAX(COALESCE(af_adv.notarized, 0),
                COALESCE(af_ag.notarized,  0)) AS notarized,
            af_ag.edi,
            af_ag.edi_notes
        FROM order_monthly om
        JOIN orders o ON o.contract_number = om.contract_number
        LEFT JOIN advertiser_flags af_adv ON af_adv.advertiser = o.advertiser
        LEFT JOIN agency_flags     af_ag  ON af_ag.agency      = o.client
        WHERE om.year = ? AND om.month = ?
        ORDER BY o.client, o.advertiser, om.market
    """, (year, month)).fetchall()

    if not rows:
        print(f"No orders found for {year}-{month:02d}.")
        return

    from aggregate import MONTH_NAMES
    print(f"Orders active in {MONTH_NAMES[month]} {year}:\n")
    print(f"  {'CONTRACT':<10} {'ADVERTISER':<28} {'AGENCY':<28} {'MKT':<6} {'GROSS':>10}  {'NOTARIZED':<10} {'EDI'}")
    print("  " + "-" * 100)

    notarized_list = []
    edi_list = []

    for row in rows:
        notarized = "YES" if row["notarized"] else "-"
        edi = "YES" if row["edi"] else "-"
        print(
            f"  {row['contract_number']:<10} {(row['advertiser'] or '?'):<28} "
            f"{(row['agency'] or '?'):<28} {row['market']:<6} "
            f"${row['gross']:>9,.2f}  {notarized:<10} {edi}"
        )
        if row["notarized"] and row["advertiser"] not in notarized_list:
            notarized_list.append(row["advertiser"])
        if row["edi"] and row["agency"] not in edi_list:
            edi_list.append(row["agency"])

    if notarized_list:
        print(f"\n  NEEDS NOTARIZATION: {', '.join(sorted(notarized_list))}")
    if edi_list:
        print(f"  EDI UPLOAD:         {', '.join(sorted(edi_list))}")


def cmd_remove_order(conn, args):
    try:
        cn = int(args.contract_number)
    except ValueError:
        print(f"Invalid contract number '{args.contract_number}'. Must be an integer.")
        sys.exit(1)

    row = get_order(conn, cn)
    if row is None:
        print(f"No order found with contract number {cn}.")
        sys.exit(1)

    monthly = conn.execute(
        "SELECT year, month, market, gross FROM order_monthly WHERE contract_number = ? ORDER BY year, month, market",
        (cn,)
    ).fetchall()

    print(f"Contract:   {cn}")
    print(f"Advertiser: {row['advertiser'] or '?'}")
    print(f"Agency:     {row['client'] or '?'}")
    print(f"Market:     {row['market'] or '?'}")
    print(f"File:       {row['file_path']}")
    if monthly:
        print(f"Monthly rows ({len(monthly)}):")
        for m in monthly:
            print(f"  {m['year']}-{m['month']:02d} {m['market']:<8} ${m['gross']:,.2f}")
    else:
        print("Monthly rows: (none)")

    confirm = input("\nDelete this order and all its monthly data? [y/N] ").strip().lower()
    if confirm != "y":
        print("Cancelled.")
        return

    conn.execute("DELETE FROM orders WHERE contract_number = ?", (cn,))
    # order_monthly deleted via ON DELETE CASCADE
    print(f"Removed contract {cn}.")


def main():
    parser = argparse.ArgumentParser(
        description="Manage agency/advertiser flags in the billing database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db", default=str(DB_PATH), help="Path to billing database")
    sub = parser.add_subparsers(dest="command")
    sub.required = True

    sub.add_parser("list-agencies",    help="List all agencies with EDI flag status")
    sub.add_parser("list-advertisers", help="List all advertisers with notarized flag status")

    p_ag = sub.add_parser("set-agency", help="Set flags for an agency")
    p_ag.add_argument("agency")
    p_ag.add_argument("--notarized",    action="store_true", default=False)
    p_ag.add_argument("--no-notarized", action="store_true", default=False)
    p_ag.add_argument("--edi",          action="store_true", default=False)
    p_ag.add_argument("--no-edi",       action="store_true", default=False)
    p_ag.add_argument("--edi-notes",    default=None, metavar="TEXT")

    p_adv = sub.add_parser("set-advertiser", help="Set notarized flag for an advertiser")
    p_adv.add_argument("advertiser")
    p_adv.add_argument("--notarized",    action="store_true", default=False)
    p_adv.add_argument("--no-notarized", action="store_true", default=False)

    p_month = sub.add_parser("show-month", help="Show orders active in a billing month")
    p_month.add_argument("month", metavar="YYYY-MM")

    p_remove = sub.add_parser("remove-order", help="Remove an order and its monthly data from the DB")
    p_remove.add_argument("contract_number", metavar="CONTRACT_NUMBER")

    args = parser.parse_args()
    db_path = Path(args.db)
    _ensure_db(db_path)

    with get_conn(db_path) as conn:
        if args.command == "list-agencies":
            cmd_list_agencies(conn, args)
        elif args.command == "list-advertisers":
            cmd_list_advertisers(conn, args)
        elif args.command == "set-agency":
            cmd_set_agency(conn, args)
        elif args.command == "set-advertiser":
            cmd_set_advertiser(conn, args)
        elif args.command == "show-month":
            cmd_show_month(conn, args)
        elif args.command == "remove-order":
            cmd_remove_order(conn, args)


if __name__ == "__main__":
    main()
