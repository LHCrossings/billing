"""
manage_db.py - View and update agency/advertiser flags in the billing database.

EDI is an agency-level setting (the agency has a vendor portal).
Notarization is an advertiser-level setting (the client requires a notarized affidavit,
which also drives which affidavit template is used).

Commands:
    show         <contract> [--field NAME]  Show all fields for an order (or one field)
    monthly      <contract>                 Show monthly gross breakdown for an order
    search       <text>                     Search by client, advertiser, or estimate
    list-agencies                           List all agencies with EDI flags
    list-advertisers                        List all advertisers with notarized flags
    set-agency   <agency>   [options]       Set EDI flags for an agency
    set-advertiser <advertiser> [options]   Set notarized flag for an advertiser
    show-month   <YYYY-MM>                  Show all orders active in a billing month
    remove-order <contract_number>          Remove an order and its monthly data from the DB

set-agency options:
    --edi / --no-edi        Invoice delivered via EDI upload
    --edi-notes TEXT        Notes about EDI system or process (portal, login, etc.)

set-advertiser options:
    --notarized / --no-notarized    Affidavit requires notarization (different template)

Examples:
    uv run python manage_db.py show 2557
    uv run python manage_db.py show 2557 --field client
    uv run python manage_db.py monthly 2557
    uv run python manage_db.py search "Muckleshoot"
    uv run python manage_db.py list-agencies
    uv run python manage_db.py list-advertisers
    uv run python manage_db.py set-agency "Admerasia Inc." --edi --edi-notes "Mediaocean"
    uv run python manage_db.py set-advertiser "Muckleshoot Casino" --notarized
    uv run python manage_db.py show-month 2026-02
    uv run python manage_db.py remove-order 2400
    uv run python manage_db.py register "M:\\Clients\\Agency\\Client\\order.xlsx"
    uv run python manage_db.py affidavit list 2026-02
    uv run python manage_db.py affidavit show 2602-047
    uv run python manage_db.py affidavit status 2602-047 reviewed
    uv run python manage_db.py affidavit next 2026-02
    uv run python manage_db.py affidavit next 2026-02 --pre-bill
"""

import argparse
import json
import sys
from pathlib import Path

from orders_db import (
    DB_PATH,
    get_advertiser_flags,
    get_affidavit,
    get_affidavit_lines,
    get_affidavits_for_month,
    get_agency_flags,
    get_all_agency_flags,
    get_conn,
    get_order,
    init_db,
    next_affidavit_number,
    set_advertiser_flags,
    set_agency_flags,
    upsert_affidavit,
    upsert_monthly,
    upsert_order,
)


ORDER_FIELDS = [
    "contract_number", "client", "advertiser", "contact",
    "address", "city", "state", "zip", "phone", "fax",
    "emails", "billing_type", "market", "estimate", "notes",
    "agency_discount", "station_rep", "revision", "date_order_written",
    "total_gross", "total_net", "last_updated", "file_path",
]


def cmd_show(conn, args):
    row = conn.execute(
        "SELECT * FROM orders WHERE contract_number = ?", (args.contract,)
    ).fetchone()

    if row is None:
        print(f"No order found for contract {args.contract}.")
        sys.exit(1)

    if args.field:
        field = args.field.lower()
        if field not in ORDER_FIELDS:
            print(f"Unknown field '{field}'. Available: {', '.join(ORDER_FIELDS)}")
            sys.exit(1)
        val = row[field]
        if field == "emails":
            try:
                val = ", ".join(json.loads(val or "[]"))
            except Exception:
                pass
        print(val if val is not None else "(empty)")
    else:
        print(f"\n{'─' * 50}")
        print(f"  Contract {row['contract_number']}  —  {row['advertiser'] or '?'}  /  {row['client'] or '?'}")
        print(f"{'─' * 50}")
        for field in ORDER_FIELDS:
            if field in ("contract_number", "file_path"):
                continue
            val = row[field]
            if field == "emails":
                try:
                    val = ", ".join(json.loads(val or "[]"))
                except Exception:
                    pass
            if val is not None and val != "":
                print(f"  {field.replace('_', ' ').title():<22} {val}")
        print(f"\n  File: {row['file_path']}")
        print()


def cmd_monthly(conn, args):
    order = conn.execute(
        "SELECT advertiser, client FROM orders WHERE contract_number = ?", (args.contract,)
    ).fetchone()
    if order is None:
        print(f"No order found for contract {args.contract}.")
        sys.exit(1)

    rows = conn.execute("""
        SELECT year, month, market, gross, net
        FROM order_monthly
        WHERE contract_number = ?
        ORDER BY year, month, market
    """, (args.contract,)).fetchall()

    print(f"\n  Monthly breakdown — contract {args.contract}  ({order['advertiser'] or '?'} / {order['client'] or '?'})\n")
    print(f"  {'Month':<12} {'Market':<8} {'Gross':>12}  {'Net':>12}")
    print(f"  {'─' * 50}")
    for r in rows:
        print(f"  {r['year']}-{r['month']:02d}  {r['market']:<8} ${r['gross']:>11,.2f}  ${r['net']:>11,.2f}")
    if not rows:
        print("  (no monthly data)")
    print()


def cmd_search(conn, args):
    term = f"%{args.text}%"
    rows = conn.execute("""
        SELECT contract_number, advertiser, client, market, estimate
        FROM orders
        WHERE client LIKE ? OR advertiser LIKE ? OR estimate LIKE ?
        ORDER BY client, advertiser, contract_number
    """, (term, term, term)).fetchall()

    if not rows:
        print(f"No orders matching '{args.text}'.")
        return

    print(f"\n  {'Contract':<12} {'Advertiser':<28} {'Client':<28} {'Market':<8} Estimate")
    print(f"  {'─' * 90}")
    for r in rows:
        print(f"  {r['contract_number']:<12} {(r['advertiser'] or ''):<28} {(r['client'] or ''):<28} {(r['market'] or ''):<8} {r['estimate'] or ''}")
    print()


def cmd_register(conn, args):
    from order_parser import parse_order_file
    path = Path(args.filepath)
    if not path.exists():
        print(f"File not found: {path}")
        sys.exit(1)

    record = parse_order_file(path)
    if record is None:
        print("Not a valid order file (missing required sheets or contract number).")
        sys.exit(1)

    monthly = record.pop("_monthly")
    cn = record["contract_number"]
    upsert_order(conn, record)
    upsert_monthly(conn, cn, monthly)
    print(f"Registered contract {cn}  —  {record.get('advertiser') or '?'} / {record.get('client') or '?'}")
    print(f"  Market: {record.get('market') or '?'}   Estimate: {record.get('estimate') or '?'}")
    print(f"  Monthly rows: {len(monthly)}")


def cmd_affidavit(conn, args):
    sub = args.affidavit_command

    if sub == "list":
        try:
            year, month = map(int, args.month.split("-"))
        except ValueError:
            print(f"Invalid month '{args.month}'. Use YYYY-MM.")
            sys.exit(1)

        rows = get_affidavits_for_month(conn, year, month)
        if not rows:
            print(f"No affidavits for {year}-{month:02d}.")
            return

        print(f"\n  {'Number':<12} {'Status':<10} {'Advertiser':<28} {'Market':<8} {'Spots':>6}  {'Gross':>12}  {'Pre-bill'}")
        print(f"  {'─' * 90}")
        for r in rows:
            advertiser = r["advertiser"] or r["bill_code"] or "?"
            pb = "YES" if r["pre_bill"] else "-"
            spots = str(r["spot_count"]) if r["spot_count"] is not None else "?"
            gross = f"${r['gross_total']:>11,.2f}" if r["gross_total"] is not None else "?"
            print(f"  {r['affidavit_number']:<12} {r['status']:<10} {advertiser:<28} {r['market']:<8} {spots:>6}  {gross:>12}  {pb}")
        print()

    elif sub == "show":
        aff = get_affidavit(conn, args.number)
        if aff is None:
            print(f"No affidavit found: {args.number}")
            sys.exit(1)

        print(f"\n{'─' * 55}")
        print(f"  Affidavit {aff['affidavit_number']}  —  {aff['status'].upper()}")
        print(f"{'─' * 55}")
        if aff["contract_number"]:
            order = get_order(conn, aff["contract_number"])
            if order:
                print(f"  Advertiser:  {order['advertiser'] or '?'}")
                print(f"  Agency:      {order['client'] or '?'}")
        print(f"  Bill code:   {aff['bill_code'] or '?'}")
        print(f"  Market:      {aff['market']}")
        print(f"  Month:       {aff['year']}-{aff['month']:02d}")
        print(f"  Pre-bill:    {'YES' if aff['pre_bill'] else 'no'}")
        if aff["gross_total"] is not None:
            print(f"  Gross total: ${aff['gross_total']:,.2f}")
        if aff["spot_count"] is not None:
            print(f"  Spot count:  {aff['spot_count']}")

        lines = get_affidavit_lines(conn, args.number)
        if lines:
            print(f"\n  {'Date':<12} {'Time':<10} {'Len':<6} {'Type':<5} {'Gross':>10}  Program")
            print(f"  {'─' * 65}")
            for ln in lines:
                print(
                    f"  {str(ln['air_date'] or ''):<12} {str(ln['air_time'] or ''):<10} "
                    f"{str(ln['length'] or ''):<6} {str(ln['line_type'] or ''):<5} "
                    f"${(ln['gross'] or 0):>9,.2f}  {ln['program'] or ''}"
                )
        else:
            print("\n  (no spot lines)")
        print()

    elif sub == "status":
        valid = ("draft", "reviewed", "numbered")
        if args.status not in valid:
            print(f"Invalid status '{args.status}'. Choose: {', '.join(valid)}")
            sys.exit(1)
        aff = get_affidavit(conn, args.number)
        if aff is None:
            print(f"No affidavit found: {args.number}")
            sys.exit(1)
        from datetime import datetime
        conn.execute(
            "UPDATE affidavits SET status = ?, updated_at = ? WHERE affidavit_number = ?",
            (args.status, datetime.now().isoformat(), args.number)
        )
        print(f"  {args.number} → {args.status}")

    elif sub == "next":
        try:
            year, month = map(int, args.month.split("-"))
        except ValueError:
            print(f"Invalid month '{args.month}'. Use YYYY-MM.")
            sys.exit(1)
        pre_bill = getattr(args, "pre_bill", False)
        num = next_affidavit_number(conn, year, month, pre_bill=pre_bill)
        print(num)


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

    p_show = sub.add_parser("show", help="Show order details")
    p_show.add_argument("contract", type=int)
    p_show.add_argument("--field", default=None, help="Show a single field only")

    p_monthly = sub.add_parser("monthly", help="Show monthly gross breakdown")
    p_monthly.add_argument("contract", type=int)

    p_search = sub.add_parser("search", help="Search by client, advertiser, or estimate")
    p_search.add_argument("text")

    p_register = sub.add_parser("register", help="Parse and register a single order file immediately")
    p_register.add_argument("filepath", help="Path to the order .xlsx file")

    p_aff = sub.add_parser("affidavit", help="Manage affidavits")
    aff_sub = p_aff.add_subparsers(dest="affidavit_command", required=True)

    p_aff_list = aff_sub.add_parser("list", help="List affidavits for a month")
    p_aff_list.add_argument("month", metavar="YYYY-MM")

    p_aff_show = aff_sub.add_parser("show", help="Show affidavit header and spot lines")
    p_aff_show.add_argument("number", metavar="YYMM-XXX")

    p_aff_status = aff_sub.add_parser("status", help="Update affidavit status")
    p_aff_status.add_argument("number", metavar="YYMM-XXX")
    p_aff_status.add_argument("status", choices=["draft", "reviewed", "numbered"])

    p_aff_next = aff_sub.add_parser("next", help="Show next available affidavit number")
    p_aff_next.add_argument("month", metavar="YYYY-MM")
    p_aff_next.add_argument("--pre-bill", action="store_true", default=False)

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
        if args.command == "show":
            cmd_show(conn, args)
        elif args.command == "monthly":
            cmd_monthly(conn, args)
        elif args.command == "search":
            cmd_search(conn, args)
        elif args.command == "register":
            cmd_register(conn, args)
        elif args.command == "affidavit":
            cmd_affidavit(conn, args)
        elif args.command == "list-agencies":
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
