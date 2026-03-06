"""
worldlink.py - Process Worldlink Etere placement confirmation CSVs into
billing rows using the same A-AC dict format as aggregate.py log rows.

Worldlink orders bypass the MASTER tab and go directly to affidavits,
then to CLEANED.
"""

import csv
from datetime import date, datetime, timedelta
from pathlib import Path

from aggregate import expected_billing_month, MONTH_NAMES

WORLDLINK_DIR = Path(__file__).parent / "Worldlink"

# Etere market name → standard market code (from EtereBridge config.ini)
MARKET_REPLACEMENTS = {
    "NEW YORK": "NYC",
    "Central Valley": "CVC",
    "SAN FRANCISCO": "SFO",
    "CHI MSP": "CMP",
    "HOUSTON": "HOU",
    "LOS ANGELES": "LAX",
    "SEATTLE": "SEA",
    "DALLAS": "DAL",
}

DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# Fixed values for all Worldlink orders
WL_BILLING_TYPE  = "Broadcast"
WL_AGENCY_FLAG   = "Agency"
WL_AGENCY_FEE    = 0.15
WL_SALES_PERSON  = "House"
WL_REVENUE_TYPE  = "Direct Response Sales"
WL_AFFIDAVIT     = "Y"
WL_LANGUAGE      = "E"
WL_PRIORITY      = 4


def parse_filename(path: Path) -> tuple[str, str]:
    """
    Extract contract and estimate from filename.
    Format: '{contract} {description} {estimate}.csv'
    First token = contract, last token = estimate.
    """
    parts = path.stem.split()
    return parts[0], parts[-1]


def parse_time(time_str: str) -> timedelta | None:
    """Parse 'HH:MM' or 'HH:MM:SS' string to timedelta. Returns None if unparseable."""
    if not time_str or not time_str.strip():
        return None
    try:
        parts = time_str.strip().split(":")
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        s = int(parts[2]) if len(parts) > 2 else 0
        return timedelta(hours=h, minutes=m, seconds=s)
    except (ValueError, IndexError):
        return None


def round_to_15_seconds(seconds: float) -> int:
    """Round spot length to nearest 15-second increment."""
    if seconds < 15:
        return int(seconds)
    return round(seconds / 15) * 15


def parse_gross(raw: str) -> float:
    """Parse gross rate string to float, preserving full precision."""
    cleaned = raw.replace("$", "").replace(",", "").strip()
    try:
        return float(cleaned) if cleaned else 0.0
    except ValueError:
        return 0.0


def load_worldlink_csv(path: Path, contract: str, estimate: str) -> tuple[list[dict], list[str]]:
    """
    Parse one Worldlink Etere CSV into A-AC billing row dicts.

    CSV structure:
      Row 0: header block column names (Textbox180, COD_CONTRATTO, ...)
      Row 1: header block data (bill code parts)
      Row 2: blank
      Row 3: spot data column headers
      Row 4+: spot data rows

    Returns (rows, warnings).
    """
    warnings = []
    rows = []

    with open(path, "r", encoding="utf-8-sig") as f:
        all_lines = list(csv.reader(f))

    if len(all_lines) < 5:
        warnings.append(f"{path.name}: too few rows, skipping")
        return rows, warnings

    # --- Bill code from header block ---
    header_data = all_lines[1]
    first_part  = header_data[0].strip() if len(header_data) > 0 else ""
    second_part = header_data[5].strip() if len(header_data) > 5 else ""
    if first_part and second_part:
        bill_code = f"{first_part}:{second_part}"
    else:
        bill_code = first_part or second_part

    # --- Spot data column headers ---
    col_headers = [h.strip() for h in all_lines[3]]
    col_index = {name: i for i, name in enumerate(col_headers)}

    def get(row: list, col_name: str, default: str = "") -> str:
        idx = col_index.get(col_name)
        if idx is None or idx >= len(row):
            return default
        return row[idx].strip()

    # --- Spot data rows ---
    for line_num, row in enumerate(all_lines[4:], start=5):
        if not any(v.strip() for v in row):
            continue

        air_date_str = get(row, "dateschedule")
        if not air_date_str or air_date_str.lower() == "unplaced":
            continue

        # Air date
        try:
            air_date = datetime.strptime(air_date_str, "%m/%d/%Y").date()
        except ValueError:
            warnings.append(f"{path.name} line {line_num}: unparseable date '{air_date_str}'")
            continue

        # Time In / Time Out from timerange2 (e.g. "10:00-23:00")
        timerange = get(row, "timerange2")
        if "-" in timerange:
            time_in_str, time_out_str = timerange.split("-", 1)
        else:
            time_in_str, time_out_str = timerange, ""
        time_in  = parse_time(time_in_str)
        time_out = parse_time(time_out_str)

        # Length (duration3 in seconds → rounded → timedelta)
        try:
            raw_secs = float(get(row, "duration3") or "0")
            length = timedelta(seconds=round_to_15_seconds(raw_secs))
        except ValueError:
            length = timedelta(0)

        # Gross rate — full precision
        gross = parse_gross(get(row, "IMPORTO2", "0"))

        # Market
        raw_market  = get(row, "nome2")
        market_code = MARKET_REPLACEMENTS.get(raw_market, raw_market)
        is_tac      = (market_code == "DAL")

        # Type: COM if gross > 0, BNS if zero
        spot_type = "COM" if gross > 0 else "BNS"

        # Billing month (Worldlink is always Broadcast), standardized to 15th
        b_year, b_month = expected_billing_month(air_date, "Broadcast")
        month_date = date(b_year, b_month, 15)

        # Financials — full precision throughout
        broker_fees = gross * WL_AGENCY_FEE
        station_net = gross - broker_fees

        # Line / spot number
        try:
            line_val = int(float(get(row, "id_contrattirighe") or "0"))
        except ValueError:
            line_val = 0
        try:
            spot_num = int(float(get(row, "Textbox14") or "1"))
        except ValueError:
            spot_num = 1

        record = {
            "A":  bill_code,
            "B":  datetime(air_date.year, air_date.month, air_date.day),
            "C":  datetime(air_date.year, air_date.month, air_date.day),
            "D":  DAY_NAMES[air_date.weekday()],
            "E":  time_in,
            "F":  time_out,
            "G":  length,
            "H":  get(row, "bookingcode2"),       # Media / spot identifier
            "I":  get(row, "airtimep"),            # Actual air time
            "J":  WL_LANGUAGE,
            "K":  None,                            # Format/Show (not in Worldlink CSVs)
            "L":  spot_num,
            "M":  line_val,
            "N":  spot_type,
            "O":  estimate,
            "P":  gross,
            "Q":  market_code,                     # Customer-visible market (Make Good)
            "R":  gross,                           # Spot value = gross for Worldlink
            "S":  month_date,
            "T":  broker_fees,
            "U":  WL_PRIORITY,
            "V":  station_net,
            "W":  WL_SALES_PERSON,
            "X":  WL_REVENUE_TYPE,
            "Y":  WL_BILLING_TYPE,
            "Z":  WL_AGENCY_FLAG,
            "AA": WL_AFFIDAVIT,
            "AB": contract,
            "AC": "DAL" if is_tac else "Admin",   # Accounting market
            "_source_file": path.name,
            "_is_worldlink": True,
        }
        rows.append(record)

    return rows, warnings


def sort_worldlink_rows(rows: list[dict]) -> list[dict]:
    """
    Sort Worldlink rows: contract (AB) -> market (Q, customer-visible) -> date (B) -> air time (I).
    Uses Q for within-affidavit market ordering, not AC (which is Admin or DAL for accounting).
    """
    def sort_key(row):
        ab = str(row.get("AB") or "")
        q  = str(row.get("Q") or "")
        b  = row.get("B")
        b  = b.date() if isinstance(b, datetime) else (b or date.min)
        i  = row.get("I") or ""
        return (ab, q, b, i)

    return sorted(rows, key=sort_key)


def validate_filenames(csv_files: list[Path]) -> list[str]:
    """
    Check for duplicate contract numbers across Worldlink CSV filenames.
    Returns a list of error messages (empty = all clear).
    Contract number = first token of the filename stem.
    """
    seen: dict[str, list[str]] = {}
    for path in csv_files:
        contract, _ = parse_filename(path)
        seen.setdefault(contract, []).append(path.name)

    errors = []
    for contract, files in seen.items():
        if len(files) > 1:
            errors.append(
                f"Duplicate contract number {contract} across files: {', '.join(files)}"
            )
    return errors


def validate_market_isolation(all_rows: list[dict]) -> list[str]:
    """
    Check that DAL never appears in the same contract as any other market.
    Returns a list of error messages (empty = all clear).
    """
    from collections import defaultdict
    contract_markets: dict[str, set] = defaultdict(set)
    for row in all_rows:
        contract_markets[row["AB"]].add(row["Q"])

    errors = []
    for contract, markets in contract_markets.items():
        if "DAL" in markets and len(markets) > 1:
            others = sorted(markets - {"DAL"})
            errors.append(
                f"Contract {contract} has DAL mixed with other markets: {', '.join(others)}"
            )
    return errors


def load_all_worldlink(
    worldlink_dir: Path | None = None,
    billing_year: int | None = None,
    billing_month: int | None = None,
) -> tuple[list[dict], list[str]]:
    """
    Load and process all Worldlink CSV files.
    Validates filenames for duplicate contracts and data for DAL market isolation.
    Halts on validation errors — these must be corrected before proceeding.
    Filters to the specified billing month if provided.
    Returns (rows, warnings).
    """
    if worldlink_dir is None:
        worldlink_dir = WORLDLINK_DIR

    csv_files = sorted(worldlink_dir.glob("*.csv"))
    print(f"\nFound {len(csv_files)} Worldlink CSV files")

    # --- Validation 1: duplicate contract numbers in filenames ---
    filename_errors = validate_filenames(csv_files)
    if filename_errors:
        print(f"\n--- WORLDLINK FILENAME ERRORS (must fix before proceeding) ---")
        for err in filename_errors:
            print(f"  {err}")
        raise SystemExit("Worldlink processing halted: duplicate contract numbers in filenames.")

    all_rows = []
    all_warnings = []

    for path in csv_files:
        contract, estimate = parse_filename(path)
        rows, warnings = load_worldlink_csv(path, contract, estimate)
        all_warnings.extend(warnings)
        print(f"  {path.name}: {len(rows)} rows (contract {contract}, est {estimate})")
        all_rows.extend(rows)

    # --- Validation 2: DAL must not appear alongside other markets in same contract ---
    market_errors = validate_market_isolation(all_rows)
    if market_errors:
        print(f"\n--- WORLDLINK MARKET ISOLATION ERRORS (must fix before proceeding) ---")
        for err in market_errors:
            print(f"  {err}")
        raise SystemExit("Worldlink processing halted: DAL market isolation violation.")

    if billing_year and billing_month:
        before = len(all_rows)
        all_rows = [
            r for r in all_rows
            if isinstance(r.get("S"), date)
            and r["S"].year == billing_year
            and r["S"].month == billing_month
        ]
        filtered = before - len(all_rows)
        if filtered:
            print(f"  ({filtered} rows outside {MONTH_NAMES[billing_month]} {billing_year} filtered out)")

    all_rows = sort_worldlink_rows(all_rows)
    return all_rows, all_warnings
