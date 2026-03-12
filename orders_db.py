"""
orders_db.py - SQLite schema and CRUD for order metadata and monthly gross breakouts.
"""

import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path(r"M:\Accounting\Billing\billing.db")


@contextmanager
def get_conn(db_path: Path = DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: Path = DB_PATH):
    """Create tables if they don't exist. Migrates legacy client_flags if present."""
    with get_conn(db_path) as conn:
        # Migrate legacy client_flags → agency_flags + advertiser_flags
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        # Add notarized column to agency_flags if missing (added after initial schema)
        if "agency_flags" in tables:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(agency_flags)").fetchall()}
            if "notarized" not in cols:
                conn.execute(
                    "ALTER TABLE agency_flags ADD COLUMN notarized INTEGER NOT NULL DEFAULT 0"
                )

        if "client_flags" in tables and "agency_flags" not in tables:
            conn.executescript("""
                CREATE TABLE agency_flags (
                    agency      TEXT PRIMARY KEY,
                    edi         INTEGER NOT NULL DEFAULT 0,
                    edi_notes   TEXT
                );
                CREATE TABLE advertiser_flags (
                    advertiser  TEXT PRIMARY KEY,
                    notarized   INTEGER NOT NULL DEFAULT 0
                );
                INSERT INTO advertiser_flags (advertiser, notarized)
                    SELECT client, notarized FROM client_flags WHERE notarized = 1;
                INSERT INTO agency_flags (agency, edi, edi_notes)
                    SELECT client, edi, edi_notes FROM client_flags WHERE edi = 1;
                DROP TABLE client_flags;
            """)

        conn.executescript("""
            CREATE TABLE IF NOT EXISTS orders (
                contract_number    INTEGER PRIMARY KEY,
                file_path          TEXT    NOT NULL,
                client             TEXT,
                advertiser         TEXT,
                contact            TEXT,
                address            TEXT,
                city               TEXT,
                state              TEXT,
                zip                TEXT,
                phone              TEXT,
                fax                TEXT,
                billing_type       TEXT,
                market             TEXT,
                estimate           TEXT,
                notes              TEXT,
                agency_discount    REAL,
                date_order_written TEXT,
                revision           INTEGER,
                station_rep        TEXT,
                emails             TEXT,
                total_gross        REAL,
                total_net          REAL,
                last_updated       TEXT
            );

            CREATE TABLE IF NOT EXISTS agency_flags (
                agency      TEXT PRIMARY KEY,
                notarized   INTEGER NOT NULL DEFAULT 0,
                edi         INTEGER NOT NULL DEFAULT 0,
                edi_notes   TEXT
            );

            CREATE TABLE IF NOT EXISTS advertiser_flags (
                advertiser  TEXT PRIMARY KEY,
                notarized   INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS order_monthly (
                contract_number INTEGER NOT NULL
                    REFERENCES orders(contract_number) ON DELETE CASCADE,
                year            INTEGER NOT NULL,
                month           INTEGER NOT NULL,
                market          TEXT    NOT NULL,
                gross           REAL    NOT NULL DEFAULT 0,
                net             REAL    NOT NULL DEFAULT 0,
                PRIMARY KEY (contract_number, year, month, market)
            );

            CREATE TABLE IF NOT EXISTS affidavits (
                affidavit_number  TEXT    PRIMARY KEY,
                contract_number   INTEGER REFERENCES orders(contract_number) ON DELETE SET NULL,
                bill_code         TEXT,
                estimate          TEXT,
                year              INTEGER NOT NULL,
                month             INTEGER NOT NULL,
                market            TEXT    NOT NULL,
                status            TEXT    NOT NULL DEFAULT 'draft',
                pre_bill          INTEGER NOT NULL DEFAULT 0,
                spot_count        INTEGER,
                gross_total       REAL,
                created_at        TEXT,
                updated_at        TEXT
            );

            CREATE TABLE IF NOT EXISTS affidavit_lines (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                affidavit_number  TEXT    NOT NULL
                    REFERENCES affidavits(affidavit_number) ON DELETE CASCADE,
                bill_code         TEXT,
                air_date          TEXT,
                air_time          TEXT,
                length            TEXT,
                program           TEXT,
                line_type         TEXT,
                gross             REAL,
                net               REAL,
                market            TEXT,
                contract_number   INTEGER,
                estimate          TEXT,
                source_file       TEXT
            );
        """)


def upsert_order(conn: sqlite3.Connection, record: dict):
    """Insert or replace an order record."""
    conn.execute("""
        INSERT INTO orders (
            contract_number, file_path, client, advertiser, contact,
            address, city, state, zip, phone, fax,
            billing_type, market, estimate, notes,
            agency_discount, date_order_written, revision, station_rep,
            emails, total_gross, total_net, last_updated
        ) VALUES (
            :contract_number, :file_path, :client, :advertiser, :contact,
            :address, :city, :state, :zip, :phone, :fax,
            :billing_type, :market, :estimate, :notes,
            :agency_discount, :date_order_written, :revision, :station_rep,
            :emails, :total_gross, :total_net, :last_updated
        )
        ON CONFLICT(contract_number) DO UPDATE SET
            file_path=excluded.file_path,
            client=excluded.client,
            advertiser=excluded.advertiser,
            contact=excluded.contact,
            address=excluded.address,
            city=excluded.city,
            state=excluded.state,
            zip=excluded.zip,
            phone=excluded.phone,
            fax=excluded.fax,
            billing_type=excluded.billing_type,
            market=excluded.market,
            estimate=excluded.estimate,
            notes=excluded.notes,
            agency_discount=excluded.agency_discount,
            date_order_written=excluded.date_order_written,
            revision=excluded.revision,
            station_rep=excluded.station_rep,
            emails=excluded.emails,
            total_gross=excluded.total_gross,
            total_net=excluded.total_net,
            last_updated=excluded.last_updated
    """, record)


def upsert_monthly(conn: sqlite3.Connection, contract_number: int, monthly: list[dict]):
    """Replace all monthly rows for a contract."""
    conn.execute(
        "DELETE FROM order_monthly WHERE contract_number = ?",
        (contract_number,)
    )
    conn.executemany("""
        INSERT INTO order_monthly (contract_number, year, month, market, gross, net)
        VALUES (:contract_number, :year, :month, :market, :gross, :net)
    """, monthly)


def get_expected_monthly(
    conn: sqlite3.Connection,
    contract_number: int,
    year: int,
    month: int,
    market: str,
) -> tuple[float, float] | None:
    """Return (gross, net) expected for a contract/market/month. None if not found."""
    row = conn.execute("""
        SELECT gross, net FROM order_monthly
        WHERE contract_number = ? AND year = ? AND month = ? AND market = ?
    """, (contract_number, year, month, market)).fetchone()
    return (row["gross"], row["net"]) if row else None


def _upsert_flag(conn, table: str, key_col: str, key_val: str, updates: dict):
    """Generic upsert helper for single-key flag tables."""
    existing = conn.execute(
        f"SELECT * FROM {table} WHERE {key_col} = ?", (key_val,)
    ).fetchone()
    if existing is None:
        cols = [key_col] + list(updates.keys())
        vals = [key_val] + list(updates.values())
        placeholders = ", ".join("?" * len(cols))
        conn.execute(
            f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})", vals
        )
    elif updates:
        set_clause = ", ".join(f"{k} = :{k}" for k in updates)
        updates[key_col] = key_val
        conn.execute(f"UPDATE {table} SET {set_clause} WHERE {key_col} = :{key_col}", updates)


# --- Agency flags (EDI) ---

def get_agency_flags(conn: sqlite3.Connection, agency: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM agency_flags WHERE agency = ?", (agency,)).fetchone()


def set_agency_flags(
    conn: sqlite3.Connection,
    agency: str,
    notarized: bool | None = None,
    edi: bool | None = None,
    edi_notes: str | None = None,
):
    updates = {}
    if notarized is not None:
        updates["notarized"] = int(notarized)
    if edi is not None:
        updates["edi"] = int(edi)
    if edi_notes is not None:
        updates["edi_notes"] = edi_notes
    if updates:
        _upsert_flag(conn, "agency_flags", "agency", agency, updates)


def get_all_agency_flags(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM agency_flags ORDER BY agency").fetchall()


# --- Advertiser flags (notarization) ---

def get_advertiser_flags(conn: sqlite3.Connection, advertiser: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM advertiser_flags WHERE advertiser = ?", (advertiser,)
    ).fetchone()


def set_advertiser_flags(
    conn: sqlite3.Connection,
    advertiser: str,
    notarized: bool | None = None,
):
    if notarized is not None:
        _upsert_flag(conn, "advertiser_flags", "advertiser", advertiser,
                     {"notarized": int(notarized)})


def get_order(conn: sqlite3.Connection, contract_number: int) -> sqlite3.Row | None:
    """Return the order record for a contract number."""
    return conn.execute(
        "SELECT * FROM orders WHERE contract_number = ?", (contract_number,)
    ).fetchone()


# --- Affidavits ---

def upsert_affidavit(conn: sqlite3.Connection, record: dict):
    """Insert or replace an affidavit header record."""
    from datetime import datetime
    now = datetime.now().isoformat()
    record.setdefault("created_at", now)
    record["updated_at"] = now
    conn.execute("""
        INSERT INTO affidavits (
            affidavit_number, contract_number, bill_code, estimate,
            year, month, market, status, pre_bill, spot_count, gross_total,
            created_at, updated_at
        ) VALUES (
            :affidavit_number, :contract_number, :bill_code, :estimate,
            :year, :month, :market, :status, :pre_bill, :spot_count, :gross_total,
            :created_at, :updated_at
        )
        ON CONFLICT(affidavit_number) DO UPDATE SET
            contract_number=excluded.contract_number,
            bill_code=excluded.bill_code,
            estimate=excluded.estimate,
            year=excluded.year,
            month=excluded.month,
            market=excluded.market,
            status=excluded.status,
            pre_bill=excluded.pre_bill,
            spot_count=excluded.spot_count,
            gross_total=excluded.gross_total,
            updated_at=excluded.updated_at
    """, record)


def upsert_affidavit_lines(conn: sqlite3.Connection, affidavit_number: str, lines: list[dict]):
    """Replace all lines for an affidavit."""
    conn.execute("DELETE FROM affidavit_lines WHERE affidavit_number = ?", (affidavit_number,))
    for line in lines:
        line["affidavit_number"] = affidavit_number
    conn.executemany("""
        INSERT INTO affidavit_lines (
            affidavit_number, bill_code, air_date, air_time, length, program,
            line_type, gross, net, market, contract_number, estimate, source_file
        ) VALUES (
            :affidavit_number, :bill_code, :air_date, :air_time, :length, :program,
            :line_type, :gross, :net, :market, :contract_number, :estimate, :source_file
        )
    """, lines)


def get_affidavit(conn: sqlite3.Connection, affidavit_number: str) -> sqlite3.Row | None:
    """Return the affidavit header for the given number."""
    return conn.execute(
        "SELECT * FROM affidavits WHERE affidavit_number = ?", (affidavit_number,)
    ).fetchone()


def get_affidavit_lines(conn: sqlite3.Connection, affidavit_number: str) -> list[sqlite3.Row]:
    """Return all spot lines for the given affidavit."""
    return conn.execute(
        "SELECT * FROM affidavit_lines WHERE affidavit_number = ? ORDER BY air_date, air_time",
        (affidavit_number,)
    ).fetchall()


def get_affidavits_for_month(conn: sqlite3.Connection, year: int, month: int) -> list[sqlite3.Row]:
    """Return all affidavit headers for a billing month, joined with order info."""
    return conn.execute("""
        SELECT a.*, o.advertiser, o.client
        FROM affidavits a
        LEFT JOIN orders o ON o.contract_number = a.contract_number
        WHERE a.year = ? AND a.month = ?
        ORDER BY a.pre_bill DESC, a.affidavit_number
    """, (year, month)).fetchall()


def next_affidavit_number(
    conn: sqlite3.Connection, year: int, month: int, pre_bill: bool = False
) -> str:
    """
    Compute the next available affidavit number for the given month.
    Regular: YYMM-001 through YYMM-499.
    Pre-bill: YYMM-500+.
    """
    yymm = f"{year % 100:02d}{month:02d}"
    if pre_bill:
        row = conn.execute("""
            SELECT MAX(CAST(SUBSTR(affidavit_number, 6) AS INTEGER)) AS max_seq
            FROM affidavits
            WHERE affidavit_number LIKE ? AND pre_bill = 1
        """, (f"{yymm}-%",)).fetchone()
        seq = max(500, (row["max_seq"] or 499) + 1)
    else:
        row = conn.execute("""
            SELECT MAX(CAST(SUBSTR(affidavit_number, 6) AS INTEGER)) AS max_seq
            FROM affidavits
            WHERE affidavit_number LIKE ? AND pre_bill = 0
        """, (f"{yymm}-%",)).fetchone()
        seq = (row["max_seq"] or 0) + 1
    return f"{yymm}-{seq:03d}"
