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
    """Create tables if they don't exist."""
    with get_conn(db_path) as conn:
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

            CREATE TABLE IF NOT EXISTS client_flags (
                client      TEXT PRIMARY KEY,
                notarized   INTEGER NOT NULL DEFAULT 0,
                edi         INTEGER NOT NULL DEFAULT 0,
                edi_notes   TEXT
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


def get_client_flags(conn: sqlite3.Connection, client: str) -> sqlite3.Row | None:
    """Return client_flags row for a client name, or None if not set."""
    return conn.execute(
        "SELECT * FROM client_flags WHERE client = ?", (client,)
    ).fetchone()


def set_client_flags(
    conn: sqlite3.Connection,
    client: str,
    notarized: bool | None = None,
    edi: bool | None = None,
    edi_notes: str | None = None,
):
    """
    Insert or update client_flags for a client.
    Only updates fields that are explicitly passed (not None).
    """
    existing = get_client_flags(conn, client)
    if existing is None:
        conn.execute("""
            INSERT INTO client_flags (client, notarized, edi, edi_notes)
            VALUES (?, ?, ?, ?)
        """, (
            client,
            int(notarized) if notarized is not None else 0,
            int(edi) if edi is not None else 0,
            edi_notes,
        ))
    else:
        updates = {}
        if notarized is not None:
            updates["notarized"] = int(notarized)
        if edi is not None:
            updates["edi"] = int(edi)
        if edi_notes is not None:
            updates["edi_notes"] = edi_notes
        if updates:
            set_clause = ", ".join(f"{k} = :{k}" for k in updates)
            updates["client"] = client
            conn.execute(
                f"UPDATE client_flags SET {set_clause} WHERE client = :client",
                updates,
            )


def get_all_client_flags(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Return all client_flags rows, sorted by client name."""
    return conn.execute(
        "SELECT * FROM client_flags ORDER BY client"
    ).fetchall()


def get_order(conn: sqlite3.Connection, contract_number: int) -> sqlite3.Row | None:
    """Return the order record for a contract number."""
    return conn.execute(
        "SELECT * FROM orders WHERE contract_number = ?", (contract_number,)
    ).fetchone()
