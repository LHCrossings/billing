# Billing Redesign — Session Handoff

> **Purpose of this file:** A handoff written at the end of a WSL Claude Code
> session so the work can continue on **Claude Code for Windows** without
> re-explaining context. Claude's chat history and per-machine memory do NOT
> travel between machines — only git-committed files do. This is that bridge.
>
> **When you open Claude Code for Windows in this repo, say:**
> *"Read BILLING_REDESIGN_HANDOFF.md and let's continue the billing redesign."*

---

## Why we're doing this

The current billing process is built around the **old Excel-based system**: lots
of manual copy/paste between workbooks, which is slow. Utilities already exist
(see inventory below) and have removed a lot of wasted time, but the goal is to
**automate far more of the flow** — not just speed up the copy/paste, but
replace it.

**This is a design conversation first, not a build.** The next step is for Lee
to walk Claude through the *actual current billing flow* end to end. Claude then
proposes a new, more automated workflow, iterates with Lee until it's right, and
only then builds.

---

## Why we moved to Windows

The billing system is the most Windows-coupled part of the whole stack:
- Data lives on the **`M:\` network drive** (`M:\Clients`, `M:\Accounting\Billing\billing.db`).
- Excel `.xlsm` / `.xlsx` files are central.
- Tools run via `uv run` on Windows.

The billing code **hardcodes native Windows paths** (e.g. `Path(r"M:\Clients")`
in `backfill.py:23`), which do not resolve under WSL/Linux. From WSL, Claude
cannot run or test the tools against real data. On Windows, everything resolves
natively. Hence the move.

**Design principle adopted:** make all `M:\...` paths **configurable** (env var
or config file) instead of hardcoded, so the tools become testable anywhere and
aren't locked to a drive letter.

---

## Repo recon (current state)

Python / `uv` CLI toolkit (not a web app). SQLite DB at
`M:\Accounting\Billing\billing.db`. Everything is keyed on the Etere
**contract number**. All Excel row data is referenced **by column letter
(A–AC), never by header name** — headers vary across files (hard rule).

### Two parallel pipelines

1. **Logs pipeline:** weekly `*.xlsm` logs (MASTER FOR BILLING tab, cols A–AC)
   → `aggregate.py` (validates/standardizes the "true month" in col S, filters
   by affidavit flag) → Master Billing Sheet → affidavits.
2. **Orders pipeline:** `backfill.py` walks `M:\Clients` → `order_parser.py`
   (Sales Confirmation + Run Sheet tabs) → SQLite → `validate.py` reconciles
   *expected* gross (from orders) vs. *actually billed* gross (from logs).

Plus **Worldlink** as a special case and `manage_db.py` for flags/affidavits.

### Utility inventory

| Utility | Purpose | Entry point |
|---|---|---|
| `backfill.py` | Scan `M:\Clients` for order files; upsert metadata + monthly gross to DB | `main()` ~line 61; scanner `iter_order_files()` lines 38–58 |
| `aggregate.py` | Load weekly logs, combine rows, validate/standardize month, filter | `load_log()`, `validate_and_standardize()`, `filter_rows()` |
| `order_parser.py` | Parse one order file (Sales Confirmation + Run Sheet) | `parse_order_file(path, metadata_only=False)` ~line 150 |
| `orders_db.py` | SQLite schema + CRUD | `init_db()`, `upsert_order()`, `upsert_monthly()` |
| `validate.py` | Compare actual billed gross vs. DB expected, per (contract, market) | `main()` ~line 220 |
| `worldlink.py` | Parse Worldlink Etere CSVs → A–AC billing rows | `load_worldlink_csv()`, `load_all_worldlink()` |
| `manage_db.py` | CLI: EDI flags, notarization flags, affidavits, show/remove orders | argparse `main()` |
| `debug_contract.py` | Dump raw log rows for a contract | `main()` |
| `diagnose.py` | Flag None/missing values (formula caching issues) | `main()` |

`scripts/EtereBridge/` is a separate, partially-integrated tool for processing
Etere CSV/Excel exports (gross-up of rounded rates, time/monetary normalization).
Was previously a submodule; now standalone. ~3 months stale.

### DB schema (SQLite)

- `orders` — order metadata, PK `contract_number`
- `order_monthly` — monthly gross per order, PK `(contract_number, year, month, revenue_type)`
- `agency_flags` — `agency` PK, `edi` (0/1), `edi_notes`
- `advertiser_flags` — `advertiser` PK, `notarized` (0/1)
- `affidavits` — PK `affidavit_number`, year/month/contract/advertiser/client/status
- `affidavit_lines` — line items per affidavit

### Inputs / outputs

- **In:** weekly logs (`logs/{MARKET} Log - {YYMMDD}.xlsm`), order files
  (`M:\Clients\{Agency}\{Client}\*.xlsx`), Worldlink Etere CSVs.
- **Out:** SQLite billing.db, Master Billing Sheet `.xlsm` (MASTER + CLEANED
  tabs), affidavits, console validation reports.
- **No live Etere DB connection** today — Etere data arrives only via exports/CSVs.
- **No tvinvoices.com / EDI / email integration** in code yet (EDI is just a
  per-agency flag + notes today).

---

## Known pain points / things Lee dislikes

1. **The `M:\` drive scan (`backfill.py` `iter_order_files()`)** — "good in
   theory, not good in practice." Crawling the entire M drive for all client
   orders is the thing to rethink. **Good news:** the scanner is the *only* part
   hardwired to `M:\Clients`; `order_parser.py` is fully decoupled and takes any
   path. So replacing "crawl everything" with a targeted ingest (or pulling
   billing data from Etere as the source of truth instead of re-reading order
   spreadsheets) is a contained change, not a rewrite.
   - **To explore with Lee:** *why* is it bad in practice? Likely stale/irrelevant
     files, slowness, or that order spreadsheets shouldn't be the source of truth.
2. Manual copy/paste between workbooks (the core complaint driving this redesign).
3. Month standardization / formula-caching issues requiring manual review.
4. Affidavit grouping still needs human review before numbering (`YYMM-XXX`).
5. Persistent manual rows in the Master Billing Sheet (e.g. Cornerstone Media &
   Desert Media PI revenue) that must be prompted for each time.

---

## Update 2026-07-07 (WSL session) — superseded; see BILLING_REDESIGN_DESIGN.md

A WSL session drafted a completion plan here before discovering that the
Windows-side walkthrough had already produced **`BILLING_REDESIGN_DESIGN.md`**,
which supersedes this file as the working reference. That plan assumed
`billing.db` remained the working store — the design doc retires it — so the
plan was withdrawn rather than left to mislead.

**One durable input from that session (Lee, verbatim intent):** the
**Commercial Log**, the **MASTER tab**, and the **CLEANED tab** must remain
the 29-column (A–AC) Excel grids — many reports run off those tables. Other
data may move to JSON/DB, but those three surfaces stay Excel; the redesign
should treat them as *generated views*, not hand-maintained workbooks. This
constraint has been added to the design doc's north star (§1).

---

## Next steps (original framing — superseded by BILLING_REDESIGN_DESIGN.md)

1. **Lee walks Claude through the current end-to-end billing flow.** Most useful
   framing:
   - **Inputs:** what arrives, from where, in what format.
   - **Steps:** each action in order — *especially every copy/paste: from where,
     to where, and why.* That's where the automation lives.
   - **Decisions/judgment:** anywhere Lee has to look and decide (rates, trade
     exclusions, what's billable, corrections).
   - **Outputs:** what's produced and who consumes it.
   - **Current utilities:** what they do today and where they stop short.
   - Sharing **sample files** (a real order file, a weekly log, the Master
     Billing Sheet template, a Worldlink CSV) is especially valuable.
2. Claude proposes a new, more automated flow (what's fully automated, what stays
   a human decision, where data comes from, rough tool shape) — design only.
3. Iterate, then build.

---

## Cross-repo context (Lee's environment)

- **Billing is its own repo** (`github.com:LHCrossings/billing.git`). Related
  repos: `ctv-orderentry` (Control Room + order entry), `EtereBridge` (run
  sheet / backwrite), `ReportSort` (WorldLink pre/post log sorting).
- Etere is the traffic/automation system; billing keys off Etere contract
  numbers. There is a direct Etere DB access pattern used in `ctv-orderentry`
  (pymssql) that billing does NOT currently use but *could* — relevant if we
  decide Etere should be the billing source of truth.
- Broadcast calendar: weeks run **Mon–Sun**; a broadcast month starts on the
  Monday of the week containing the 1st. Month logic is central to billing
  (`aggregate.expected_billing_month()`).
