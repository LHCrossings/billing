# Billing Data Reference

## Log Files (`logs/*.xlsm`)
- Sheet: `MASTER FOR BILLING`
- Filename: `{MARKET} Log - {YYMMDD}.xlsm` (date = Monday of broadcast week)
- Markets: CMP, CVC, DAL, HOU, LAX, MMT, NYC, SEA, SFO, WDC
- Always reference columns by letter (A–AC), not header name — headers vary across files

## Column Definitions (A–AC)
| Col | Field | Notes |
|-----|-------|-------|
| A | Bill code | client, or agency:client |
| B | Start date | |
| C | End date | almost always same as B — legacy |
| D | Day of week | |
| E | Time in | scheduled window start |
| F | Time out | scheduled window end |
| G | Length | |
| H | Program/commercial name | |
| I | Actual air time | blank = normal for PRG; error for COM/BB/BNS/AV; "billing line" = one-time charge |
| J | Language | |
| K | Show | program the commercial aired in |
| L | Line indicator | usually 1 |
| M | Etere line number | if exists |
| N | Type | COM, PRG, BB, BNS, CRD, PKG, PRD, AV |
| O | Estimate number | |
| P | Gross rate | appears on invoices |
| Q | Makegood | y/n and if so, for what |
| R | True spot value | PKG lines hold full package rate; spots show $0. Internal reports only, not invoices |
| S | True month | formula-assigned; standardized to date(year, month, 15) |
| T | Broker fees | agency/broker commission |
| U | Priority | PRG=1, COM=4; used for sorting |
| V | Net rate | gross minus broker fee |
| W | AE | account executive |
| X | Revenue type | |
| Y | Billing type | Calendar or Broadcast — drives col S calculation |
| Z | Agency flag | agency or non-agency; most agencies 15% commission |
| AA | Affidavit required | y/n — AA=N rows stay in MASTER only |
| AB | Etere contract number | primary key; null/0 for PRD and CRD lines |
| AC | Market | "Admin" = Worldlink/Crossings TV network-level entry |

## Line Types (col N)
| Type | Description | Blank air time (col I)? |
|------|-------------|------------------------|
| COM | Commercial | Error |
| PRG | Program | Normal |
| BB | Billboard | Error |
| BNS | Bonus spot | Error |
| AV | Added value ($0) | Error |
| CRD | Credit | N/A — often no contract number |
| PKG | Package charge | N/A |
| PRD | Production charge | N/A — always manual, usually no contract number |

## Broadcast Calendar
- Weeks run **Monday–Sunday**
- Broadcast month starts on the **Monday of the week containing the 1st** of the Gregorian month
  - e.g. if Feb 1 is Wednesday → broadcast February starts Monday Jan 30
- Months can overlap Gregorian month boundaries

## Billing Pipeline (summary)
1. Aggregate MASTER FOR BILLING from all weekly logs
2. Filter: AA=N rows stay in MASTER, never leave
3. Validate: flag blank col I where col N is COM/BB/BNS/AV
4. Sort: A → AB → AC → B → I
5. Group into affidavits: by col AB (contract) if valid; else by col A + col O
6. Generate affidavits: header from Sales Confirmation, spots from row 16
7. Human review
8. Number affidavits: YYMM-XXX (001+); pre-bills = 500+ range
9. Compile to CLEANED tab

## Filtering Rules by Destination
| Destination | Rows included |
|-------------|--------------|
| MASTER | All rows |
| Affidavits | Exclude AA=N, exclude col A = "DO NOT INVOICE" |
| CLEANED | Affidavit lines + DNI broker lines + persistent rows 2–5 |

## File Paths
| File | Path |
|------|------|
| Weekly logs | `logs/{MARKET} Log - {YYMMDD}.xlsm` |
| Sales Confirmations | `Local Order/{Agency} - {Advertiser} {Market} {Estimate}.xlsx` |
| Master Billing Sheet | `Billing Book/Master Billing Sheet YYMM.xlsm` |
| Billing DB | `M:\Accounting\Billing\billing.db` |
| Customers DB | `M:\CLIENTS\customers.db` |
| Order files | `M:\Clients\{Agency}\{Client}\filename.xlsx` |

## Sales Confirmation Structure
- Tab 1 `Sales Confirmation`: header info — client, contact, address, phone, fax, email(s), advertiser, estimate, billing type, market, contract number, notes
  - End of header block: "Line Number" in col B
  - Contract number: row 9, right side
- Tab 2 `Run Sheet`: same column layout as MASTER FOR BILLING (A–AC)
- Contract number in col AB ("Contract") is script-generated — reliable key

## CLEANED Tab (Master Billing Sheet)
- Row 1: Header
- Row 2: Cornerstone Media Group (PI revenue — manual prompt)
- Row 3: Desert Media Partners (PI revenue — manual prompt)
- Row 4–5: WorldLink Broker Fees DNI — computed last from CLEANED data
- Rows 6+: DNI broker lines, then all affidavit lines
