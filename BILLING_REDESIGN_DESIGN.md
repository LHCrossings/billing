# Billing Redesign — Design Document

> **Status:** Living design doc, written during/after a step-by-step walkthrough of the
> current billing process (June 2026 as the worked example). Supersedes the earlier
> `BILLING_REDESIGN_HANDOFF.md` as the working reference. This is still **design, not build.**
>
> **To resume:** *"Read BILLING_REDESIGN_DESIGN.md and let's continue the billing redesign."*
> Pick up at **§9 "Not yet walked"** — that's where the walkthrough paused.

---

## 1. The north star (decided)

- **Etere is the source of truth.** Lee maintains clean Etere contracts.
- **Contract number is the universal key.** It is stable: **one contract per IO**;
  Worldlink revisions keep the same contract number (just revised), they never mint a new one.
- **Retire `billing.db` entirely.** Better ways now exist (below).
- **Retire the `M:\Clients` crawl (`backfill.py`) and order-spreadsheet parsing
  (`order_parser.py`)** as billing inputs — replaced by Etere queries keyed on contract number.
- **Goal:** `contract number in → fully-formed, reconciled affidavit out, exceptions flagged.`
  Move affidavits off the hand-edited Excel system entirely.

---

## 2. Current process (AS-IS), as walked

### 2.1 The billing book
- Books live locally at `C:\Work Temp\Billing`. Archives in `Miscellany/`.
- One workbook per month: `Master Billing Sheet YYMM.xlsm` (e.g. `…2606.xlsm` = June 2026).
  It is the **bible** for the month.
- **New month = clone last month's book**, rename, then **reset every affidavit** (65–100/mo):
  erase affidavit number (B1), set **Billing Cycle** (L7) to the month's 15th, set
  **Billing Date** (L9) to the last calendar day. Then **clear the Master tab**.
- Workbook tab anatomy (in order):
  | Tab | Role |
  |---|---|
  | **Scratch** | Sorting workspace — arrange spots to assign onto affidavits |
  | **Master** | Aggregation of every log line for the billing month. Cleared fresh each book; then frozen as the raw record |
  | **Cleaned** | Final to-the-cent record of every billed line. Downstream apps report off this *(not yet walked in detail)* |
  | *(misc tabs, e.g. **Ford Tally**)* | Ford Tally = pivot for Ford co-op invoicing. Several trimmable |
  | **Individual affidavits** | One per contract. Run up to → |
  | **"MAKE SURE TO DO THIS!!!"** | Notes + invoice **email distribution list** |
  | **Invoice templates** | Everything after |

### 2.2 Broadcast month / billing month (terminology)
- Broadcast **weeks run Mon–Sun**. A **broadcast month** starts on the Monday of the week
  containing the 1st, through the Sunday before the next broadcast month.
- **Billing month = the union (max date range) of the broadcast month and the calendar month.**
  June 2026: broadcast June = Mon 6/1 → Sun 6/28; calendar June = 6/1 → 6/30;
  **union = 6/1–6/30** (6/29–6/30 are broadcast July but calendar June, so the union catches them).

### 2.3 Weekly log aggregation (Control Room) — runs every Monday
Logs at `K:\Traffic\logs\{YEAR}\{MM Month}\{MARKET} Log - {YYMMDD}.xlsm`
(date = Monday of the broadcast week; 10 markets: CMP CVC DAL HOU LAX MMT NYC SEA SFO WDC).
Two operations, in order:
1. **`/master-control/compile-logs`** — runs the per-log macro on each of the 10 logs,
   consolidating every tab within that file into its **`MASTER FOR BILLING`** tab.
2. **`/billing/compile-logs`** — appends all 10 logs' `MASTER FOR BILLING` rows into the
   book's **`MASTER`** tab (market order), A–AC verbatim. (No DB, no Etere; pure Excel I/O.)

By month-end (after a partial pull of the week of 6/29 to grab 6/29–6/30), **Master holds the
full billing month**, then is frozen as the raw, uncleaned record.

### 2.4 Spot List cleanup (`Spot List.xlsx` — staging area)
Purpose: strip the fluff; Master stays the full record.
1. Copy all of Master → Spot List `master` tab.
2. Delete rows where **col S ≠ the billing month** (S = "true month", formula-assigned to
   `date(yr, mo, 15)` — the **authoritative billing-month tag**).
3. Delete rows where **col AA = N** (affidavit-not-required; these live in Master only).
4. Clear the `finished` tab.
5. Sort `master` by **col I (air time) ascending** → naturally clusters by I contents:
   - **`"Billing line"` text** → cut to `finished` (production charges, credits, sometimes **PKG** package charges — they bill but never aired).
   - **PRG** lines (blank I) → cut to `finished`.
   - **Remaining timed commercials** (COM, BNS, BB, AV, **TRD**) → cut to `finished` after the time check.
6. *(Legacy, being retired)* **`Worksheet` tab** copies A–G + I to check each air time falls in
   the spot's allowed window — caught manual-entry typos. **Obsolete now that Control Room fills
   exact air times from Etere.** → First concrete cut.
7. Visual-only coloring on `finished`: commercials **yellow**, billing lines **blue**, PRG **pink** (cosmetic; not coded).
8. `finished` = the complete billable set → pasted into the book's **`SCRATCH`** tab.

### 2.5 Worldlink (separate stream — broadcast-billed, can run earlier)
- Broadcast-billed, so ready as soon as broadcast June closes (Sun 6/28) → can do Mon 6/29,
  before calendar-billed clients are ready (those wait for 6/30).
- **`/billing/worldlink-placement`** → enter **broadcast range 6/1–6/28**, leave **House / 15%**.
- Queries Etere directly for **Agency 133 (Worldlink)** contracts active in range, pulls the
  **actually-aired spots** (TPALINSE/trafficPalinse), aggregates → **`WL_Placement_{from}_{to}.xlsx`**
  (one `Run Sheet`, ~26k rows, A–AC layout), broker fees split **DAL vs non-DAL**.
- Output pasted into the same **`SCRATCH`** tab. (Already the Etere-as-truth pattern, working.)

### 2.6 Scratch sort + prep
- With regular `finished` + `WL_Placement` both in `SCRATCH`:
- **Sort by A → AB → AC → B → I** (bill code, contract, market, air date, air time) → contiguous per-contract blocks.
- **Worldlink market relabel:** set **col AC → "Admin"** (national-buy label) for every market
  **except DAL, which stays DAL** (Dallas = The Asian Channel). **Col Q preserves the original market.**
- **Market shading** (driven by col Q) — carries onto the affidavit (see §2.8 rules).

### 2.7 Affidavit creation (one per contract)
- **Grouping key: col AB (contract number); if absent, fall back to col A + col O (bill code + estimate).**
- Today: split `SCRATCH` into individual affidavit **tabs**. Returning contract → reuse its
  carried-over tab; new IO → new tab from a template; dropped client → delete stale tab.
- Per affidavit:
  - Delete last month's spots (e.g. rows 13–642), paste this contract's lines in their place.
  - Enter **subtotals**: spot count (e.g. L643) + dollar total (e.g. P643); footer formulas below.
  - **Verify header** against the most-recent order revision on `M:\Clients`; **copy comments**
    from the order's comments box.
  - Eyeball total vs **expected monthly amount** (mismatch is OK if a spot didn't air).
  - Set **network in F1**: `AFFIDAVIT OF PERFORMANCE — CROSSINGS TV` vs The Asian Channel
    (**DAL → Asian Channel, Admin → Crossings**). Manual today → error-prone.
  - Header **contract-number formula** (e.g. `L10 = =AB13`) added **last**, derived from a spot line.

### 2.8 Affidavit anatomy & rules (MUST survive any redesign)
- **Universal 29-column format (A–AC)** company-wide since day one. The *data* is always the same;
  only **header labels differ per document** → **always reference by column letter, never header.**
- Header block rows 1–10: affidavit # (B1), Client/Advertiser/Contact/Address, Estimate,
  Billing Type, **Billing Cycle (L7)**, Market, **Billing Date (L9)**, **Contract Number (L10)**.
  Header size is **variable** (e.g. extra email rows) → spots start at **the row beneath the
  label row (row 12 here)**, *not* a fixed row. (Data-reference's "row 16" is wrong/variable.)
- **Shading rules (permanent requirement):**
  - Default: all lines **clear**.
  - **Multi-market affidavit:** alternate by market block — first market clear, next light blue, next clear, …
  - **Billing line alone:** clear.
  - **Billing line with commercials under it:** billing line **light blue**, spots **clear**.
  - **Precedence:** multi-market alternation **wins** over the billing-line rule when both apply.

---

## 3. What can come from Etere (investigation findings)

Etere DB: `Etere_crossing` @ `100.85.38.72` (override via `ETERE_DB_SERVER`).
Client: `ctv-orderentry/browser_automation/etere_direct_client.py` (pyodbc/pymssql).
Etere docs in ctv-orderentry: `.claude/documents/data-reference.md`,
`docs/ETERE_CLIENT_GOLDEN_RULES.md`, `docs/API_AND_EXPORT_CONTRACTS.md`.

| Need | Source | Status |
|---|---|---|
| Aired spot lines (affidavit body) | `TPALINSE` + `trafficPalinse` | ✅ in Etere |
| Header identity (client/advertiser/address/dates/agency/market) | `CONTRATTITESTATA` + `ANAGRAF` | ✅ in Etere |
| **Comments box** | **`CONTRATTITESTATA.NOTE`** (writable; Lee maintains) | ✅ in Etere |
| Contract terms (rate `IMPORTO`, ordered `N_PASSAGGI`, daypart, dates) | `CONTRATTIRIGHE` | ✅ in Etere |
| **Line type** (COM/BNS/BB/BOOK/AV/TRD/BART…) | `CONTRATTIRIGHE.NEWTYPE` (semicolon codes, `;COMS` suffix) | ✅ in Etere |
| Copy/ISCI code | `FILMATI.COD_PROGRA` | ✅ in Etere |
| Agency commission %, sales agents | `ANAGRAF.Commissione`, `AGENTE1–5` | ✅ in Etere |
| End date, Day, Time out, **true-month (S)**, broker fees (T), net (V), spot value (R) | — | ⚙️ **computed** (deterministic) |
| Revenue type (X), Agency flag (Z), **Affidavit-required (AA)**, AE assignment | — | ⚠️ **billing-side** — needs a home |
| Billing type Calendar/Broadcast (Y) | `FATTURAZIONE_PRINCIPALE` (or override) | mixed |

- **Generalized contract query:** the Worldlink query works for **any contract** by
  parameterizing the hardcoded `AGENZIA = 133`. No Etere-side limitation.
- **Non-airing billable lines** (rate but no airings) are queryable per contract via
  `IMPORTO > 0 AND NOT EXISTS (trafficPalinse link)` — relevant to paid programming (below).

---

## 4. Target architecture

1. **Etere = primary source** (by contract number): header, terms, NOTE/comments, line types, airings.
2. **Logs = independent validation** of what *actually* aired. Etere is **not blindly trusted**
   because: master control sometimes deletes a contract-linked spot and **re-adds it unlinked**
   (true orphan, no bridge) — it aired but Etere's contract query misses it. The log catches it.
3. **Reconciliation engine** (≈ `validate.py` reborn, now **Etere-vs-logs** instead of orders-vs-logs):
   compare per contract/market/month; **flag only discrepancies** so human reviews the few, not all 99.
   Two classes:
   - **Coverage/count mismatches** — aired-but-unlinked, didn't-air, etc.
   - **Rounding deltas** — *only* when **grossing up an agency order delivered in net** (net→gross).
     That's the sole source of cent-level drift; everything else ties out exactly. (see §6).
4. **Orphan spot recovery** (resolution half of reconciliation):
   - On "contract short N spots," find **true orphan airings** (`TPALINSE` with no `trafficPalinse`
     bridge) in the contract's **market(s) + date range + daypart**, **filtered to matching copy code**.
   - **Copy/ISCI code is the discriminator** — fillers (PI spots, PSAs) carry different copy codes
     and fall away automatically; near-zero false positives.
   - Surface as **"this orphan almost certainly fills contract X's gap — confirm?"** Never auto-applied
     (it moves billing dollars). Human confirms → re-associate for billing.
5. **Generated affidavits** (not hand-edited carryovers): emit **one per distinct contract present**
   this month; header + comments from Etere by contract number; totals computed; **network (F1) and
   market shading auto-derived**. Eliminates the clone-and-reset-99-affidavits ritual, the manual
   header verification, the F1 typos, and `billing.db`.
   - **Authoritative total (decided):** an affidavit's total is **always the sum of the spots listed
     on it** — the displayed lines are the truth. Comparing to the order total is **validation only**
     (flag missing/short); it never overrides the affidavit total.
6. **Thin billing overlay** for the ⚠️ billing-side fields Etere can't give (revenue type, agency
   flag, affidavit-required, AE) — small config/table or rules, **not** a re-implementation of `billing.db`.

---

## 5. Phasing

- **Step 1 (build now):**
  - Etere-native for the bulk: **aired commercials, regular + Worldlink, by contract number**,
    reconciled vs logs, exceptions flagged, **orphan recovery** included.
  - **Billing lines (production + paid programming) = a manual-entry slot** for now.
  - **Design that slot as a pluggable input source** behind a stable interface
    ("here is the list of billing lines for contract X"), so the source can swap later with no rearchitecting.
- **Step 2 (after Lee solves the Etere modeling):**
  - **Paid programming → Etere contracts that don't go to the scheduler** (appear as billable
    contract lines with no airings — already matches the `NOT EXISTS` query, so possibly pull-able
    even in step 1 once built that way).
  - **Production → Etere** once the accurate representation is worked out (genuinely harder).
  - Swap the manual slot for Etere queries.

---

## 6. Parked / open questions

- **Rounding (scoped + decided):** the affidavit total is **always the sum of the spots listed**
  (§4.5). Rounding drift happens **only when grossing up an agency order delivered in net** —
  Etere rates are 3-decimal, grossing-up at full precision diverges by cents. Two sub-problems
  remain to design: (a) a **consistent grossing-up method** (net→gross) so per-spot values are
  right, and (b) a **good way to match the order total from the M drive** (the agency's net order)
  as a validation check. **Open:** does the expected/order total come from the **M-drive order file**
  (where the agency's net amount lives) or from **Etere** contract terms? Net-given agency orders may
  force a targeted M-drive lookup (one order's total — not the old "crawl everything").
- **Home for billing classifications** (revenue type X, agency flag Z, affidavit-required AA, AE):
  Etere where possible, else thin overlay, else default rules?
- **Verify in live Etere:** `SELECT NOTE FROM CONTRATTITESTATA …` readable by contract number;
  the orphan query (`TPALINSE` with no bridge) returns what we expect.
- **Billing type (Y):** derive from `FATTURAZIONE_PRINCIPALE` or allow override?

---

## 7. Concrete cuts already identified

- ❌ `Worksheet` time-window check (Etere times make it redundant)
- ❌ `billing.db` (orders, order_monthly, flags, affidavits)
- ❌ `backfill.py` M:\Clients crawl + `order_parser.py` as billing inputs
- ❌ Clone-and-reset-every-affidavit ritual
- ❌ Manual one-by-one header verification against M:\Clients
- ❌ Manual F1 network labeling (auto-derive from DAL vs Admin)
- ➖ Spot List round-trip + Scratch sort/split (largely collapses once affidavits are generated)
- ➖ Misc trimmable tabs (Ford Tally, etc.) — revisit case by case

---

## 8. Doc corrections discovered (apply to data-reference.md later)

- Add line type **TRD (trade)**; note Etere `NEWTYPE` codes: COM, BNS, BB, BOOK, AV, TRD, BART (`;COMS` suffix).
- Affidavit **spot-start row is variable** (row beneath the label row), not fixed at 16.
- **Etere `CONTRATTITESTATA.NOTE`** field exists (comments source).
- Reaffirm: header labels differ across documents (WL_Placement, affidavit tabs) — **go by column letter**.

---

## 9. Not yet walked — RESUME HERE

The walkthrough paused after affidavit creation. Still to cover:
- **Affidavit numbering** (`YYMM-XXX`; regular 001–499, pre-bills 500+) — how/when assigned.
- **Cleaned tab compile** — the to-the-cent downstream contract; what builds it and what consumes it.
- **Invoices / EDI** — the `!!EDI` folder, `CSV Template for EDI Billing.xlsx`, distribution
  (tvinvoices.com? email?). EDI is a per-agency flag today.
- **Revenue sharing** — the `!!Revenue Sharing` folder.
- **Persistent manual rows** — Cornerstone Media & Desert Media PI revenue (Cleaned rows 2–3);
  Worldlink Broker Fees DNI (Cleaned rows 4–5).
- **Ford Tally** and other co-op / trimmable tabs.
- **"MAKE SURE TO DO THIS!!!"** notes + email distribution list.

---

## 10. Key references

- **Control Room (ctv-orderentry) endpoints:** `/master-control/compile-logs`,
  `/billing/compile-logs`, `/billing/worldlink-placement[/generate]`, `/backwrite/worldlink/*`.
- **Etere code:** `browser_automation/etere_direct_client.py`; queries in
  `src/web/routes/orders.py`, `src/backwrite/eterebridge_runner.py`.
- **Etere tables:** `CONTRATTITESTATA`, `CONTRATTIRIGHE`, `TPALINSE`, `trafficPalinse`,
  `FILMATI`, `ANAGRAF`.
- **Billing repo today:** `aggregate.py`, `validate.py`, `worldlink.py`, `manage_db.py`,
  `orders_db.py`, `order_parser.py`, `backfill.py` (the last three slated for removal).
- **Paths:** books `C:\Work Temp\Billing`; logs `K:\Traffic\logs\{YEAR}\{MM Month}\`;
  orders (being retired) `M:\Clients\{Agency}\{Client}`.
