# Lessons Learned

Rules derived from corrections during development. Review at session start.

<!-- Format:
## [short rule title]
**Rule:** what to do or avoid
**Why:** reason / past incident
**How to apply:** when this kicks in
-->

## Update lessons files after corrections
**Rule:** After any correction or new guidance from the user, add an entry to both `tasks/lessons.md` and `memory/feedback_lessons.md`.
**Why:** User explicitly asked for this so rules accumulate and mistakes don't repeat.
**How to apply:** Any time the user corrects an approach, clarifies a preference, or says "remember to..." — log it immediately in both files.

## Pull and read BILLING_REDESIGN_DESIGN.md before proposing plans
**Rule:** `git pull` and read `BILLING_REDESIGN_DESIGN.md` before proposing any billing plan or architecture. Never build on `billing.db` — it is being retired (with `backfill.py`/`order_parser.py`); Lee considers it entirely unnecessary and Etere is the source of truth.
**Why:** Lee works across two machines (WSL + Windows); either clone and its per-machine Claude memory can be stale. A 2026-07-07 WSL session proposed a billing.db-based completion plan that the Windows walkthrough had already obsoleted, and it had to be withdrawn after pushing.
**How to apply:** At session start on either machine: pull, read the design doc's §1 (north star) and §9 (resume point), and treat committed docs as authoritative over per-machine memory.
