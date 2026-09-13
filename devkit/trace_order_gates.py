"""Ground truth for the ordering funnel: which line each SKU actually executes.

The funnel used to be counted by reading `reasoning`, a string the engine
writes about itself. That proves the engine says consistent things, not that it
does them -- and it was wrong: three exits emit the phrase "no demand signal",
so a classifier that tested that phrase before "[Schedule:" moved 2,900 lines
from the calendar gate into the demand gate. This traces LINE EXECUTION inside
calculate_order_quantity instead, so the counts come from the interpreter.

TWO RULES, both learned from getting this wrong:

1. NO HARDCODED LINE NUMBERS. The first version of this probe carried a dict of
   {311: "AMIT", 322: "MANDE", ...}. One edit to simulation_bridge.py and every
   count silently moves to the wrong gate -- the exact failure it exists to
   catch. Every line here is resolved from source text at run time.

2. THE COUNTS MUST CLOSE. Every SKU reaches exactly one
   `recommendations.append(rec)`, so those lines PARTITION the population and
   must sum to it. The first version counted a hand-written list of exits, came
   up 389 short, and reported the shortfall as "unattributed" -- when those 389
   were gate-15 exits taking a second reasoning branch that the hand-written
   list had simply missed. A remainder is not a mystery, it is an
   uninstrumented branch. This asserts, and fails the run.

Usage:  python devkit/trace_order_gates.py <pos.db> [--as-of YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import copy
import inspect
import os
import sys
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# The labels below are the engine's own reasoning literals, and several carry an
# em-dash ("review or delist"). A cp1252 console dies on those AFTER the trace
# has run -- ten minutes of work lost at the print. Never let formatting throw.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, ValueError):  # pragma: no cover - old/odd streams
    pass

EXIT_MARK = "recommendations.append(rec)"
#: A line that decides what the SKU is TOLD. The last one executed before the
#: exit is the reason attached to it.
REASON_MARKS = ("rec['reasoning']", "rec['recommended_quantity']")


def _index(func):
    """{lineno: source text} for the exits and the reason-writes, from source.

    Returns (exits, reasons). Raises if the function has no exits -- that means
    the marker text moved and every count below would silently read zero.
    """
    src, start = inspect.getsourcelines(func)
    exits, reasons = {}, {}
    for off, text in enumerate(src):
        ln, t = start + off, text.strip()
        if t.startswith("#"):
            continue
        if EXIT_MARK in t:
            exits[ln] = t
        elif any(m in t for m in REASON_MARKS):
            reasons[ln] = t
    if not exits:
        raise SystemExit(
            f"no {EXIT_MARK!r} found in {func.__qualname__} -- the loop was "
            "restructured and this probe must be rewritten, not trusted")
    return exits, reasons


#: The per-SKU fields every gate below actually reads. If two runs agree here
#: and disagree on the funnel, the difference is in the loop; if they disagree
#: here, the funnel difference is inherited and chasing gates is wasted work.
#: Spelled exactly as the loop reads them -- `lead_time_days`, not `lead_time`.
#: A key that does not exist hashes as "None" on every row, so the fingerprint
#: stays stable while ignoring the field entirely: a hand-written list that
#: silently covers less than it claims, which is the mistake this file exists
#: to stop. The presence counts printed below are what catch a wrong spelling.
DECISION_INPUTS = ("avg_daily_sales", "current_stock", "median_gap_days",
                   "lead_time_days", "days_since_delivery", "reorder_point",
                   "sales_90d", "is_fresh", "supplier_name")


def _fingerprint(enriched) -> str:
    """One hash over every input the ordering loop consumes.

    Two runs of the same engine on the same store must produce the same digest.
    Two traces once disagreed on 7,589 SKUs with byte-identical inputs, and
    without this there was no way to tell whether enrichment or the loop had
    moved -- so the whole funnel had to be re-derived to find out.
    """
    import hashlib
    h = hashlib.md5()
    for p in sorted(enriched, key=lambda r: str(r.get("itm_cd") or r.get("ITM_CD") or "")):
        row = [str(p.get("itm_cd") or p.get("ITM_CD") or "")]
        for f in DECISION_INPUTS:
            v = p.get(f)
            row.append(f"{v:.6f}" if isinstance(v, (int, float)) else str(v))
        h.update("|".join(row).encode("utf-8", "replace"))
    return h.hexdigest()[:16]


def _ads_summary(enriched) -> str:
    vals = sorted(float(p.get("avg_daily_sales") or 0) for p in enriched)
    nz = [v for v in vals if v > 0]
    if not nz:
        return "ADS: none non-zero"
    return (f"ADS: nonzero {len(nz):,}  median {nz[len(nz) // 2]:.4f}  "
            f"p90 {nz[int(.9 * len(nz))]:.4f}  max {nz[-1]:.2f}  "
            f"sum {sum(nz):.1f}")


def _store_provenance(db_path: str, as_of_date: str) -> None:
    """Say WHICH store this is, and whether its clock agrees with --as-of.

    Two runs of this probe once disagreed on 7,589 SKUs and an hour went into
    the engine looking for nondeterminism. The engine was fine: one run had
    been pointed at a stale build whose identities were collapsed and whose
    receipts were all one flat future date. Nothing in the output said so.

    Every figure below is a property of the FILE, not of the engine, and each
    one distinguishes a sound build from the known-bad one.
    """
    import sqlite3
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        one = lambda q: conn.execute(q).fetchone()
        items, split = one(
            "SELECT COUNT(*), SUM(ITM_CD <> SCAN_ITM_CD) FROM ITEM_MST")
        recv_n, recv_lo, recv_hi = one(
            "SELECT COUNT(DISTINCT SM_LAST_RECV_DT), MIN(SM_LAST_RECV_DT), "
            "MAX(SM_LAST_RECV_DT) FROM STOCK_MASTER")
        bill_lo, bill_hi, bills = one(
            "SELECT MIN(BILL_DT), MAX(BILL_DT), COUNT(*) FROM POS_SALES_HDR")
        future_recv = one("SELECT COUNT(*) FROM STOCK_MASTER "
                          f"WHERE SM_LAST_RECV_DT > '{as_of_date}'")[0]
        future_bills = one("SELECT COUNT(*) FROM POS_SALES_HDR "
                           f"WHERE BILL_DT > '{as_of_date}'")[0]
    finally:
        conn.close()

    print(f"\n  STORE   {db_path}")
    print(f"    items {items:,}  |  distinct item code vs barcode: "
          f"{(split or 0):,}")
    print(f"    last receipt: {recv_n:,} distinct dates, {recv_lo} .. {recv_hi}")
    print(f"    bills       : {bills:,} from {bill_lo} .. {bill_hi}")
    print(f"    measuring as of {as_of_date}")

    if not split:
        print("    !! ITM_CD == SCAN_ITM_CD on every row. The two identities "
              "are collapsed, so the\n       item-code and barcode match "
              "branches are one test run twice.")
    if recv_n <= 1:
        print("    !! ONE receipt date for the whole book. days_since_delivery "
              "is a constant, so the\n       stale-fresh and dead-stock gates "
              "cannot fire on evidence.")
    if future_recv:
        print(f"    !! {future_recv:,} of {items:,} receipts are dated AFTER "
              f"the as-of date. Those lines have a\n       NEGATIVE "
              f"days_since_delivery and can never reach the staleness gates.")
    if future_bills:
        print(f"    !! {future_bills:,} bills are dated AFTER the as-of date. "
              f"The 30-day demand bucket is\n       `BILL_DT >= :c30` with no "
              f"upper bound, so these all land in it at 60% weight and\n"
              f"       inflate ADS. Seed and measurement are on different "
              f"clocks.")


def _label(text: str, width: int = 62) -> str:
    """A readable name for a branch: the literal the engine writes."""
    for a, b in (("= (", ""), ("= f", ""), ("= ", ""), ("+= f", ""), ("+= ", "")):
        if a in text:
            text = text.split(a, 1)[1]
            break
    text = text.strip().strip("(").strip().lstrip('f"').lstrip("f'")
    text = text.strip('"').strip("'").strip()
    return (text[:width - 3] + "...") if len(text) > width else text


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("db")
    ap.add_argument("--as-of", default="2025-12-09",
                    help="measure as of this date (the data's own horizon), "
                         "not today; pass 'today' to use the wall clock")
    args = ap.parse_args()

    os.environ.update({"OASIS_DB_PATH": args.db,
                       "OASIS_ORDER_MODEL": "order_up_to"})
    if args.as_of != "today":
        os.environ["OASIS_AS_OF"] = args.as_of
    # A stale URL in the environment silently wins over OASIS_DB_PATH and the
    # probe then measures a different store than the one named on the command
    # line.
    for k in ("OASIS_POS_DB_URL", "OASIS_DB_URL"):
        os.environ.pop(k, None)

    from oasis.logic.db_connector import UniversalConnector, SchemaMapper
    from oasis.logic.pos_erp_adapter import PosErpAdapter
    from oasis.logic.simulation_bridge import SimulationOrderUtil

    target = SimulationOrderUtil.calculate_order_quantity
    exits, reasons = _index(target)
    code = target.__code__

    _store_provenance(args.db, os.environ.get("OASIS_AS_OF", "today"))

    conn = UniversalConnector(f"sqlite:///{args.db}", SchemaMapper.for_pos_erp())
    adapter = PosErpAdapter(conn)
    org = adapter.fetch_all_organizations()[0]["ORG_CD"]
    util = SimulationOrderUtil(os.path.join(ROOT, "oasis", "data"))
    enriched = util.prepare_sku_data(adapter.fetch_enriched_products(org))
    print(f"\n  {_ads_summary(enriched)}")
    print(f"  decision-input fingerprint: {_fingerprint(enriched)}  "
          f"(same store + same engine must give the same digest)")
    missing = [f for f in DECISION_INPUTS
               if not any(p.get(f) is not None for p in enriched)]
    for f in DECISION_INPUTS:
        n = sum(1 for p in enriched if p.get(f) is not None)
        print(f"      {f:<22}{n:>8,} rows" + ("   <-- NOT PRESENT" if not n else ""))
    if missing:
        raise SystemExit(
            f"\nfingerprint covers nothing for {missing}: the key is spelled "
            f"differently on the enriched record, so this digest would match "
            f"across two runs that differ in that field. Fix the spelling.")
    print(f"\ntracing {len(enriched):,} SKUs through {target.__qualname__} "
          f"({len(exits)} exits, {len(reasons)} reason branches)\n", flush=True)

    by_exit: Counter = Counter()
    by_pair: Counter = Counter()
    pending = {"ln": None}

    def line_tracer(frame, event, arg):
        if event == "line":
            ln = frame.f_lineno
            if ln in exits:
                by_exit[ln] += 1
                by_pair[(ln, pending["ln"])] += 1
                pending["ln"] = None
            elif ln in reasons:
                pending["ln"] = ln
        return line_tracer

    def tracer(frame, event, arg):
        return line_tracer if event == "call" and frame.f_code is code else None

    sys.settrace(tracer)
    try:
        recs = util.calculate_order_quantity(copy.deepcopy(enriched),
                                             use_real_date=True)
    finally:
        sys.settrace(None)

    traced = sum(by_exit.values())
    print("EXIT                                                     lines"
          "\n" + "-" * 70)
    for ln in sorted(exits):
        n = by_exit.get(ln, 0)
        print(f"  line {ln:<5} {_label(exits[ln], 40):<40}{n:>9,}")
        for (e, r), m in sorted(by_pair.items(), key=lambda kv: -kv[1]):
            if e == ln and r is not None:
                print(f"        |- {_label(reasons[r], 50):<50}{m:>9,}")
        orphan = by_pair.get((ln, None), 0)
        if orphan:
            print(f"        |- {'(no reasoning written on this path)':<50}"
                  f"{orphan:>9,}")

    ordered = sum(1 for r in recs
                  if float(r.get("recommended_quantity") or 0) > 0)
    print("-" * 70)
    print(f"  {'traced exits':<48}{traced:>9,}")
    print(f"  {'SKUs in':<48}{len(enriched):,}")
    print(f"  {'of which reached a quantity > 0':<48}{ordered:>9,}")

    # THE CLOSURE TEST. Each SKU appends exactly once, so these lines partition
    # the population. A shortfall is an uninstrumented branch, not a curiosity.
    if traced != len(enriched):
        raise SystemExit(
            f"\nFUNNEL DOES NOT CLOSE: traced {traced:,} exits for "
            f"{len(enriched):,} SKUs ({len(enriched) - traced:+,}). Every SKU "
            f"reaches exactly one {EXIT_MARK!r}; a gap means the loop can "
            f"leave by a path this probe does not see, and every count above "
            f"is a floor rather than a figure. Do not publish these numbers.")
    print(f"\n  funnel closes: {traced:,} exits == {len(enriched):,} SKUs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
