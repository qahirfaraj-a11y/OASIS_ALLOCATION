"""Re-key the sales forecast file from product NAME to item code / barcode.

WHY
    The ordering methodology matches a SKU by item code, then barcode, then
    name. find_best_match implements exactly that -- and against this file it
    can never reach past the name, because the file has no codes to match on.

    Measured on the live store: sales_forecasting holds 24,004 records whose
    fields are avg_daily_sales, avg_monthly_sales, monthly_sales,
    months_active, total_10mo_sales, trend, trend_pct. No item code. No
    barcode. Not one record carries a barcode field. Its keys are product
    names, and the "codes" the matcher sees are the first whitespace token of
    those names -- 2,847 of them, overlapping the catalogue's 39,728 item
    codes on ELEVEN entries (0.03%), seven of which resolve to the wrong
    product:

        Shopping Trolley Bag Red   code=RED -> RED BULL 250ML CAN   8.592/day
        Pilferage Recovery         code=P   -> P & M YS 601 RICE SPOON
        Bonus Card Payment         code=BC  -> BC 360G PANCAKE MIX

    So the first two branches of the stated precedence are dead weight and the
    join is name-only in practice. This rebuilds the file against the identity
    the catalogue actually carries on 100% of lines.

WHAT IT WILL NOT DO
    Guess. A forecast name that resolves to two different item codes is
    reported as ambiguous and DROPPED, not assigned to whichever came first --
    an ambiguous demand history silently attached to one of two SKUs is worse
    than no history, because nothing downstream can tell it happened.

    The output keeps product_name and match_method on every record, so any
    line can be traced back to the row it came from.

USAGE
    python devkit/rekey_forecast_by_barcode.py [--db PATH] [--out PATH] [--write]

    Without --write it reports the join and changes nothing.
"""
import json
import os
import sqlite3
import sys
from collections import Counter, defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

DATA = os.path.join(ROOT, "oasis", "data")
DEFAULT_DB = os.path.join(DATA, "rhapta_pos.db")
DEFAULT_OUT = os.path.join(DATA, "sales_forecasting_by_barcode.json")


def _arg(argv, flag, default=None):
    return argv[argv.index(flag) + 1] if flag in argv else default


def main(argv) -> int:
    db_path = _arg(argv, "--db", DEFAULT_DB)
    out_path = _arg(argv, "--out", DEFAULT_OUT)
    write = "--write" in argv

    from oasis.logic.order_engine import OrderEngine, pick_intelligence_file

    src_name = pick_intelligence_file(
        DATA, "sales_forecasting_2025", os.listdir(DATA))
    if not src_name:
        print("no sales_forecasting_2025* file found in oasis/data")
        return 1
    src_path = os.path.join(DATA, src_name)
    with open(src_path, "r", encoding="utf-8") as f:
        forecast = json.load(f)
    print(f"source   : {src_name}  ({len(forecast):,} records, keyed by name)")

    if not os.path.exists(db_path):
        print(f"no catalogue at {db_path}")
        return 1
    con = sqlite3.connect(db_path)
    rows = list(con.execute(
        "SELECT ITM_CD, SCAN_ITM_CD, ITM_LONG_NAME FROM ITEM_MST"))
    con.close()
    print(f"catalogue: {os.path.basename(db_path)}  ({len(rows):,} items)")

    eng = OrderEngine(DATA)
    norm = eng.normalize_product_name

    # normalised name -> set of item codes. A SET, so a name shared by two
    # SKUs is visible as ambiguity instead of collapsing to whichever row the
    # database returned last.
    by_norm = defaultdict(set)
    by_exact = defaultdict(set)
    code_scan = {}
    for itm, scan, name in rows:
        c = str(itm or "").strip()
        if not c:
            continue
        code_scan[c] = str(scan or "").strip()
        nm = str(name or "").strip()
        if nm:
            by_exact[nm].add(c)
            by_norm[norm(nm)].add(c)

    out = {}
    stats = Counter()
    ambiguous = []
    unmatched = []
    for key, rec in forecast.items():
        name = str(key).strip()
        cands = by_exact.get(name) or by_norm.get(norm(name)) or set()
        method = ("exact_name" if by_exact.get(name)
                  else "normalised_name" if by_norm.get(norm(name)) else None)
        if not cands:
            stats["unmatched"] += 1
            if len(unmatched) < 8:
                unmatched.append(name)
            continue
        if len(cands) > 1:
            stats["ambiguous_dropped"] += 1
            if len(ambiguous) < 8:
                ambiguous.append((name, sorted(cands)[:4]))
            continue
        code = next(iter(cands))
        if code in out:
            # Two forecast rows claiming one SKU: also ambiguous, the other way
            # round. Keep neither rather than let ordering decide.
            stats["duplicate_target_dropped"] += 1
            out.pop(code, None)
            continue
        body = dict(rec) if isinstance(rec, dict) else {"value": rec}
        body["product_name"] = name
        body["match_method"] = method
        body["scan_code"] = code_scan.get(code, "")
        out[code] = body
        stats[f"matched_{method}"] += 1

    print("\njoin result")
    for k, v in stats.most_common():
        print(f"  {k:<26}{v:>8,}")
    cov = 100.0 * len(out) / max(1, len(forecast))
    print(f"  {'re-keyed records':<26}{len(out):>8,}  ({cov:.1f}% of source)")

    catalogue_hit = sum(1 for c in code_scan if c in out)
    print(f"\ncatalogue lines that now resolve BY CODE: {catalogue_hit:,} "
          f"of {len(code_scan):,} ({100.0*catalogue_hit/max(1,len(code_scan)):.1f}%)")

    if ambiguous:
        print("\ndropped as ambiguous (one forecast name, several SKUs):")
        for nm, cs in ambiguous:
            print(f"  {nm[:46]:<48} -> {cs}")
    if unmatched:
        print("\nunmatched forecast names (not in this catalogue):")
        for nm in unmatched:
            print(f"  {nm[:66]}")

    if write:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=1)
        print(f"\nwrote {out_path}  ({len(out):,} records keyed by item code)")
        print("source file left untouched.")
    else:
        print("\nDRY RUN — nothing written. Pass --write to emit the file.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
