"""Gross margin from the GRN book, on ONE tax basis.

WHAT WENT WRONG THREE TIMES
    1. Net Amt / Qty as "unit cost" -- Net Amt is VAT-inclusive. 79.8% negative.
    2. Scorecard Unit_Price -- a 108.20 placeholder on 57.6% of lines.
    3. `Cost Price` against raw `SP` -- cost is VAT-EXCLUSIVE, SP is
       VAT-INCLUSIVE. Median came out 35.3% against a true 25.0%.

    The third had a natural experiment sitting in the data and I missed it:
    zero-rated lines, where inclusive and exclusive are the same number, sit at
    exactly 25.00%. Dividing the vatable lines by their own tax rate lands them
    on 25.00% too, gap 0.00. The store prices to a 25% gross margin; the extra
    10.34 points was VAT counted as profit.

SO: the tax rate is taken PER ROW from the book itself --

    rate = Total tax Amt / Taxable Amt        (0.16 vatable, 0.00 zero-rated)
    SP_ex = SP / (1 + rate)
    margin = (SP_ex - Cost Price) / SP_ex

no constant, no assumption about which lines are vatable.

ALSO HANDLED
    * every grnd*/grnds* workbook, not a hand-picked subset
    * `grnd_1_1.5.xlsx` sits beside `grnds_1_1.5.xlsx` -- a one-character name
      twin. Rows are keyed and de-duplicated rather than concatenated.
    * spreadsheet footer rows (Org/Vendor/Item == "Total")
    * the span is 15 months (Jan 2025 - Mar 2026), so Jan/Feb/Mar appear twice.
      Prices are summarised per SKU with the spread reported, and the LATEST
      observed price is carried as well as the median, because a median over a
      period with a price change is wrong for part of it.

`derive()` is the entry point callers should use. It returns the margin table
in memory. The json on disk is a CACHE, not the source of truth: AMIT calls
derive() and only reads the file when it is newer than every workbook feeding
it. Nothing downstream is allowed to depend on the file existing.
"""
from __future__ import annotations

import argparse, glob, json, os, re, statistics as st, sys, datetime as dt
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit.methodology.traps import ratio, run_all      # noqa: E402

SUB = re.compile(r"^\s*(grand\s*)?(sub\s*)?total\s*$", re.I)
PATTERN = "oasis/data/*grnd*.xlsx"
OUT = ROOT / "oasis" / "data" / "margin_from_grn.json"
NEED = ["Item Name", "GRN Qty", "Cost Price", "SP", "Taxable Amt",
        "Total tax Amt", "GRN No", "Bar Code", "Vendor Code - Name", "GRN Date"]


def norm(x): return " ".join(str(x).upper().split())


def workbooks(pattern: str = PATTERN):
    return sorted(glob.glob(str(ROOT / pattern)))


def derive(pattern: str = PATTERN, quiet: bool = True):
    """Build the margin table from the GRN workbooks. No file is read or written."""
    import openpyxl
    files = workbooks(pattern)
    seen = set()
    per = defaultdict(lambda: {"obs": [], "qty": 0.0, "cost_sum": 0.0,
                               "bar": None, "vendor": None, "dept": None})
    stats = defaultdict(int)
    for f in files:
        wb = openpyxl.load_workbook(f, read_only=True, data_only=True)
        ws = wb[wb.sheetnames[0]]; it = ws.iter_rows(values_only=True)
        hdr = [str(h).strip() if h else "" for h in next(it)]
        ix = {h: i for i, h in enumerate(hdr)}
        if not all(k in ix for k in NEED):
            stats["file_missing_cols"] += 1; wb.close(); continue
        dcol = ix.get("Department")
        for r in it:
            if len(r) <= max(ix[k] for k in NEED): stats["short"] += 1; continue
            item = str(r[ix["Item Name"]] or "")
            if SUB.match(item) or SUB.match(str(r[ix["Vendor Code - Name"]] or "")):
                stats["footer"] += 1; continue
            key = (str(r[ix["GRN No"]]), str(r[ix["Bar Code"]]), norm(item),
                   str(r[ix["GRN Qty"]]), str(r[ix["Cost Price"]]))
            if key in seen: stats["dupe"] += 1; continue
            seen.add(key)
            def num(k):
                try: return float(r[ix[k]])
                except (TypeError, ValueError): return None
            q, cp, sp = num("GRN Qty"), num("Cost Price"), num("SP")
            tax, taxable = num("Total tax Amt"), num("Taxable Amt")
            if not q or not cp or not sp or q <= 0 or cp <= 0 or sp <= 0:
                stats["incomplete"] += 1; continue
            rate = (tax / taxable) if (tax is not None and taxable) else 0.0
            if rate < 0 or rate > 0.5: rate = 0.0
            d = r[ix["GRN Date"]]
            try:
                dd = d.date() if isinstance(d, dt.datetime) else dt.datetime.strptime(str(d), "%d-%b-%Y").date()
            except Exception:
                dd = None
            k = norm(item); p = per[k]
            p["obs"].append({"sp_ex": sp / (1.0 + rate), "sp_inc": sp,
                             "cost": cp, "rate": rate, "date": dd})
            p["qty"] += q; p["cost_sum"] += cp * q
            p["bar"] = p["bar"] or str(r[ix["Bar Code"]] or "")
            p["vendor"] = p["vendor"] or str(r[ix["Vendor Code - Name"]] or "")
            if dcol is not None and p["dept"] is None and len(r) > dcol:
                p["dept"] = norm(r[dcol] or "") or None
        wb.close()

    out = {}
    for k, p in per.items():
        obs = p["obs"]
        cost = p["cost_sum"] / p["qty"]
        sp_ex = st.median([o["sp_ex"] for o in obs])
        sp_inc = st.median([o["sp_inc"] for o in obs])
        dated = [o for o in obs if o["date"]]
        latest = max(dated, key=lambda o: o["date"])["sp_ex"] if dated else sp_ex
        m = (sp_ex - cost) / sp_ex * 100 if sp_ex else 0.0
        spread = max(o["sp_ex"] for o in obs) - min(o["sp_ex"] for o in obs)
        out[k] = {"unit_cost": round(cost, 4),
                  "selling_price": round(sp_ex, 4),
                  "selling_price_inc_vat": round(sp_inc, 4),
                  "selling_price_latest": round(latest, 4),
                  "tax_rate": round(st.median([o["rate"] for o in obs]), 4),
                  "gross_margin_pct": round(m, 3),
                  "gross_profit_per_unit": round(sp_ex - cost, 4),
                  "grn_qty": round(p["qty"], 2), "lines": len(obs),
                  "sp_spread_pct": round(100 * spread / sp_ex, 2) if sp_ex else 0,
                  "barcode": p["bar"], "vendor": p["vendor"],
                  "department": p["dept"] or "",
                  "basis": "ex_vat", "provenance": "observed"}
    stats["files"] = len(files); stats["rows_kept"] = len(seen)
    return out, dict(stats), files


def load_or_derive(pattern: str = PATTERN, cache: Path = OUT, quiet: bool = True):
    """Margin table plus the rung it came from. The file is never required."""
    files = workbooks(pattern)
    if cache.exists() and files:
        newest = max(os.path.getmtime(f) for f in files)
        if os.path.getmtime(cache) >= newest:
            try:
                return json.loads(cache.read_text(encoding="utf-8")), "cache"
            except (OSError, ValueError):
                pass
    out, _stats, _f = derive(pattern, quiet=quiet)
    if out:
        try:
            cache.write_text(json.dumps(out, indent=1, sort_keys=True), encoding="utf-8")
        except OSError:
            pass
    return out, "derived"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--glob", default=PATTERN)
    ap.add_argument("--write", action="store_true")
    a = ap.parse_args(argv)

    out, stats, files = derive(a.glob, quiet=False)
    print(f"  {len(files)} workbooks")
    print(f"  rows kept {stats.get('rows_kept',0):,} · dropped: "
          f"{ {k:v for k,v in stats.items() if k not in ('files','rows_kept')} }")
    if not out:
        print("  no rows -- check --glob; nothing written"); return 2

    mar_ex = sorted(v["gross_margin_pct"] for v in out.values())
    mar_inc = sorted(((v["selling_price_inc_vat"] - v["unit_cost"]) /
                      v["selling_price_inc_vat"] * 100) if v["selling_price_inc_vat"] else 0
                     for v in out.values())
    q = lambda v, x: v[int(x * len(v))]
    zero = [v["gross_margin_pct"] for v in out.values() if v["tax_rate"] < 0.01]
    vat = [v["gross_margin_pct"] for v in out.values() if v["tax_rate"] >= 0.01]
    print(f"\n  SKUs {len(out):,}")
    print(f"  margin EX-VAT (correct)  p25 {q(mar_ex,.25):.1f} · median {q(mar_ex,.5):.2f} · p75 {q(mar_ex,.75):.1f}")
    print(f"  margin as shipped before p25 {q(mar_inc,.25):.1f} · median {q(mar_inc,.5):.2f} · p75 {q(mar_inc,.75):.1f}")
    print(f"  zero-rated SKUs median {st.median(zero):.2f}% (n={len(zero):,}) · "
          f"vatable median {st.median(vat):.2f}% (n={len(vat):,}) · gap {abs(st.median(zero)-st.median(vat)):.2f} pts")
    print(f"  negative margin: {sum(1 for m in mar_ex if m<0):,}")
    print(f"  SKUs whose price moved >20%: {sum(1 for v in out.values() if v['sp_spread_pct']>20):,}")
    run_all([ratio(st.median(vat), max(st.median(zero), 1e-9),
                   "vatable vs zero-rated median margin", lo=0.9, hi=1.1)])
    if a.write:
        OUT.write_text(json.dumps(out, indent=1, sort_keys=True), encoding="utf-8")
        print(f"  wrote {OUT.relative_to(ROOT)} (cache)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
