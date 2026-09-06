from __future__ import annotations
import sys, json
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit.engine_ablation import build, simulate

H = 0.1438
OBS_DAYS = 315
keys, f = build()

CONFIGS = {
 "none":       dict(amit=0, lata=0, dharam=0, mande=0, shelf=0),
 "AMIT only":  dict(amit=1, lata=0, dharam=0, mande=0, shelf=0),
 "LATA only":  dict(amit=0, lata=1, dharam=0, mande=0, shelf=0),
 "MANDE only (enforcing)": dict(amit=0, lata=0, dharam=0, mande=1, shelf=0),
 "shelf-life clamp only":  dict(amit=0, lata=0, dharam=0, mande=0, shelf=1),
 "ALL ON (MANDE enforcing)": dict(amit=1, lata=1, dharam=1, mande=1, shelf=1),
 "ALL ON (MANDE reporting)": dict(amit=1, lata=1, dharam=1, mande=0, shelf=1),
 "RECOMMENDED": dict(amit=0, lata=1, dharam=0, mande=0, shelf=1),
}

def true_metrics(name, cfg):
    m = simulate(f, cfg, 12, real_open=True, warm=0, cv_mode="poisson",
                 demand_law="poisson", per_sku=True)
    sku = m["per_sku"]
    sold = sku["sold"]; waste = sku["waste"]
    true_gp = float((sold * f["gp"]).sum()) / OBS_DAYS * 365.0
    true_waste_year = float(waste.sum()) / OBS_DAYS * 365.0
    stock = m["avg_stock_kes"]
    ep = true_gp - H * stock - true_waste_year
    print(f"{name:<28} true_GP={true_gp:>13,.0f} engine_GP={m['gp_realised_year']:>13,.0f}"
          f" stock={stock:>12,.0f} true_waste={true_waste_year:>12,.0f} true_EP={ep:>13,.0f}"
          f" svc={m['service']:.4f}", flush=True)
    return dict(true_gp=true_gp, engine_gp=m['gp_realised_year'], stock=stock,
                true_waste=true_waste_year, true_ep=ep, svc=m['service'])

if __name__ == "__main__":
    names = sys.argv[1:]
    out = {}
    for n in names:
        out[n] = true_metrics(n, CONFIGS[n])
    p = ROOT / "devkit" / "probe_truegp_batch_out.json"
    prev = json.loads(p.read_text()) if p.exists() else {}
    prev.update(out)
    p.write_text(json.dumps(prev, indent=1))
