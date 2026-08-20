"""Render a live view of the Odoo depot and the transfer plan it produces.

Reads the RUNNING instance through OdooAdapter — the same path the product
uses — so the page is a snapshot of what OASIS actually sees, not of a fixture.

    python connectors/odoo/build_live_view.py
    # -> connectors/odoo/live_view.html
"""

from __future__ import annotations

import contextlib
import datetime
import io
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, REPO)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(REPO, "devkit"))

from oasis.logic.odoo_adapter import OdooAdapter                      # noqa: E402
from oasis.logic.consolidated_transfer_service import (               # noqa: E402
    ConsolidatedTransferService as CTS)
from verify_store_network import fetch_with_retry                     # noqa: E402


def pull():
    seed = json.load(open(os.path.join(HERE, "store_network_seed.json"),
                          encoding="utf-8"))
    a = OdooAdapter(url=os.getenv("ODOO_URL", "http://localhost:8069"),
                    db=os.getenv("ODOO_DB", "oasis"),
                    user=os.getenv("ODOO_USER", "admin"),
                    password=os.getenv("ODOO_PASSWORD", "admin"))
    health = a.health_check()
    if not health.get("connected"):
        raise SystemExit(f"Odoo unreachable: {health.get('error')}")
    orgs = a.fetch_all_organizations()
    data = {s["code"]: fetch_with_retry(a, s["code"]) for s in seed["stores"]}
    names = {s["code"]: s["name"] for s in seed["stores"]}
    coords = {s["code"]: {"lat": s["latitude"], "lon": s["longitude"]}
              for s in seed["stores"]}

    from oasis.desktop.data import store_db_path
    with contextlib.redirect_stderr(io.StringIO()):
        svc = CTS(org_names=names, stock_data=data, distance_map=coords,
                  data_dir=os.path.join(REPO, "oasis", "data"),
                  settings_db=store_db_path(REPO))
        scan = svc.scan_network_opportunities()
    opps = scan.opportunities

    stores = []
    for s in seed["stores"]:
        c = s["code"]
        prods = data[c]
        stock = sum(float(p["current_stocks"]) for p in prods)
        ads = sum(float(p["avg_daily_sales"]) for p in prods)
        st = scan.store_stats.get(c, {})
        stores.append(dict(
            code=c, name=s["name"], region=s.get("region", ""),
            sqft=float(s.get("floor_area_sqft") or 0),
            stock=stock, ads=ads,
            value=sum(float(p["current_stocks"]) * float(p.get("cost_price") or 0)
                      for p in prods),
            ranged=sum(1 for p in prods if float(p["current_stocks"]) > 0
                       or float(p["avg_daily_sales"]) > 0),
            cover=(stock / ads if ads else 0.0),
            deficits=st.get("deficits", 0), overstock=st.get("overstock", 0),
            out=sum(o.transfer_qty for o in opps if o.from_org == c),
            inb=sum(o.transfer_qty for o in opps if o.to_org == c)))

    pulls = [o for o in opps if o.type == "PULL"]
    pushes = [o for o in opps if o.type == "PUSH"]
    return dict(
        generated=datetime.datetime.now().strftime("%d %b %Y, %H:%M"),
        odoo=dict(url="localhost:8069", db="oasis",
                  latency=health.get("latency_ms") or 0,
                  products=health.get("tables_found") or 0,
                  warehouses=len(orgs)),
        engine=dict(lata=len(svc.supplier_rhythm), relief=svc._median_relief or 0,
                    tiers=len(svc._perishability), rho=svc.RELEASE_FRACTION,
                    dead=svc.DEAD_STOCK_DAYS, maxrelief=svc.MAX_RELIEF_DAYS,
                    cost=svc.transfer_cost_kes, ratio=svc.min_excess_ratio,
                    overrides=svc.settings),
        plan=dict(lines=len(opps), units=sum(o.transfer_qty for o in opps),
                  value=sum(o.value_kes for o in opps),
                  pull=len(pulls), push=len(pushes),
                  fresh=sum(1 for o in opps if o.manual_only)),
        stores=stores,
        top=[dict(t=o.type, sku=o.product_name[:44], frm=o.from_org, to=o.to_org,
                  qty=o.transfer_qty, val=o.value_kes, dept=o.department[:22],
                  fresh=o.manual_only)
             for o in sorted(opps, key=lambda x: -x.value_kes)[:12]])


def band(cover):
    """Cover health, encoded as form as well as number."""
    if cover < 12:
        return "tight"
    if cover > 30:
        return "idle"
    return "ok"


CSS = """
:root{--ground:#F2F5F5;--surface:#FFF;--raise:#FAFCFC;--ink:#0C1A1C;
--muted:#5A6C6F;--line:#D8E0E0;--line-soft:#E8EEEE;--petrol:#0B4F52;
--petrol-ink:#EAF4F4;--marigold:#B87415;--marigold-lift:#D98A1F;
--ok:#1E7A55;--ok-bg:#E4F1EB;--tight:#B14A22;--tight-bg:#F8E9E3;
--idle:#5F4794;--idle-bg:#EDE9F6;}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){
--ground:#0A1416;--surface:#0F1E21;--raise:#132528;--ink:#E4EDEC;
--muted:#8FA3A5;--line:#1F3438;--line-soft:#17292C;--petrol:#0E3437;
--petrol-ink:#CFE8E8;--marigold:#E9A845;--marigold-lift:#F0B95F;
--ok:#5CC08E;--ok-bg:#102C22;--tight:#E08558;--tight-bg:#2E1A13;
--idle:#A38FD6;--idle-bg:#1F1930;}}
:root[data-theme="dark"]{--ground:#0A1416;--surface:#0F1E21;--raise:#132528;
--ink:#E4EDEC;--muted:#8FA3A5;--line:#1F3438;--line-soft:#17292C;
--petrol:#0E3437;--petrol-ink:#CFE8E8;--marigold:#E9A845;--marigold-lift:#F0B95F;
--ok:#5CC08E;--ok-bg:#102C22;--tight:#E08558;--tight-bg:#2E1A13;
--idle:#A38FD6;--idle-bg:#1F1930;}
*{box-sizing:border-box}
body{margin:0;background:var(--ground);color:var(--ink);
font-family:"Source Sans 3",ui-sans-serif,system-ui,sans-serif;font-size:15px;line-height:1.55}
.wrap{max-width:1180px;margin:0 auto;padding:0 20px 72px}
.n,.code,.chip,.lv{font-family:"IBM Plex Mono",ui-monospace,monospace;font-variant-numeric:tabular-nums}
.rail{background:var(--petrol);color:var(--petrol-ink);font-family:"IBM Plex Mono",monospace;
font-size:12px;letter-spacing:.04em;padding:9px 0;margin-bottom:34px}
.rail .wrap{padding-bottom:0;display:flex;flex-wrap:wrap;gap:8px 26px;align-items:center}
.dot{width:7px;height:7px;border-radius:50%;background:var(--ok);display:inline-block;
margin-right:7px;vertical-align:1px}
.rail b{font-weight:600}.rail .sp{margin-left:auto;opacity:.75}
h1{font-family:"Bricolage Grotesque",sans-serif;font-weight:800;
font-size:clamp(30px,4.4vw,46px);line-height:1.04;margin:0 0 6px;
letter-spacing:-.022em;text-wrap:balance}
.sub{color:var(--muted);margin:0 0 34px;max-width:64ch}
.thesis{display:grid;grid-template-columns:auto 1fr;gap:28px 40px;align-items:end;
padding:26px 28px;border-radius:3px;background:var(--surface);border:1px solid var(--line);
border-left:4px solid var(--marigold-lift)}
.big{font-family:"Bricolage Grotesque",sans-serif;font-weight:800;
font-size:clamp(52px,8vw,86px);line-height:.86;color:var(--marigold);
letter-spacing:-.03em;font-variant-numeric:tabular-nums}
.big small{display:block;font-family:"Source Sans 3",sans-serif;font-size:13px;
font-weight:600;letter-spacing:.09em;text-transform:uppercase;color:var(--muted);margin-top:12px}
.split{display:flex;flex-wrap:wrap;gap:30px}.split div{min-width:104px}
.split .v{font-family:"IBM Plex Mono",monospace;font-size:22px;font-weight:600;
font-variant-numeric:tabular-nums}
.split .k{font-size:11px;letter-spacing:.09em;text-transform:uppercase;color:var(--muted)}
h2{font-family:"Bricolage Grotesque",sans-serif;font-weight:600;font-size:12px;
letter-spacing:.14em;text-transform:uppercase;color:var(--muted);margin:42px 0 12px}
.scroll{overflow-x:auto;border:1px solid var(--line);border-radius:3px;background:var(--surface)}
table{border-collapse:collapse;width:100%;font-size:13.5px}
th{text-align:right;font-weight:600;font-size:10.5px;letter-spacing:.1em;text-transform:uppercase;
color:var(--muted);padding:11px 12px;border-bottom:1px solid var(--line);
white-space:nowrap;background:var(--raise)}
th:nth-child(1),th:nth-child(2){text-align:left}
td{padding:9px 12px;border-bottom:1px solid var(--line-soft);white-space:nowrap}
td.n{text-align:right;font-family:"IBM Plex Mono",monospace;font-variant-numeric:tabular-nums}
tr:last-child td{border-bottom:none}
.code{font-size:12.5px;font-weight:600;position:relative;padding-left:20px}
.stripe{position:absolute;left:0;top:50%;transform:translateY(-50%);width:3px;height:20px;border-radius:2px}
.stripe.ok{background:var(--ok)}.stripe.tight{background:var(--tight)}.stripe.idle{background:var(--idle)}
.nm{white-space:normal;min-width:210px}
.nm em{display:block;font-style:normal;font-size:11.5px;color:var(--muted)}
.chip{display:inline-block;padding:2px 8px;border-radius:2px;font-size:12px;font-weight:500}
.chip.ok{background:var(--ok-bg);color:var(--ok)}
.chip.tight{background:var(--tight-bg);color:var(--tight)}
.chip.idle{background:var(--idle-bg);color:var(--idle)}
td.def{color:var(--tight)}td.idl{color:var(--idle)}td.out{color:var(--muted)}td.inb{color:var(--ok)}
.tag{font-family:"IBM Plex Mono",monospace;font-size:10.5px;font-weight:600;
padding:2px 7px;border-radius:2px;letter-spacing:.06em}
.tag.pull{background:var(--ok-bg);color:var(--ok)}
.tag.push{background:var(--idle-bg);color:var(--idle)}
.fr{font-size:9.5px;letter-spacing:.09em;margin-left:7px;color:var(--tight);
border:1px solid var(--tight);padding:0 4px;border-radius:2px;vertical-align:1px}
.levers{list-style:none;margin:0;padding:0;display:grid;
grid-template-columns:repeat(auto-fit,minmax(212px,1fr));gap:1px;
background:var(--line);border:1px solid var(--line);border-radius:3px}
.levers li{background:var(--surface);padding:15px 16px;display:flex;flex-direction:column;gap:3px}
.lk{font-size:11px;letter-spacing:.07em;text-transform:uppercase;color:var(--muted)}
.lv{font-size:20px;font-weight:600}
.src{font-size:10.5px;letter-spacing:.08em;text-transform:uppercase}
.src.der{color:var(--ok)}.src.set{color:var(--marigold)}
.foot{margin-top:38px;padding-top:18px;border-top:1px solid var(--line);
font-size:13px;color:var(--muted);max-width:74ch}
.foot code{font-family:"IBM Plex Mono",monospace;font-size:12px}
@media (max-width:720px){.thesis{grid-template-columns:1fr;align-items:start}}
"""


def render(d):
    o, p, e = d["odoo"], d["plan"], d["engine"]
    rows = []
    for x in sorted(d["stores"], key=lambda v: -v["value"]):
        b = band(x["cover"])
        rows.append(
            f'<tr><td class="code"><span class="stripe {b}"></span>{x["code"]}</td>'
            f'<td class="nm">{x["name"].replace("Chandarana ", "")}'
            f'<em>{x["region"]}</em></td>'
            f'<td class="n">{x["sqft"]:,.0f}</td><td class="n">{x["ranged"]:,}</td>'
            f'<td class="n">{x["value"]:,.0f}</td>'
            f'<td class="n"><span class="chip {b}">{x["cover"]:.1f}d</span></td>'
            f'<td class="n def">{x["deficits"]:,}</td>'
            f'<td class="n idl">{x["overstock"]:,}</td>'
            f'<td class="n out">-{x["out"]:,.0f}</td>'
            f'<td class="n inb">+{x["inb"]:,.0f}</td></tr>')

    tops = []
    for t in d["top"]:
        fresh = '<b class="fr">FRESH</b>' if t["fresh"] else ""
        kind = "push" if t["t"] == "PUSH" else "pull"
        tops.append(
            f'<tr><td><span class="tag {kind}">{t["t"]}</span></td>'
            f'<td class="nm">{t["sku"]}{fresh}<em>{t["dept"]}</em></td>'
            f'<td class="n">{t["frm"]} &rarr; {t["to"]}</td>'
            f'<td class="n">{t["qty"]:,.0f}</td>'
            f'<td class="n">{t["val"]:,.0f}</td></tr>')

    ov = e["overrides"] or {}
    levers = [("Release fraction", f'{e["rho"]}', "release_fraction"),
              ("Dead-stock window", f'{e["dead"]} d', "dead_stock_days"),
              ("Max relief horizon", f'{e["maxrelief"]:.0f} d', "max_relief_days"),
              ("Transfer cost", f'KES {e["cost"]:,.0f}', "max_transfer_cost_kes"),
              ("Donor eligibility", f'{e["ratio"]}x', "min_excess_ratio")]
    lev = "".join(
        f'<li><span class="lk">{lab}</span><span class="lv">{val}</span>'
        f'<span class="src {"set" if k in ov else "der"}">'
        f'{"set by operator" if k in ov else "derived"}</span></li>'
        for lab, val, k in levers)

    return f"""<title>Rhapta Depot Live</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600;12..96,800&family=IBM+Plex+Mono:wght@400;500;600&family=Source+Sans+3:wght@400;600&display=swap">
<style>{CSS}</style>
<div class="rail"><div class="wrap">
<span><span class="dot"></span><b>ODOO {o['url']}</b> &middot; db {o['db']}</span>
<span>{o['warehouses']} warehouses</span><span>{o['products']:,} products</span>
<span>{o['latency']:.0f} ms</span>
<span class="sp">read via OdooAdapter &middot; XML-RPC &middot; {d['generated']}</span>
</div></div>
<div class="wrap">
<h1>Rhapta Depot, live</h1>
<p class="sub">Fourteen Chandarana outlets in a running Odoo 16 instance, read through the
same adapter the product uses, with the transfer plan OASIS computes off them right now.</p>
<div class="thesis">
<div class="big">{p['lines']:,}<small>movements proposed</small></div>
<div class="split">
<div><div class="v">{p['units']:,.0f}</div><div class="k">units</div></div>
<div><div class="v">{p['value']:,.0f}</div><div class="k">KES at retail</div></div>
<div><div class="v">{p['pull']:,}</div><div class="k">pull &middot; plug gaps</div></div>
<div><div class="v">{p['push']:,}</div><div class="k">push &middot; clear idle</div></div>
<div><div class="v">{p['fresh']:,}</div><div class="k">fresh &middot; manual only</div></div>
</div></div>
<h2>The network &middot; sorted by stock at cost</h2>
<div class="scroll"><table>
<thead><tr><th>Store</th><th>Outlet</th><th>Sq ft</th><th>SKUs</th><th>Stock (KES)</th>
<th>Cover</th><th>Short</th><th>Idle</th><th>Out</th><th>In</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table></div>
<h2>Largest movements by value</h2>
<div class="scroll"><table>
<thead><tr><th>Kind</th><th>Line</th><th>Route</th><th>Units</th><th>KES</th></tr></thead>
<tbody>{''.join(tops)}</tbody></table></div>
<h2>Levers &middot; Settings &rarr; System Configuration</h2>
<ul class="levers">{lev}</ul>
<p class="foot">Horizons are <b>derived, not declared</b>: relief comes from LATA's measured
delivery gaps across <b>{e['lata']} suppliers</b> (network median <b>{e['relief']:.0f} days</b>),
and category thresholds from AMIT's <b>{e['tiers']} perishability tiers</b> &mdash; so bakery is
judged at 5 days where cereals get 60. Every lever above is an override with a derived
default: left alone the engine derives, set it and the scan says so in the log.
Specification: <code>OASIS_Master_Transfer_Formulae.md</code>.</p>
</div>"""


if __name__ == "__main__":
    d = pull()
    out = os.path.join(HERE, "live_view.html")
    io.open(out, "w", encoding="utf-8").write(render(d))
    print(f"wrote {os.path.relpath(out, REPO)}")
    print(f"  {d['odoo']['warehouses']} warehouses, {d['odoo']['products']:,} products, "
          f"{d['odoo']['latency']:.0f}ms")
    print(f"  plan: {d['plan']['lines']:,} lines / {d['plan']['units']:,.0f} units "
          f"/ KES {d['plan']['value']:,.0f}")
