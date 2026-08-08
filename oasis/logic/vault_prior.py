"""
Vault coarse-prior extractor — a cold-start basket halo from the Obsidian vault.

The vault has no SKU-level co-purchase, but its Supplier nodes carry weighted
``[complimentary]:: [[PARTNER]] (Weight: N)`` affinity edges (derived from the real
sample GRN/supply data). We parse those and project them down to a DEPARTMENT-level
halo prior — {dept: {complementary_dept: weight}} — via each vendor's primary
department. That gives baskets *some* real, weighted structure at cold-start,
before live POS co-purchase exists (which then supersedes it with SKU-level
confidence/lift, per Kenyan_Retail_Bible Ch. 8).

Pure parsers (parse_supplier_complimentary / project_to_departments) are unit
tested; build_department_prior is the vault+catalog integration.
"""

from __future__ import annotations

import glob
import json
import os
import re
from collections import defaultdict
from typing import Dict

_COMPL = re.compile(r"\[complimentary\]::\s*\[\[([^\]]+)\]\]\s*(?:\(Weight:\s*([0-9.]+)\))?")


def _norm(s: str) -> str:
    return str(s).strip().upper()


def parse_supplier_complimentary(vault_dir: str) -> Dict[str, Dict[str, float]]:
    """{supplier -> {partner -> weight}} from Nodes/Suppliers/*.md."""
    sup_dir = os.path.join(vault_dir, "Nodes", "Suppliers")
    out: Dict[str, Dict[str, float]] = {}
    for path in glob.glob(os.path.join(sup_dir, "*.md")):
        supplier = _norm(os.path.splitext(os.path.basename(path))[0])
        try:
            with open(path, "r", encoding="utf-8") as f:
                text = f.read()
        except OSError:
            continue
        partners: Dict[str, float] = {}
        for m in _COMPL.finditer(text):
            partner = _norm(m.group(1))
            if not partner or partner == "UNKNOWN":
                continue
            weight = float(m.group(2)) if m.group(2) else 1.0
            partners[partner] = partners.get(partner, 0.0) + weight
        if partners:
            out[supplier] = partners
    return out


def project_to_departments(sup_edges: Dict[str, Dict[str, float]],
                           vendor_dept: Dict[str, str]) -> Dict[str, Dict[str, float]]:
    """Project supplier↔supplier affinity onto departments via vendor's primary
    dept. Undirected at department level (weights aggregate both ways)."""
    prior: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for sup, partners in sup_edges.items():
        da = vendor_dept.get(sup)
        if not da:
            continue
        for partner, w in partners.items():
            db = vendor_dept.get(partner)
            if not db or db == da:
                continue
            prior[da][db] += w
            prior[db][da] += w
    return {d: dict(v) for d, v in prior.items()}


def load_prior(path: str) -> Dict[str, Dict[str, float]]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_department_prior(vault_dir: str, data_dir: str, out_path: str) -> dict:
    """Parse vault supplier affinity, project to departments using the real
    catalog vendor->dept map, write basket_prior.json."""
    from .catalog_snapshot import load_catalog, vendor_departments
    rows = load_catalog(data_dir)
    vendor_dept = {_norm(v): d for v, d in vendor_departments(rows).items()}
    sup_edges = parse_supplier_complimentary(vault_dir)
    prior = project_to_departments(sup_edges, vendor_dept)

    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(prior, f, indent=2)
    return {
        "suppliers_with_affinity": len(sup_edges),
        "departments_with_prior": len(prior),
        "dept_pairs": sum(len(v) for v in prior.values()),
        "out_path": out_path,
    }
