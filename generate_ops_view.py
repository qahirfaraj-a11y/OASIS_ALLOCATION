import re

with open("oasis/desktop/views/ops_view.py", "r", encoding="utf-8") as f:
    content = f.read()

# 1. Add eod_stock, roi, and transfer logic
new_imports = """
    eod = D.eod_stock_heuristic(org, project_root)
    roi = D.executive_roi(org, project_root)
"""
content = content.replace('pend = D.pending_orders(org, project_root)\n', 'pend = D.pending_orders(org, project_root)\n' + new_imports)

# 2. Add EOD Stock UI
eod_ui = """
    # ── Stock (EOD) ──────────────────────────────────────────────────────
    eod_content = []
    if eod.get("error"):
        eod_content.append(_error_row(f"Failed to read stock heuristic: {eod['error']}"))
    else:
        eod_content.append(ft.Text("Items with < 3 days of cover (ADS Heuristic)", size=12, color=T.TEXT_MUTED))
        rows = [[i["Severity"], i["Product"], i["Dept"], str(i["Stock"]), str(i["ADS"]), i["Cover"]] for i in eod.get("items", [])]
        eod_content.append(_table(["Severity", "Product", "Dept", "Stock", "ADS", "Cover"], rows, "All items have healthy cover (>3 days)."))
"""

# 3. Add ROI Overview UI
roi_ui = """
    # ── ROI Overview ─────────────────────────────────────────────────────
    roi_content = []
    if roi.get("error"):
        roi_content.append(_error_row(f"Failed to read ROI metrics: {roi['error']}"))
    else:
        _dead_c = T.SUCCESS if roi['dead_pct'] < 5 else T.DANGER
        _so_c = T.SUCCESS if roi['so_pct'] < 2 else T.DANGER
        roi_content = [
            ft.Row([
                T.metric_card("Active SKUs", f"{roi['total_skus']:,}", status="info"),
                T.metric_card("Dead Stock", f"{roi['dead_pct']}%", status="success" if roi['dead_pct']<5 else "danger", sub="<5% target"),
                T.metric_card("Stockout", f"{roi['so_pct']}%", status="success" if roi['so_pct']<2 else "danger", sub="<2% target"),
                T.metric_card("Recoverable", _money(roi["trapped"]), status="warning", sub="trapped capital"),
            ], spacing=12, expand=True)
        ]
"""

# 4. Replace Transfers UI
trans_ui = """
    # ── Transfers ────────────────────────────────────────────────────────
    if not _ok("transfers"):
        transfers = [build_upsell(TAB_MODULES["transfers"])]
    elif multi:
        ti = D.transfer_intelligence(project_root)
        if ti.get("error"):
            transfers = [_error_row(f"Intelligence failure: {ti['error']}")]
        else:
            risks = ti.get("risks", [])
            recs = ti.get("recs", [])
            
            risk_cards = [
                T.metric_card(f"{r['store_id']}", f"{r['risk']:.2f}", 
                              status="danger" if r["risk"] > 0.7 else ("warning" if r["risk"] > 0.4 else "success"))
                for r in risks
            ]
            
            transfers = [
                T.section_header("Network Risk Status", "🧠"),
                ft.Row(risk_cards, spacing=12, expand=True),
                ft.Container(height=10),
                T.section_header("Transfer Proposals (ST-GAT)", "📦"),
            ]
            if recs:
                rows = [[r["From"], r["To"], r["Score"], r["Priority Index"]] for r in recs]
                transfers.append(_table(["From", "To", "Score", "Priority"], rows, ""))
            else:
                transfers.append(ft.Text("No viable transfers found.", size=12, color=T.TEXT_MUTED))
    else:
        transfers = [_not_migrated(
            "Transfers need more than one store",
            "This install has a single store, so there is nothing to transfer "
            "between. Multi-store installs get donor/receiver proposals here.",
            "Home → first-run setup → multi-store demo network")]
"""

content = content.replace('    # ── Transfers ────────────────────────────────────────────────────────', eod_ui + roi_ui + '    # ── Transfers ────────────────────────────────────────────────────────')

content = re.sub(r'    # ── Transfers ────────────────────────────────────────────────────────.*?    # ── Suppliers ────────────────────────────────────────────────────────', trans_ui + '\n    # ── Suppliers ────────────────────────────────────────────────────────', content, flags=re.DOTALL)

# 5. Add to tabs
new_tabs = """
                        _tab("ROI", ft.Icons.ATTACH_MONEY, roi_content),
                        _tab("Stock (EOD)", ft.Icons.INVENTORY, eod_content),
"""
content = content.replace('_tab("Ordering", ft.Icons.INVENTORY_2_OUTLINED, ordering),', '_tab("Ordering", ft.Icons.INVENTORY_2_OUTLINED, ordering),\n' + new_tabs)

with open("oasis/desktop/views/ops_view.py", "w", encoding="utf-8") as f:
    f.write(content)
