"""
Markdown → PDF for OASIS reports (pure-Python via fpdf2, no system deps).

Renders the light Markdown our report writers produce (H1/H2, GFM tables,
paragraphs, bold) into a clean, branded A4 PDF. Not a general Markdown engine —
just the subset the reports use, kept deliberately small and dependency-light so
it runs on any client Windows box.
"""

from __future__ import annotations

import re
from typing import List

_BOLD = re.compile(r"\*\*(.+?)\*\*")


def _clean(text: str) -> str:
    text = _BOLD.sub(r"\1", text)                 # drop bold markers
    return (text.replace("≈", "~").replace("×", "x").replace("→", "->")
            .replace("’", "'").replace("—", "-").replace("·", "-")
            .encode("latin-1", "replace").decode("latin-1"))   # fpdf core fonts = latin-1


def _split_table(lines: List[str], i: int):
    """Collect a GFM table starting at line i; return (headers, rows, next_i)."""
    def cells(row):
        return [c.strip() for c in row.strip().strip("|").split("|")]
    headers = cells(lines[i])
    j = i + 2                                       # skip the |---| separator
    rows = []
    while j < len(lines) and lines[j].lstrip().startswith("|"):
        rows.append(cells(lines[j]))
        j += 1
    return headers, rows, j


def markdown_to_pdf(md: str, out_path: str, title: str = "O.A.S.I.S. Report") -> str:
    from fpdf import FPDF

    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_title(title)
    epw = pdf.w - 2 * pdf.l_margin

    lines = md.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()
        if not stripped:
            pdf.ln(2)
            i += 1
        elif stripped.startswith("# "):
            pdf.set_font("Helvetica", "B", 17)
            pdf.multi_cell(epw, 8, _clean(stripped[2:]))
            pdf.ln(1)
            i += 1
        elif stripped.startswith("## "):
            pdf.ln(2)
            pdf.set_font("Helvetica", "B", 13)
            pdf.set_text_color(20, 60, 100)
            pdf.multi_cell(epw, 7, _clean(stripped[3:]))
            pdf.set_text_color(0, 0, 0)
            pdf.ln(1)
            i += 1
        elif stripped.startswith("|") and i + 1 < len(lines) and set(lines[i + 1].strip()) <= set("|:- "):
            headers, rows, ni = _split_table(lines, i)
            _render_table(pdf, headers, rows, epw)
            i = ni
        elif stripped.startswith("---"):
            pdf.ln(1)
            i += 1
        else:
            italic = stripped.startswith("*") and stripped.endswith("*")
            pdf.set_font("Helvetica", "I" if italic else "", 9 if italic else 10)
            pdf.set_text_color(90, 90, 90) if italic else pdf.set_text_color(0, 0, 0)
            pdf.multi_cell(epw, 5, _clean(stripped.strip("*")))
            pdf.set_text_color(0, 0, 0)
            i += 1

    pdf.output(out_path)
    return out_path


def _render_table(pdf, headers, rows, epw):
    ncol = len(headers)
    if ncol == 0:
        return
    # first column wider (labels/items), rest share the remainder
    first = min(0.42, max(0.2, 0.42 if ncol <= 3 else 0.34))
    widths = [epw * first] + [epw * (1 - first) / (ncol - 1)] * (ncol - 1) if ncol > 1 else [epw]

    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(20, 60, 100)
    pdf.set_text_color(255, 255, 255)
    for w, h in zip(widths, headers):
        pdf.cell(w, 6, _clean(h)[:38], border=0, align="L", fill=True)
    pdf.ln(6)
    pdf.set_text_color(0, 0, 0)
    pdf.set_font("Helvetica", "", 8)
    fill = False
    for row in rows:
        pdf.set_fill_color(240, 244, 248)
        for idx, w in enumerate(widths):
            val = _clean(row[idx]) if idx < len(row) else ""
            align = "L" if idx == 0 else "R"
            pdf.cell(w, 5.2, val[:40], border=0, align=align, fill=fill)
        pdf.ln(5.2)
        fill = not fill
    pdf.ln(2)
