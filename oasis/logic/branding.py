"""
Whitelabel branding — tenant name / logo / accent colors from a config file.

    branding.json lives next to the app root. Everything the client sees —
    console sidebar badge, Home launcher header, page tabs, PDF report
    headers — reads from :func:`load_branding` so a single edit re-skins the
    whole suite. Absent file → default OASIS branding (backwards compatible).

Config schema (all fields optional):
    {
        "tenant_name":     "Client Name",
        "product_name":    "OASIS",
        "logo":            "logo.png"            (path relative to app root)
        "logo_data_uri":   "data:image/png;..."  (embedded — alternative to logo)
        "primary_color":   "#00E5C4",
        "accent_color":    "#00B89E",
        "welcome_message": "Welcome, team."
    }
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from typing import Optional


DEFAULTS = {
    "tenant_name": "O.A.S.I.S.",
    "product_name": "O.A.S.I.S.",
    "logo": None,
    "logo_data_uri": None,
    "primary_color": "#00E5C4",
    "accent_color": "#00B89E",
    "welcome_message": "",
    "pdf_footer": "",
}


@dataclass(frozen=True)
class Branding:
    tenant_name: str = DEFAULTS["tenant_name"]
    product_name: str = DEFAULTS["product_name"]
    logo: Optional[str] = None
    logo_data_uri: Optional[str] = None
    primary_color: str = DEFAULTS["primary_color"]
    accent_color: str = DEFAULTS["accent_color"]
    welcome_message: str = ""
    pdf_footer: str = ""
    #: absolute path to the source config file (None when defaults are used)
    source: Optional[str] = field(default=None, repr=False)

    def display_title(self) -> str:
        if self.tenant_name and self.tenant_name != DEFAULTS["tenant_name"]:
            return f"{self.product_name} — {self.tenant_name}"
        return self.product_name


def branding_path(root: Optional[str] = None) -> str:
    """Where the tenant reads/writes their branding.json."""
    if root is None:
        root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return os.getenv("OASIS_BRANDING", os.path.join(root, "branding.json"))


def _is_valid_hex(v: str) -> bool:
    return isinstance(v, str) and len(v) in (4, 7) and v.startswith("#")


def load_branding(path: Optional[str] = None) -> Branding:
    """Load branding config; falls back to defaults on missing/invalid file.

    Only the fields defined in Branding are consumed; extras are ignored (safe
    for forward compatibility). Color fields are validated as #rgb / #rrggbb.
    """
    p = path or branding_path()
    data = dict(DEFAULTS)
    src = None
    if os.path.exists(p):
        try:
            with open(p, "r", encoding="utf-8") as f:
                raw = json.load(f) or {}
            for k, v in raw.items():
                if k in DEFAULTS:
                    data[k] = v
            src = p
        except (OSError, ValueError):
            pass
    for c in ("primary_color", "accent_color"):
        if not _is_valid_hex(data[c]):
            data[c] = DEFAULTS[c]
    return Branding(source=src, **{k: data[k] for k in DEFAULTS})


def save_branding(branding: Branding, path: Optional[str] = None) -> str:
    """Write a Branding out to disk (used by the future settings page)."""
    p = path or branding_path()
    payload = {k: v for k, v in asdict(branding).items()
               if k in DEFAULTS and v is not None}
    os.makedirs(os.path.dirname(os.path.abspath(p)) or ".", exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    return p
