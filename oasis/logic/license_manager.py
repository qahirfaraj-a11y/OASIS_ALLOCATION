"""
Offline, on-premise licensing for O.A.S.I.S. direct installs.

A license is a JSON key file signed per-module with a salted SHA-256 fingerprint
(tenant + module + expiry + OASIS_LICENSE_SALT). The salt lives only with the
issuer (iLink) — clients receive the key file, not the salt, so keys cannot be
forged or extended on-site. Verification is fully offline.

Without a key the install runs in **evaluation mode** for OASIS_TRIAL_DAYS
(default 14, keyed to a first-run stamp), then locks. With a key, each module is
allowed only if listed, signature-valid, and not expired.

    issue (vendor side, salt required):
        python entrypoint.py --mode issue-license --tenant ACME \
            --modules ops,intel,command --expiry 2027-06-30

    verify (client side): consoles call console_gate() at startup.
"""

import hashlib
import json
import logging
import os
from datetime import date, datetime
from typing import Dict, List, Optional

logger = logging.getLogger("OASIS.LicenseManager")

#: the sellable module SKUs. "core" is the mandatory base every install needs;
#: the rest gate feature groups (pages/tabs/CLI modes) via CAPABILITIES below.
KNOWN_MODULES = ("core", "ordering", "network", "revenue", "api")

MODULE_LABELS = {
    "core": "OASIS Core",
    "ordering": "Smart Ordering",
    "network": "Network (Transfers & Allocation)",
    "revenue": "Revenue Intelligence",
    "api": "Integrations (REST API)",
}

#: sales bundles → module sets (issue-license --bundle <name>)
BUNDLES = {
    "starter": ("core",),
    "pro": ("core", "ordering", "revenue"),
    "enterprise": KNOWN_MODULES,
}

_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _default_key_path() -> str:
    return os.getenv("OASIS_LICENSE_KEY",
                     os.path.join(_ROOT, "oasis_license.key"))


def _default_state_path() -> str:
    return os.path.join(_ROOT, "oasis", "data", ".oasis_install_state.json")


class OfflineLicenseManager:
    """Issue and verify offline license keys; track the evaluation trial."""

    def __init__(self, key_path: Optional[str] = None,
                 state_path: Optional[str] = None):
        self.key_path = key_path or _default_key_path()
        self.state_path = state_path or _default_state_path()
        self._salt = os.getenv("OASIS_LICENSE_SALT", "")

    # ── signing ──────────────────────────────────────────────────────────
    def _fingerprint(self, tenant_id: str, module: str, expiry: str) -> str:
        raw = f"{tenant_id}:{module}:{expiry}:{self._salt}"
        return hashlib.sha256(raw.encode()).hexdigest()

    # ── issuing (vendor side) ────────────────────────────────────────────
    def issue(self, tenant_id: str, modules: List[str], expiry_date: str,
              out_path: Optional[str] = None) -> dict:
        """Create and write a signed key file. Requires OASIS_LICENSE_SALT."""
        if not self._salt:
            raise RuntimeError("OASIS_LICENSE_SALT must be set to issue licenses")
        datetime.strptime(expiry_date, "%Y-%m-%d")   # validate format
        key = {
            "tenant_id": tenant_id,
            "issued": date.today().isoformat(),
            "expiry_date": expiry_date,
            "authorized_modules": {
                m: self._fingerprint(tenant_id, m, expiry_date)
                for m in modules
            },
        }
        path = out_path or self.key_path
        with open(path, "w", encoding="utf-8") as f:
            json.dump(key, f, indent=2)
        logger.info("Issued license for %s (%s) until %s -> %s",
                    tenant_id, ",".join(modules), expiry_date, path)
        return {"tenant_id": tenant_id, "modules": modules,
                "expiry_date": expiry_date, "path": path}

    # ── trial stamp ──────────────────────────────────────────────────────
    def _first_run(self) -> date:
        try:
            with open(self.state_path, "r", encoding="utf-8") as f:
                return date.fromisoformat(json.load(f)["first_run"])
        except Exception:
            first = date.today()
            try:
                os.makedirs(os.path.dirname(self.state_path), exist_ok=True)
                with open(self.state_path, "w", encoding="utf-8") as f:
                    json.dump({"first_run": first.isoformat()}, f)
            except OSError as e:
                logger.warning("Could not persist first-run stamp: %s", e)
            return first

    def _trial_days_left(self) -> int:
        trial_days = int(os.getenv("OASIS_TRIAL_DAYS", "14"))
        used = (date.today() - self._first_run()).days
        return trial_days - used

    # ── verification (client side) ───────────────────────────────────────
    def status(self, module: str) -> dict:
        """Full license status for a module.

        Returns {mode, reason, tenant, expiry, days_left, trial_days_left}
        where mode ∈ licensed | evaluation | locked.
        """
        if not os.path.exists(self.key_path):
            left = self._trial_days_left()
            if left > 0:
                return {"mode": "evaluation", "reason": "no license key",
                        "tenant": None, "expiry": None, "days_left": None,
                        "trial_days_left": left}
            return {"mode": "locked", "reason": "evaluation period ended",
                    "tenant": None, "expiry": None, "days_left": None,
                    "trial_days_left": 0}
        try:
            with open(self.key_path, "r", encoding="utf-8") as f:
                key = json.load(f)
            tenant = key.get("tenant_id", "")
            expiry = key.get("expiry_date", "")
            modules: Dict[str, str] = key.get("authorized_modules", {}) or {}

            if module not in modules:
                return {"mode": "locked", "reason": f"module '{module}' not licensed",
                        "tenant": tenant, "expiry": expiry, "days_left": None,
                        "trial_days_left": 0}
            if not self._salt:
                return {"mode": "locked", "reason": "OASIS_LICENSE_SALT not configured",
                        "tenant": tenant, "expiry": expiry, "days_left": None,
                        "trial_days_left": 0}
            if modules.get(module) != self._fingerprint(tenant, module, expiry):
                return {"mode": "locked", "reason": "license signature mismatch",
                        "tenant": tenant, "expiry": expiry, "days_left": None,
                        "trial_days_left": 0}
            exp = date.fromisoformat(expiry)
            days_left = (exp - date.today()).days
            if days_left < 0:
                return {"mode": "locked", "reason": f"license expired {expiry}",
                        "tenant": tenant, "expiry": expiry, "days_left": days_left,
                        "trial_days_left": 0}
            return {"mode": "licensed", "reason": "ok", "tenant": tenant,
                    "expiry": expiry, "days_left": days_left,
                    "trial_days_left": None}
        except Exception as e:
            logger.error("Failed to read license key: %s", e)
            return {"mode": "locked", "reason": f"unreadable license key: {e}",
                    "tenant": None, "expiry": None, "days_left": None,
                    "trial_days_left": 0}

    def verify_license(self, module_name: str) -> bool:
        """Back-compat boolean check (licensed or in-trial evaluation)."""
        return self.status(module_name)["mode"] in ("licensed", "evaluation")


def verify_module_startup(module_name: str) -> bool:
    """Helper function to run on application startup (back-compat)."""
    return OfflineLicenseManager().verify_license(module_name)


def allowed_modules(mgr: Optional[OfflineLicenseManager] = None) -> set:
    """The set of module SKUs this install may use right now.

    Evaluation (trial) unlocks EVERYTHING — the day-15 lock screens do the
    selling. A licensed install gets exactly the modules in its key. A locked
    install gets nothing (the console core gate stops it first anyway).
    """
    mgr = mgr or OfflineLicenseManager()
    core = mgr.status("core")
    if core["mode"] == "evaluation":
        return set(KNOWN_MODULES)
    if core["mode"] != "licensed":       # core is mandatory — no core, no modules
        return set()
    return {m for m in KNOWN_MODULES if mgr.status(m)["mode"] == "licensed"}


def module_allowed(module: str, allowed: Optional[set] = None) -> bool:
    return module in (allowed if allowed is not None else allowed_modules())


def render_upsell(st, module: str) -> None:
    """The locked-feature stub — a sales surface, not a dead end."""
    label = MODULE_LABELS.get(module, module.title())
    st.markdown(
        f"""<div style="border:1px dashed #888; border-radius:12px; padding:28px;
                    text-align:center; margin-top:24px;">
            <div style="font-size:34px;">🔒</div>
            <div style="font-size:19px; font-weight:700; margin:6px 0;">
                {label} module</div>
            <div style="color:#888; max-width:520px; margin:0 auto;">
                This capability is part of the <b>{label}</b> module, which is
                not included in your current license. Your data is already
                being collected — activation is immediate once licensed.</div>
            <div style="margin-top:14px; color:#2e6ba6; font-weight:600;">
                Contact iLink to activate {label}.</div>
        </div>""", unsafe_allow_html=True)


def console_gate(st, module: str) -> dict:
    """Enforce licensing at a Streamlit console's entry point.

    licensed   → quiet caption in the sidebar (renewal warning at ≤30 days)
    evaluation → visible banner with trial days remaining
    locked     → full-page lock screen and st.stop()
    """
    s = OfflineLicenseManager().status(module)
    if s["mode"] == "licensed":
        try:
            if s["days_left"] is not None and s["days_left"] <= 30:
                st.sidebar.warning(f"License renews in {s['days_left']} day(s).")
            else:
                st.sidebar.caption(f"Licensed to {s['tenant']} · exp {s['expiry']}")
        except Exception:
            pass
    elif s["mode"] == "evaluation":
        st.warning(f"⏳ **EVALUATION MODE** — {s['trial_days_left']} day(s) "
                   "remaining. Contact iLink for a license key.")
    else:
        st.error(f"🔒 **O.A.S.I.S. is locked** — {s['reason']}.\n\n"
                 "Contact iLink to obtain or renew your license key, then place "
                 "it at the configured OASIS_LICENSE_KEY path and reload.")
        st.stop()
    return s
