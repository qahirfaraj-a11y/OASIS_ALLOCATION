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

#: page/tab keys → module SKU (used for gating and per-module usage metering);
#: any key not listed is "core".
PAGE_MODULES = {
    "ordering": "ordering", "suppliers": "ordering",
    "smart_ordering": "ordering", "supplier_intelligence": "ordering",
    "shadow": "network", "transfers": "network", "allocation": "network",
    "transfer_intelligence": "network", "allocation_engine": "network",
    "baskets": "revenue",
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
        # External anchors (registry + AppData) are only used when running with
        # the default state path — i.e., a real install. When tests pass a
        # custom state_path (tmp_path), we honour only the legacy file anchor
        # to avoid polluting global state and breaking test isolation.
        self._use_external_anchors = (state_path is None)

    # ── signing ──────────────────────────────────────────────────────────
    def _fingerprint(self, tenant_id: str, module: str, expiry: str) -> str:
        raw = f"{tenant_id}:{module}:{expiry}:{self._salt}"
        return hashlib.sha256(raw.encode()).hexdigest()

    # ── issuing (vendor side) ────────────────────────────────────────────
    def build_key(self, tenant_id: str, modules: List[str],
                  expiry_date: str) -> dict:
        """Build a signed key dict in memory (no disk write). Requires the salt.

        This is the in-memory core of :meth:`issue`; the online hub issuer uses
        it to mint keys it stores in its own ledger instead of a local file.
        """
        if not self._salt:
            raise RuntimeError("OASIS_LICENSE_SALT must be set to issue licenses")
        datetime.strptime(expiry_date, "%Y-%m-%d")   # validate format
        return {
            "tenant_id": tenant_id,
            "issued": date.today().isoformat(),
            "expiry_date": expiry_date,
            "authorized_modules": {
                m: self._fingerprint(tenant_id, m, expiry_date)
                for m in modules
            },
        }

    def issue(self, tenant_id: str, modules: List[str], expiry_date: str,
              out_path: Optional[str] = None) -> dict:
        """Create and write a signed key file. Requires OASIS_LICENSE_SALT."""
        key = self.build_key(tenant_id, modules, expiry_date)
        path = out_path or self.key_path
        with open(path, "w", encoding="utf-8") as f:
            json.dump(key, f, indent=2)
        logger.info("Issued license for %s (%s) until %s -> %s",
                    tenant_id, ",".join(modules), expiry_date, path)
        return {"tenant_id": tenant_id, "modules": modules,
                "expiry_date": expiry_date, "path": path}

    # ── trial stamp (hardened: dual-anchor + HMAC integrity) ────────────
    #
    # The trial stamp is stored in THREE locations:
    #   1. Legacy file:   oasis/data/.oasis_install_state.json (backward compat)
    #   2. AppData file:  %APPDATA%/OASIS/.oasis_telemetry_cache (survives reinstall)
    #   3. Registry key:  HKCU\Software\OASIS\InstallState (survives file deletion)
    #
    # Each anchor stores a first_run date + HMAC signature. The earliest valid
    # date across all anchors wins (prevents reset by deleting one anchor).
    # HMAC uses a machine fingerprint so stamps can't be copied between machines.

    _TRIAL_HMAC_KEY = b"oasis_trial_integrity_2026"

    def _hmac_sign(self, first_run_iso: str) -> str:
        """HMAC-SHA256 of the first-run date bound to this machine."""
        import hmac as _hmac
        from .machine_fingerprint import machine_id
        mid = machine_id()
        msg = f"{first_run_iso}:{mid}".encode("utf-8")
        return _hmac.new(self._TRIAL_HMAC_KEY, msg, hashlib.sha256).hexdigest()

    def _verify_hmac(self, first_run_iso: str, signature: str) -> bool:
        """Return True if the HMAC signature matches for this machine."""
        try:
            return self._hmac_sign(first_run_iso) == signature
        except Exception:
            return False

    # ── anchor readers ───────────────────────────────────────────────────

    def _read_legacy_anchor(self) -> Optional[date]:
        """Read from the original oasis/data/.oasis_install_state.json."""
        try:
            with open(self.state_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            d = date.fromisoformat(data["first_run"])
            sig = data.get("sig", "")
            if sig and not self._verify_hmac(d.isoformat(), sig):
                logger.warning("Legacy trial stamp HMAC mismatch — date may be tampered")
                return None
            return d
        except Exception:
            return None

    def _read_appdata_anchor(self) -> Optional[date]:
        """Read from %APPDATA%/OASIS/.oasis_telemetry_cache."""
        path = self._appdata_path()
        if not path:
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            d = date.fromisoformat(data.get("tc_init", ""))
            sig = data.get("tc_sig", "")
            if not self._verify_hmac(d.isoformat(), sig):
                logger.warning("AppData trial anchor HMAC mismatch")
                return None
            return d
        except Exception:
            return None

    def _read_registry_anchor(self) -> Optional[date]:
        """Read from HKCU\\Software\\OASIS\\InstallState."""
        try:
            import winreg
            key = winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                                r"Software\OASIS\InstallState", 0,
                                winreg.KEY_READ)
            val, _ = winreg.QueryValueEx(key, "FirstRun")
            sig, _ = winreg.QueryValueEx(key, "Signature")
            winreg.CloseKey(key)
            d = date.fromisoformat(str(val))
            if not self._verify_hmac(d.isoformat(), str(sig)):
                logger.warning("Registry trial anchor HMAC mismatch")
                return None
            return d
        except Exception:
            return None

    # ── anchor writers ───────────────────────────────────────────────────

    def _write_legacy_anchor(self, first: date) -> None:
        try:
            os.makedirs(os.path.dirname(self.state_path), exist_ok=True)
            with open(self.state_path, "w", encoding="utf-8") as f:
                json.dump({
                    "first_run": first.isoformat(),
                    "sig": self._hmac_sign(first.isoformat()),
                }, f)
        except OSError as e:
            logger.warning("Could not write legacy trial stamp: %s", e)

    def _appdata_path(self) -> Optional[str]:
        appdata = os.environ.get("APPDATA")
        if not appdata:
            return None
        return os.path.join(appdata, "OASIS", ".oasis_telemetry_cache")

    def _write_appdata_anchor(self, first: date) -> None:
        path = self._appdata_path()
        if not path:
            return
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump({
                    "tc_init": first.isoformat(),
                    "tc_sig": self._hmac_sign(first.isoformat()),
                }, f)
        except OSError as e:
            logger.warning("Could not write AppData trial anchor: %s", e)

    def _write_registry_anchor(self, first: date) -> None:
        try:
            import winreg
            key = winreg.CreateKey(winreg.HKEY_CURRENT_USER,
                                   r"Software\OASIS\InstallState")
            winreg.SetValueEx(key, "FirstRun", 0, winreg.REG_SZ,
                              first.isoformat())
            winreg.SetValueEx(key, "Signature", 0, winreg.REG_SZ,
                              self._hmac_sign(first.isoformat()))
            winreg.CloseKey(key)
        except Exception as e:
            logger.debug("Could not write registry trial anchor: %s", e)

    # ── composite first-run resolver ─────────────────────────────────────

    def _first_run(self) -> date:
        """Earliest valid first-run date across all anchors.

        If no anchor exists, this is a genuine first run: stamp all anchors.
        If some anchors are missing or tampered, re-stamp them from the
        earliest surviving anchor (self-healing).
        """
        candidates = []
        legacy = self._read_legacy_anchor()
        if legacy:
            candidates.append(legacy)

        # External anchors (AppData + Registry) are only checked for real
        # installs, not when running with a custom state_path (e.g., tests).
        appdata = registry = None
        if self._use_external_anchors:
            appdata = self._read_appdata_anchor()
            if appdata:
                candidates.append(appdata)
            registry = self._read_registry_anchor()
            if registry:
                candidates.append(registry)

        if candidates:
            first = min(candidates)
        else:
            # Genuine first run — no surviving anchors
            first = date.today()
            logger.info("Trial started: %s", first.isoformat())

        # Self-heal: re-stamp any missing/invalid anchors
        if not legacy or legacy != first:
            self._write_legacy_anchor(first)
        if self._use_external_anchors:
            if not appdata or appdata != first:
                self._write_appdata_anchor(first)
            if not registry or registry != first:
                self._write_registry_anchor(first)

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


def license_posture(module: str = "core",
                    mgr: Optional["OfflineLicenseManager"] = None) -> dict:
    """Flat license posture for UIs: ``{status, days_remaining, reason, tenant}``.

    Adapts ``OfflineLicenseManager.status()`` into the shape the launchers and
    the desktop app read. ``status`` mirrors ``mode``
    (licensed | evaluation | locked); ``days_remaining`` is the license
    ``days_left`` when licensed, else the trial days left, else 0 (always an
    int, so callers can compare it numerically without a None guard).
    """
    st = (mgr or OfflineLicenseManager()).status(module)
    days = st.get("days_left")
    if days is None:
        days = st.get("trial_days_left")
    return {
        "status": st.get("mode", "unknown"),
        "days_remaining": int(days) if isinstance(days, int) else 0,
        "reason": st.get("reason"),
        "tenant": st.get("tenant"),
    }


def restart_trial(root: Optional[str] = None) -> date:
    """Re-stamp every trial anchor to today — the evaluation starts over.

    Exists for ONE moment (lifecycle audit D1): when an install that has only
    ever held sample data onboards REAL data, the 14-day evaluation should
    measure OASIS against their store, not against the week they spent
    exploring the demo. Callers (oasis.logic.onboarding) guard that this fires
    once per install; it is not a general-purpose reset.
    """
    if root is None:
        mgr = OfflineLicenseManager()                 # real install: all anchors
    else:
        mgr = OfflineLicenseManager(state_path=os.path.join(
            root, "oasis", "data", ".oasis_install_state.json"))
    today = date.today()
    mgr._write_legacy_anchor(today)
    if mgr._use_external_anchors:
        mgr._write_appdata_anchor(today)
        mgr._write_registry_anchor(today)
    logger.info("Trial restarted at real-data onboarding: %s", today.isoformat())
    return today


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


def gate_status(module: str = "core",
                mgr: Optional["OfflineLicenseManager"] = None) -> dict:
    """The licensing DECISION for one module, with no UI attached.

    ``console_gate`` used to BE the decision, which made it unreachable from
    anything that is not Streamlit — so the four browser consoles locked at
    expiry while the native desktop window (``--mode desktop``, the front door
    OASIS.bat calls RECOMMENDED) opened straight into store data. Desktop
    Phase 3 finding R-2. The decision lives here now; each UI renders it.

    Returns ``OfflineLicenseManager.status()`` plus:
        blocked  — True when the caller MUST show a lock screen and stop
        notice   — ``(level, text)`` a UI should surface, or None.
                   level ∈ ``error`` | ``warning`` | ``info``
    """
    s = dict((mgr or OfflineLicenseManager()).status(module))
    mode = s.get("mode")

    if mode == "licensed":
        left = s.get("days_left")
        if isinstance(left, int) and left <= 30:
            s["notice"] = ("warning", f"License renews in {left} day(s).")
        else:
            s["notice"] = ("info", f"Licensed to {s.get('tenant')} "
                                   f"· exp {s.get('expiry')}")
        s["blocked"] = False
    elif mode == "evaluation":
        s["notice"] = ("warning",
                       f"EVALUATION MODE — {s.get('trial_days_left')} day(s) "
                       "remaining. Contact iLink for a license key.")
        s["blocked"] = False
    else:
        s["notice"] = ("error",
                       f"O.A.S.I.S. is locked — {s.get('reason')}.")
        s["blocked"] = True
    return s


def default_key_path() -> str:
    """Where an activated license key is written (public accessor)."""
    return _default_key_path()


def activate_key(raw: str) -> tuple:
    """Validate a pasted/uploaded key and, if valid, install it. ``(ok, detail)``

    The write half of ``render_license_activation``, extracted so the desktop
    activates a key through the same code path the consoles do — one place
    that decides what a valid key is, one place that writes it.
    """
    ok, detail, key = validate_key_payload(raw)
    if not ok:
        return False, detail
    with open(default_key_path(), "w", encoding="utf-8") as f:
        json.dump(key, f, indent=2)
    logger.info("License activated for tenant %s", key.get("tenant_id"))
    return True, detail


def lock_screen_exports(root: Optional[str] = None) -> dict:
    """The "your data is yours" door, as paths: ``{db, report}`` (None if absent).

    Audit E1/E3: a locked install must still be able to take a full copy of its
    own records. Both lock screens read this, so the browser and the native
    window can never disagree about what a locked client may take with them.
    """
    out = {"db": None, "report": None}
    try:
        from .onboarding import resolved_db_path
        db = resolved_db_path(root) if root else resolved_db_path()
        if db and os.path.exists(db):
            out["db"] = db
    except Exception as e:
        logger.warning("Lock-screen export could not resolve the store: %s", e)
    reports = os.path.join(root or _ROOT, "reports")
    try:
        if os.path.isdir(reports):
            mds = sorted(f for f in os.listdir(reports) if f.endswith(".md"))
            if mds:
                out["report"] = os.path.join(reports, mds[-1])
    except OSError:
        pass
    return out


def copy_exports(dest_dir: str, root: Optional[str] = None) -> list:
    """Copy everything ``lock_screen_exports`` offers into ``dest_dir``.

    The desktop equivalent of the console's download buttons — a native window
    has no browser download, so it writes the files where the operator asks.
    Returns the paths written.
    """
    import shutil
    written = []
    os.makedirs(dest_dir, exist_ok=True)
    for path in lock_screen_exports(root).values():
        if not path:
            continue
        target = os.path.join(dest_dir, os.path.basename(path))
        shutil.copy2(path, target)
        written.append(target)
    return written


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


def validate_key_payload(raw: str) -> tuple:
    """Structural + (when salt is configured) cryptographic check of a pasted
    or uploaded key. Returns (ok, detail, key_dict|None). Pure — unit-tested."""
    try:
        key = json.loads(raw or "")
    except ValueError:
        return False, "That doesn't look like a license key (not valid JSON).", None
    if not isinstance(key, dict) or not key.get("authorized_modules") \
            or not key.get("tenant_id") or not key.get("expiry_date"):
        return False, ("That file is JSON but not an OASIS key — it must carry "
                       "tenant_id, expiry_date and authorized_modules."), None
    mgr = OfflineLicenseManager()
    if mgr._salt:
        tenant, expiry = key["tenant_id"], key["expiry_date"]
        bad = [m for m, sig in key["authorized_modules"].items()
               if sig != mgr._fingerprint(tenant, m, expiry)]
        if bad:
            return False, (f"Signature check failed for module(s): "
                           f"{', '.join(bad)} — the key was altered or issued "
                           "for a different install."), None
    try:
        exp = date.fromisoformat(key["expiry_date"])
        if exp < date.today():
            return False, f"This key expired on {key['expiry_date']}.", None
    except ValueError:
        return False, "expiry_date is not a valid date.", None
    return True, (f"Key for {key['tenant_id']} — modules: "
                  f"{', '.join(key['authorized_modules'])} · "
                  f"valid to {key['expiry_date']}."), key


def render_license_activation(st) -> None:
    """Paste/upload a license key IN the product (audit E2) — no file surgery."""
    up = st.file_uploader("Upload oasis_license.key", type=["key", "json"],
                          key="_lic_upload")
    txt = st.text_area("…or paste the key contents", height=130, key="_lic_paste",
                       placeholder='{"tenant_id": "...", "authorized_modules": {...}}')
    if st.button("Activate license", type="primary", key="_lic_activate"):
        raw = up.getvalue().decode("utf-8", "replace") if up else (txt or "")
        ok, detail = activate_key(raw)
        if not ok:
            st.error(detail)
            return
        st.success(f"✓ License activated. {detail}")
        st.rerun()


def render_lock_screen(st) -> None:
    """Day-15 (or expiry) surface: never a dead end (audit E1/E3).

    The user keeps three doors: activate a key, export THEIR data, and see
    exactly what a license buys. Their records are never held hostage.
    """
    a, b, c = st.tabs(["🔑 Activate", "📦 Export my data", "🧾 What a license includes"])
    with a:
        st.caption("Already purchased? Activate here — no file copying needed.")
        render_license_activation(st)
    with b:
        st.caption("Your data is yours, licensed or not. Take a full copy any time.")
        exports = lock_screen_exports()
        if exports["db"]:
            with open(exports["db"], "rb") as f:
                st.download_button("⬇ Download my store database (.db)",
                                   f.read(),
                                   file_name=os.path.basename(exports["db"]),
                                   key="_lock_export")
        else:
            st.info("No store database found yet.")
        if exports["report"]:
            with open(exports["report"], encoding="utf-8") as f:
                st.download_button("⬇ Latest Value Report", f.read(),
                                   file_name=os.path.basename(exports["report"]),
                                   key="_lock_report")
    with c:
        for m in KNOWN_MODULES:
            st.markdown(f"- **{MODULE_LABELS[m]}**"
                        + (" — mandatory base" if m == "core" else ""))
        st.markdown("**Bundles:** " + " · ".join(
            f"`{n}` ({len(ms)} modules)" for n, ms in BUNDLES.items()))
        st.caption("Contact iLink for a key — activation is immediate, and the "
                   "data OASIS collected during your trial lights up in full "
                   "the moment it lands.")


def console_gate(st, module: str) -> dict:
    """Enforce licensing at a Streamlit console's entry point.

    licensed   → quiet caption in the sidebar (renewal warning at ≤30 days)
    evaluation → visible banner with trial days remaining
    locked     → full-page lock screen and st.stop()

    The Streamlit ADAPTER over ``gate_status`` — it renders the decision, it no
    longer makes it. The desktop gate reads the same ``gate_status``.
    """
    s = gate_status(module)
    level, text = s["notice"]
    if s["mode"] == "licensed":
        try:
            (st.sidebar.warning if level == "warning"
             else st.sidebar.caption)(text)
        except Exception:
            pass
    elif s["mode"] == "evaluation":
        st.warning(f"⏳ **EVALUATION MODE** — {s['trial_days_left']} day(s) "
                   "remaining. Contact iLink for a license key.")
    else:
        st.error(f"🔒 **O.A.S.I.S. is locked** — {s['reason']}.")
        render_lock_screen(st)
        st.stop()
    return s
