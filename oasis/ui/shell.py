"""
OASIS unified shell (U3).

One front door: a single authenticated Streamlit app whose journey-driven,
role-gated navigation replaces the ~10 standalone dashboards. This module
holds the page registry, role-visibility logic (pure, tested), and the
native page renderers. ``app.py`` is the thin entry that wires auth + theme
+ this registry together.

Migration posture (per the UI plan): pages that are cheap and already have
unified logic are rendered natively here (Home/Journey on journey_state,
Allocation on greenfield_runner — collapsing the 4× duplication). Heavier
surfaces still living in their legacy app are shown as honest *bridge* pages
until they are migrated; the legacy launchers stay until parity is confirmed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, Optional, Sequence


@dataclass(frozen=True)
class Page:
    key: str
    label: str
    icon: str
    render: Callable          # render(ctx) -> None
    roles: Sequence[str] = field(default_factory=tuple)  # empty = all roles

    def visible_to(self, role: Optional[str]) -> bool:
        return not self.roles or role in set(self.roles)


# Role groups (existing auth roles; journey target model is future work).
_ALL = ()  # visible to any authenticated role
_MGMT = ("ops_admin", "regional_manager")
_ADMIN = ("ops_admin",)


def build_registry() -> List[Page]:
    """The journey-ordered page registry (Customer Journey §5 IA)."""
    return [
        Page("home", "Home / Journey", "◎", render_home, _ALL),
        Page("diagnose", "Diagnose", "⊙", _bridge("Forensic Audit",
             "pitch_app_v2.py", "Operator-run forensic diagnosis (Phase 1)."),
             _ADMIN),
        Page("shadow", "Shadow", "◑", _bridge("Shadow Audit",
             "shadow_dashboard.py", "Human-vs-OASIS divergence (Phase 2)."),
             _MGMT),
        Page("ordering", "Ordering", "▤", _bridge("Smart Ordering",
             "ops_dashboard.py", "Daily PO review & approval (Phases 4–6)."),
             _ALL),
        Page("transfers", "Transfers", "⇄", _bridge("Transfer Intelligence",
             "ops_dashboard.py", "Network allocation & transfers (Phase 6)."),
             _MGMT),
        Page("suppliers", "Suppliers", "◇", _bridge("Supplier Shield",
             "ops_dashboard.py", "LATA supplier scorecards (Phase 5)."),
             _MGMT),
        Page("allocation", "Allocation", "▦", render_allocation, _ALL),
        Page("analytics", "Analytics", "▣", _bridge("Analytics",
             "ops_dashboard.py", "Pre→Post target scorecard (Phase 7)."),
             _MGMT),
        Page("settings", "Settings", "⚙", _bridge("Settings",
             "ops_dashboard.py", "Engine thresholds, users, mode control."),
             _ADMIN),
    ]


def visible_pages(pages: Sequence[Page], role: Optional[str]) -> List[Page]:
    return [p for p in pages if p.visible_to(role)]


# ── page renderers ───────────────────────────────────────────────────────
def render_home(ctx) -> None:
    """The Journey home: mode/phase badge, value meter, the 7-stage rail, and
    (for operators/exec) the human-confirmed advance gate."""
    st = ctx["st"]
    from . import components as C
    from ..logic import journey_state as JS

    state = JS.load_state(ctx.get("journey_state_path"))
    st.markdown("### Journey")
    C.mode_phase_badge(state["mode"], state["phase"], state["phase_name"],
                       state["value_recovered"], st_module=st)
    C.value_recovered_meter(state["value_recovered"],
                            state.get("value_target") or 0, st_module=st)
    C.journey_rail(state["phase"], st_module=st)

    nxt = JS.next_phase(state["phase"])
    role = ctx.get("role")
    can_operate = role in ("ops_admin", "regional_manager")
    if nxt is not None and can_operate:
        # Human-confirmed advancement (never automatic).
        approved = C.decision_gate_card(
            title=f"Advance to Phase {nxt}: {JS.phase_name(nxt)}?",
            evidence="Confirm the decision gate for this phase has been met.",
            next_stage=JS.phase_name(nxt),
            can_advance=True,
            key="journey_advance",
            st_module=st,
        )
        if approved:
            JS.advance_phase(ctx.get("username", "unknown"),
                             ctx.get("journey_state_path"))
            st.rerun()
    elif nxt is None:
        st.success("Sustain — the operation is self-driving.")


def render_allocation(ctx) -> None:
    """Greenfield allocation, native in the shell (one page; replaces the
    4× duplicated allocation UIs — runs on the shared greenfield_runner)."""
    st = ctx["st"]
    import os
    from ..logic.greenfield_runner import (
        find_latest_scorecard, load_scorecard_recommendations,
        run_greenfield_allocation,
    )
    from ..logic.order_engine import OrderEngine

    st.markdown("### Allocation Engine")
    search_dir = ctx.get("project_root", os.getcwd())
    scorecard = find_latest_scorecard(search_dir)
    if not scorecard or not os.path.exists(scorecard):
        from . import components as C
        C.empty_state("No scorecard found",
                      "Place a Full_Product_Allocation_Scorecard_v*.csv in the "
                      "project directory to run a greenfield allocation.",
                      st_module=st)
        return

    budget = st.slider("Capital Budget (KES)", 50_000, 200_000_000, 3_000_000,
                       step=100_000)
    if st.button("Run Allocation", type="primary"):
        with st.spinner("Running greenfield allocation…"):
            engine = ctx.get("engine") or OrderEngine(
                os.path.join(search_dir, "oasis", "data"))
            recs = load_scorecard_recommendations(scorecard)
            res = run_greenfield_allocation(engine, recs, float(budget))
        if res.is_empty:
            st.warning("No items allocated. Try a larger budget.")
            return
        from . import components as C
        s = res.summary
        C.kpi_row([
            {"label": "Budget", "value": f"KES {budget:,.0f}"},
            {"label": "Cash Used", "value": f"KES {res.cash_spend:,.0f}"},
            {"label": "Utilization", "value": f"{s.get('utilization_pct', 0):.1f}%"},
            {"label": "SKUs", "value": len(res.basket)},
        ], st_module=st)
        st.dataframe(res.basket.sort_values("Allocated_Cost", ascending=False),
                     use_container_width=True, hide_index=True)


def _bridge(title: str, legacy_file: str, blurb: str) -> Callable:
    """A page that explains a not-yet-migrated surface and points to its
    legacy launcher. Honest interim state during the incremental migration."""
    def _render(ctx) -> None:
        st = ctx["st"]
        from . import components as C
        C.empty_state(
            f"{title} — migrating to the shell",
            f"{blurb}  This surface still runs in its dedicated dashboard "
            f"({legacy_file}); it will move into the shell in a later U3 step.",
            st_module=st,
        )
    return _render
