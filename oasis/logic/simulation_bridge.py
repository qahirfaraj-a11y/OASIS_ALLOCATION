import sys
import os
import hashlib
import math
from datetime import datetime
from typing import List, Dict, Any

# Ensure we can import from the sibling modules
sys.path.append(os.getcwd())

from oasis.logic.order_engine import OrderEngine, apply_safety_guards
from oasis.data.supplier_calendar import SupplierCalendar
from oasis.logic import order_up_to as _ou
from oasis.logic import clock as _clock


def _supplier_phase_offset(supplier: str, gap_days: int) -> int:
    """Deterministic per-supplier phase offset in [0, gap_days).

    A3 fix: the old fallback `current_day % gap_days == 0` landed every
    supplier sharing a gap on the same days, producing synchronized order
    spikes (and a year-end discontinuity). Hashing the supplier name spreads
    suppliers evenly across the cycle while keeping each supplier's days
    stable run-to-run. md5 (not Python's salted hash()) guarantees the
    offset is identical across processes.
    """
    if gap_days <= 1:
        return 0
    digest = hashlib.md5(str(supplier).encode("utf-8")).hexdigest()
    return int(digest, 16) % gap_days


def _find_calendar_path(data_dir: str) -> str:
    """Discover Supplier_Order_Calendar file from data_dir or cwd."""
    candidates = [
        os.path.join(data_dir, "..", "Supplier_Order_Calendar_2026.xlsx"),
        os.path.join(os.getcwd(), "Supplier_Order_Calendar_2026.xlsx"),
        os.path.join(data_dir, "Supplier_Order_Calendar_2026.xlsx"),
    ]
    for p in candidates:
        if os.path.exists(p):
            return os.path.abspath(p)
    return candidates[1]  # Fallback to cwd


class SimulationOrderUtil:
    """
    Bridge to use Oasis OrderEngine logic within a high-speed simulation.
    Replaces LLM calls with deterministic Python logic derived from the AI prompts.
    """
    
    def __init__(self, data_dir: str, thresholds: Dict[str, Any] = None, engine: OrderEngine = None):
        self.data_dir = data_dir
        if engine is not None:
            self.engine = engine
        else:
            self.engine = OrderEngine(data_dir)
            # Synchronous load for simulation speed
            self.engine.load_local_databases()
        
        # The declared order schedule, for the derived model. Loaded once here
        # rather than per line: it is a policy file, not per-SKU data, and
        # re-reading it 3,000 times a scan would be the kind of quiet cost that
        # only shows up at a customer with a real catalogue.
        try:
            self._review_schedule = _ou.load_review_schedule(
                os.path.dirname(os.path.abspath(data_dir)))
        except Exception:
            self._review_schedule = {}

        # Calendar Integration (G2 Fix: relative path discovery)
        cal_path = _find_calendar_path(data_dir)
        self.calendar = SupplierCalendar(cal_path)
        self.calendar_loaded = False

        # F3: LATA Supplier Shield — per-supplier lead-time-variance safety
        # multipliers from supplier_patterns (written by run_lata). Unreliable
        # suppliers (>30% LT variance) inflate the buffer up to 2.0x; rock-solid
        # ones trim it toward 0.8x. Missing file/entry → neutral 1.0.
        #
        # NOT LOADED. `_load_lata_multipliers` is still available below and
        # still documented, but nothing in ordering reads it, so reading 944
        # entries into an attribute on every construction bought nothing and
        # cost something worse than the milliseconds: it read as live wiring.
        #
        # Two separate readers -- including one of mine -- concluded from this
        # line that LATA's multiplier was feeding safety stock. It is not, and
        # has not been since the DOUBLE-COUNT FIX in
        # `calculate_order_quantity`: sizing reads sigma_L off
        # `self._lead_patterns` and combines it with demand cv in quadrature,
        # once, rather than stacking a multiplier on a separate cv factor.
        #
        # LATA itself IS live in ordering -- via lead_time_stdev, on 91.2% of
        # order lines. It is only the MULTIPLIER form that is parked, and
        # parked correctly: dividing by lead time shrinks the lead-time
        # contribution as lead time grows, when L=1+/-1d and L=10+/-1d carry
        # the same absolute exposure.
        #
        # Call `_load_lata_multipliers(data_dir)` directly for comparison
        # tooling; OASIS_LATA_SOURCE=table still selects the old hand-tuned
        # table there.

        # THE WIRING FIX (task A): `order_up_to.sigma_lead()` needs a dict
        # keyed by supplier with a `lead_time_stdev`-shaped field on it.
        # `self.engine.databases['supplier_patterns']` (supplier_patterns_
        # 2025.json, loaded by order_engine.py) carries NO such key -- only
        # `lata_stdev_days` -- so the one live caller of `_ou.recommend()`
        # used to hand it that dict directly and every lookup missed,
        # silently falling back to the chain-wide 2.22d constant for EVERY
        # supplier, including the ~472 the receipt history measures
        # precisely. `_ou.default_patterns()` is THE canonical, correctly-
        # keyed source (it also folds in supplier_patterns_2025.json's
        # `lata_stdev_days` as a gap-filler -- see order_up_to.py) and is
        # cached at module level, so calling it here is cheap and keeps one
        # copy per process rather than one per SKU.
        self._lead_patterns = _ou.default_patterns()

        # F4: ROP source gate. 'heuristic' (default) keeps the flat fallback
        # ADS×(LT+safety). 'newsvendor' computes the statistically-correct
        # reorder point (μ_LTD + z·σ_LTD at OASIS_SERVICE_LEVEL) when no stored
        # ROP exists; 'newsvendor-all' also overrides stored ROPs (A/B mode).
        self._rop_mode = os.getenv("OASIS_ROP_MODE", "heuristic").lower().strip()
        self._service_level = float(os.getenv("OASIS_SERVICE_LEVEL", "0.95"))

        # G4 Fix: Configurable thresholds (can be overridden from Settings/DB)
        self.thresholds = thresholds or {
            'fresh_stale_days': 120,
            'dry_dead_days': 200,
            'dry_dead_min_sales': 5,
            'key_sku_boost_pct': 0.20,
            'critical_stockout_days': 2.0,
            # Phase C: Minimum Order Threshold to prevent micro-orders
            'min_order_units': 10,
            'min_order_value_kes': 5000,
            # Per-SKU value floors. Absolute shillings, so they bind harder
            # the smaller the store — settable for exactly that reason.
            'min_item_value_fresh_kes': 200.0,
            'min_item_value_dry_kes': 100.0,
        }
        
    @staticmethod
    def _load_lata_multipliers(data_dir: str) -> Dict[str, float]:
        """{SUPPLIER_UPPER: lata_variance_multiplier}, derived where possible.

        Two sources, in order:

          lata_derived.json      multipliers DERIVED from the receipt history —
                                 sqrt(1 + sigma_L^2 / (P * cv^2)), which is the
                                 exact inflation the safety term implies. No
                                 ceiling: the demand rate cancels and what is
                                 left is measurable.
          supplier_patterns_*    the older hand-tuned table, kept as a fallback.

        The table was well-calibrated (rho = 0.93 against lead-time CV measured
        from receipts) right up to its ceiling, where 187 of 599 suppliers sat
        at exactly 3.0 and the multiplier stopped distinguishing anything. Its
        median was 2.59; the derived median is 1.52. A cap is a decision
        disguised as a constant, and this one had drifted 50% above the value
        its own comment documented.

        Set OASIS_LATA_SOURCE=table to force the old behaviour.
        """
        import glob
        import json
        import os
        out: Dict[str, float] = {}
        source = (os.getenv("OASIS_LATA_SOURCE") or "derived").lower().strip()
        try:
            candidates = []
            if source != "table":
                derived = os.path.join(data_dir, "lata_derived.json")
                if os.path.exists(derived):
                    candidates = [derived]
            if not candidates:
                # Newest by mtime, not by name: filesystem order is not a policy.
                pats = glob.glob(os.path.join(data_dir, "supplier_patterns_*.json"))
                candidates = ([max(pats, key=os.path.getmtime)] if pats else [])
            if not candidates:
                return out
            with open(candidates[0], "r", encoding="utf-8") as f:
                patterns = json.load(f)
            for supplier, d in (patterns or {}).items():
                if isinstance(d, dict) and d.get("lata_variance_multiplier") is not None:
                    try:
                        out[str(supplier).upper().strip()] = float(d["lata_variance_multiplier"])
                    except (TypeError, ValueError):
                        continue
            print(f"LATA: {len(out)} suppliers from "
                  f"{os.path.basename(candidates[0])}")
        except (OSError, ValueError, TypeError) as e:
            # Narrow, deliberately. The previous `except Exception: return {}`
            # swallowed a NameError in this very method and reported it as "no
            # LATA data" — every supplier silently neutral, nothing said. A
            # bare except turns a programming error into a data condition, and
            # the two want opposite responses.
            print(f"LATA: unreadable ({e}); every supplier stays neutral")
            return {}
        return out

    def prepare_sku_data(self, sku_list: List[Dict[str, Any]], skip_enrichment: bool = False) -> List[Dict[str, Any]]:
        """
        Enrich raw SKU data using Oasis Intelligence.
        
        Performance: In simulation context (reordering), SKU data is already enriched
        from initialization. skip_enrichment=True bypasses the heavy 23k-item enrichment
        pipeline which was the #1 performance bottleneck (~10+ min per reorder cycle).
        """
        if skip_enrichment:
            # SKUs are already enriched — just ensure minimum required fields exist
            for sku in sku_list:
                if 'target_coverage_days' not in sku:
                    sku['target_coverage_days'] = 7 if not sku.get('is_fresh') else 2
                if 'reorder_point' not in sku:
                    ads = float(sku.get('avg_daily_sales', 0))
                    sku['reorder_point'] = ads * sku.get('target_coverage_days', 7)
                if 'safety_stock' not in sku:
                    sku['safety_stock'] = float(sku.get('avg_daily_sales', 0)) * 2
                if 'estimated_delivery_days' not in sku:
                    sku['estimated_delivery_days'] = 7
            return sku_list
        return self.engine.enrich_product_data(sku_list)

        
    def calculate_order_quantity(self, enriched_skus: List[Dict[str, Any]], 
                                 store_config: Dict[str, Any] = None,
                                 current_day: int = 1,
                                 gnn_risk_score: float = 0.0,
                                 use_real_date: bool = False) -> List[Dict[str, Any]]:
        """
        Deterministic implementation of the "AI Prompt Logic" for Replenishment.
        
        Rules ported from `analyze_batch_ai` prompt:
        1. Slow Mover Checks (Dead Stock > 200d)
        2. Freshness Checks (Stale Fresh > 120d)
        3. Net Requirement Calculation: (Target - Current - OnOrder)
        4. Key SKU Boost (Top 500 get 20% buffer)
        
        Args:
            use_real_date: If True, use today's actual weekday for schedule checks
                           instead of the simulation day counter. Use True for
                           dashboard/scheduler context, False for simulation.
        """
        # G2 Fix: Map to real calendar day when in dashboard context.
        # as_of() rather than today(): the supplier calendar is checked against
        # a WEEKDAY, so measuring a 2025 extract on a 2026 clock asks whether
        # each supplier delivers on the wrong day of the week. Unset, this is
        # today and nothing changes.
        if use_real_date:
            current_day = _clock.as_of().timetuple().tm_yday  # Day-of-year (1-366)
        
        # Load Chapter 11 Engine Caches for daily replenishment enforcement
        amit_enabled = self.engine.is_engine_enabled('amit')
        amit_blacklist = self.engine.databases.get('amit_enforcement', set()) if amit_enabled else set()
        
        mande_enabled = self.engine.is_engine_enabled('mande')
        mande_purge = self.engine.databases.get('mande_purge_list', set()) if mande_enabled else set()

        # ONE POLICY FOR BOTH ORDER PATHS. engines.<name>.mode is read by the
        # same function the classic path (procurement_mixin) uses: "report"
        # flags the line, only "enforce" blocks it. This path used to block
        # whenever the engine was enabled, so the shipped order-up-to model
        # vetoed 2,084 selling SKUs under MANDE and 1,103 under AMIT on a
        # store whose config said report -- the classic path's own ablation
        # prices MANDE enforcement at -12.9pp service and -20.3m KES/yr GP.
        from .procurement_mixin import _read_engine_mode
        amit_enforce = _read_engine_mode(self.data_dir, 'amit') == 'enforce'
        mande_enforce = _read_engine_mode(self.data_dir, 'mande') == 'enforce' 
        
        halo_list = self.engine.databases.get('halo_protection_list', set())
        
        recommendations = []
        
        for p in enriched_skus:
            # Create a recommendation object
            rec = p.copy()
            rec['recommended_quantity'] = 0
            rec['reasoning'] = ""

            # NO DEMAND SIGNAL IS A FACT ABOUT THE LINE, NOT A QUIET ZERO.
            # Set at the TOP of the loop because this line can leave by half a
            # dozen exits -- not an ordering day, above the reorder point, dead
            # stock, stale fresh -- and several of them ASSIGN to reasoning
            # rather than append, so anything written into the string later is
            # discarded. The flag lives on rec, which survives all of them.
            #
            # A line with no measured rate gets a reorder point of 0, so ANY
            # stock puts it above the trigger and it never reaches the
            # order-up-to branch at all: it lands on "[Above ROP 0.0]", which
            # is true and tells a buyer nothing. That is where the expensive
            # ones sit. Measured on the live store: 10,967 SKUs have no POS
            # sales history (27.6% of the catalogue), and NONE is a join
            # failure -- every SKU that has ever sold does resolve a
            # POS-derived rate, so refusing to order them is correct. But
            # 1,723 of them still hold stock: 17,966 units and KES 6,696,865
            # of capital in lines that have never sold a unit at this store,
            # described to the buyer as adequately covered.
            #
            # Not ordering them is right. Saying nothing about them is not.
            if float(p.get('avg_daily_sales') or 0) <= 0:
                rec['ads_missing'] = True
                rec['ads_source'] = str(p.get('ads_source') or 'none')
            
            # --- CHAPTER 11 ENFORCEMENT ---
            p_name = p.get('product_name', 'Unknown')
            p_name_norm = self.engine.normalize_product_name(p_name)
            
            # HALO Affinity Protection Check
            is_halo_protected = False
            if p_name_norm in halo_list:
                is_halo_protected = True
                p['is_key_sku'] = True
                p['is_top_sku'] = True
                rec['is_key_sku'] = True
                rec['is_top_sku'] = True
                rec['reasoning'] += " [HALO Protected]"

            # AMIT Blacklist Check (Low GMROI delisting)
            if amit_enabled and p_name_norm in amit_blacklist:
                if amit_enforce:
                    rec['recommended_quantity'] = 0
                    rec['reasoning'] = "Blocked: AMIT Blacklist (Low GMROI / Stranded Capital)"
                    recommendations.append(rec)
                    continue
                rec['amit_flag'] = True
                rec['amit_note'] = ("AMIT: over department cap on annual "
                                    "gross profit; flagged, not blocked")
                
            # MANDE Supplier Purge Check (Delisted Supplier Capital Trap)
            supplier_upper = str(p.get('supplier_name', '')).upper().strip()
            is_staple = p.get('is_staple', False) or p.get('is_top_sku', False) or (p.get('sales_rank', 999) < 500)
            is_essential = p.get('is_fresh', False) or any(x in p_name_norm for x in ['SUGAR', 'SALT', 'FLOUR', 'RICE', 'COOKING OIL', 'FRESH MILK', 'BREAD', 'EGGS'])
            
            if mande_enabled and supplier_upper in mande_purge and not (is_staple or is_essential):
                if mande_enforce:
                    rec['recommended_quantity'] = 0
                    rec['reasoning'] = "Blocked: MANDE Supplier Purge (Delisted Capital Trap)"
                    recommendations.append(rec)
                    continue
                rec['mande_flag'] = True
                rec['mande_note'] = "MANDE: supplier on the purge report; flagged, not blocked"

            # 1. DETERMINE IF WE CAN ORDER TODAY
            supplier = p.get('supplier_name', 'Unknown')
            gap_days = int(p.get('median_gap_days', 7))
            if gap_days < 1: gap_days = 1
            
            # CALENDAR CHECK
            if not self.calendar_loaded:
                 self.calendar.load()
                 self.calendar_loaded = True

            schedule = self.calendar.get_schedule(supplier)
            is_ordering_day = False
            
            if schedule == 'DAILY':
                is_ordering_day = True
            elif isinstance(schedule, set):
                is_ordering_day = current_day in schedule
            else:
                # A3: phase-staggered fallback. Each supplier gets a stable
                # offset so same-gap suppliers spread across the cycle instead
                # of all firing on the same day. Day 1 stays an ordering day
                # for every supplier (greenfield / first-run priming).
                offset = _supplier_phase_offset(supplier, gap_days)
                is_ordering_day = ((current_day + offset) % gap_days == 0) or (current_day == 1)
            
            # Check Critical Status (for Override)
            current_stock = p.get('current_stock', p.get('current_stocks', 0))
            avg_daily_sales = p.get('avg_daily_sales', 0)
            days_coverage = current_stock / avg_daily_sales if avg_daily_sales > 0 else 999
            
            # Critical Threshold: Dynamic based on Lead Time
            # We must order when Stock < Lead Time + Safety Buffer
            lead_time = p.get('lead_time_days', 1) or 1
            
            # --- DYNAMIC VOLATILITY BUFFERING (Unified) ---
            # Base Safety: 4.0 for Fresh (Robust), 1.5 for Dry
            cv = p.get('demand_cv', 0.2)
            base_safety = 4.0 if p.get('is_fresh', False) else 1.5
            
            # Volatility Factor: 2.0x penalty for variance
            vol_factor = 2.0
            
            # GNN Risk Multiplier: Increase safety stock if network predicts high vulnerability
            gnn_multiplier = 1.0
            if gnn_risk_score > 0.5:
                # scale from 1.0 at risk 0.5 to 1.3 at risk 1.0
                gnn_multiplier = 1.0 + ((gnn_risk_score - 0.5) * 0.6)

            # F3: LATA Supplier Shield.
            #
            # sigma_L now comes from `_ou.sigma_lead()` against
            # `self._lead_patterns` (task A) -- the correctly-keyed, measured
            # patterns file. Before this fix, the only per-line signal for
            # lead-time variance was `self._lata_multipliers` sourced from
            # supplier_patterns_2025.json / lata_derived.json; that lookup
            # itself was fine, but the QUANTITY MODEL's own sigma_lead() call
            # (used when OASIS_ORDER_MODEL=order_up_to) was broken -- see the
            # __init__ comment above.
            #
            # DOUBLE-COUNT FIX (diagnosis item 4): the old formula was
            #     base_safety * (1 + vol_factor*cv) * gnn_multiplier * lata_multiplier
            # `lata_multiplier` (however sourced) is itself SHAPED like
            # sqrt(1 + sigma_L^2/(P*cv^2)) -- i.e. already a function of
            # demand cv -- so multiplying it against a SEPARATE
            # (1 + vol_factor*cv) let cv's effect enter twice, and let four
            # independent-ish factors compound multiplicatively instead of
            # combining the way independent variances should. Demand
            # variability and lead-time variability are combined ONCE here,
            # additively under a square root -- the same shape as the
            # derivation this engine commits to elsewhere:
            #     S = d*P + z*sqrt(P*sigma_d^2 + d^2*sigma_L^2)
            #     (order_up_to.demand_sigma_over)
            # dividing through by d turns that into a days-of-cover term:
            #     sqrt(P*cv^2 + sigma_L^2)  ==  sqrt((sigma_d/d)^2 + sigma_L^2)
            # and P's demand-side share is approximated here by the existing
            # `vol_factor` scaling on cv, so the two components being
            # combined are `vol_factor*cv` (demand) and `sigma_L/lead_time`
            # (lead time's OWN coefficient of variation) -- both dimension-
            # less relative spreads, safe to combine via sqrt(a^2+b^2).
            #
            # `lata_multiplier` is kept, NAMED and NEUTRALISED to 1.0, rather
            # than deleted -- so anything still reading it (reasoning
            # strings, scorecards) sees an explicit no-op instead of a
            # missing field. `self._lata_multipliers` (the old table/derived
            # value) is no longer multiplied in; it stays available for
            # comparison logging only.
            # DIMENSIONS. The solver's first pass combined `vol_factor*cv` with
            # `sigma_L / lead_time` -- both "dimensionless relative spreads".
            # They are not the same thing and the division is wrong. In
            #     S = d*P + z*sqrt(P*sigma_d^2 + d^2*sigma_L^2)
            # divide by d: sigma_L enters DIRECTLY IN DAYS, because the term is
            # d^2*sigma_L^2. Turning it into sigma_L/L makes the lead-time
            # contribution SHRINK as the lead time grows, which is backwards: a
            # supplier at L=1 +/-1 day and one at L=10 +/-1 day carry the same
            # absolute exposure, one day of demand, and the CV form charges the
            # first ten times the second.
            #
            # So the safety term is computed by the engine's OWN function rather
            # than reimplemented here. demand_sigma_over(P, 1, cv, sigma_L)
            # returns sqrt(P*cv^2 + sigma_L^2) -- the days-of-cover form of the
            # derivation, exactly. One derivation, one place.
            #
            # This also retires `base_safety` (4.0 fresh / 1.5 dry) from SIZING.
            # It was calibrated as a TRIGGER constant and has no source; with
            # sigma_L measured per supplier there is nothing left for it to
            # stand in for. It survives below as a FLOOR only, so a line with a
            # perfectly reliable supplier and no measured demand spread still
            # keeps a token buffer rather than dropping to zero.
            sigma_L = _ou.sigma_lead(self._lead_patterns.get(supplier_upper), record=False)
            _P = float(gap_days) + float(lead_time)
            _z = _ou.z_score()
            lead_time_cv = (sigma_L / lead_time) if lead_time > 0 else 0.0   # reporting only
            lata_multiplier = 1.0  # NEUTRALISED -- sigma_L enters via the quadrature
            safety_days = _z * _ou.demand_sigma_over(_P, 1.0, cv, sigma_L)
            safety_buffer = max(safety_days * gnn_multiplier, base_safety * 0.5)

            critical_thresh = lead_time + safety_buffer

            if gnn_multiplier > 1.0:
                 rec['reasoning'] += f" [GNN Risk Burst: +{(gnn_multiplier-1.0)*100:.0f}% Safety]"
            if sigma_L > 0.05:
                 # NAME THE ARTEFACT, NOT JUST THE ENGINE. "LATA Shield" alone
                 # is accurate but under-specified, and the ambiguity has cost
                 # real time: LATA publishes TWO things from one receipt
                 # history -- lead_time_stdev (this, in days, entering
                 # d^2*sigma_L^2) and lata_variance_multiplier (a scalar,
                 # pinned to 1.0 in ordering and used only for allocation
                 # priority). A reader who takes "LATA Shield" to mean the
                 # multiplier concludes the multiplier is live. It is not.
                 #
                 # Saying which one removes the ambiguity at no cost: the
                 # number shown is already the one being used.
                 # The "[LATA Shield:" prefix is load-bearing -- devkit's
                 # governance_sweep matches on it literally -- so the artefact
                 # name goes AFTER the colon, not inside the bracket.
                 rec['reasoning'] += (f" [LATA Shield: lead_time_stdev "
                                      f"sigma_L={sigma_L:.2f}d -> "
                                      f"safety {safety_buffer:.2f}d of a {_P:.1f}d "
                                      f"protection interval]")
                 
            is_critical = days_coverage < critical_thresh
            
            if not is_ordering_day and not is_critical:
                 # "Not due until day 259" is true and beside the point on a
                 # line that has never sold: it will never be usefully due.
                 # The more important fact leads, and this is the exit the
                 # largest share of the book leaves by.
                 if rec.get('ads_missing'):
                     rec['reasoning'] = (
                         f" [no demand signal: never sold here, "
                         f"{current_stock:.0f} on hand — review or delist]")
                 else:
                     rec['reasoning'] = f" [Schedule: Gap {gap_days}d, Next: Day {current_day + (gap_days - current_day % gap_days)}]"
                 recommendations.append(rec)
                 continue
            
            if is_critical and not is_ordering_day:
                 rec['reasoning'] += " [CRITICAL OVERRIDE: Schedule Bypass]"

            # --- LOGIC PORTED FROM AI PROMPT ---
            
            # 1. SLOW MOVER / FRESHNESS CHECKS
            days_since_delivery = p.get('days_since_delivery', 0)
            is_fresh = p.get('is_fresh', False)
            sales_90d = p.get('total_units_sold_last_90d', 0)

            # 1a. DISCONTINUED: no stock AND nothing sold in 90 days.
            # OPT-IN, default OFF — Golden Logic behaviour is unchanged unless a
            # client sets block_discontinued. Configured like every other
            # threshold here rather than hardcoded, so this is a tuning knob and
            # not a new branch in the default hot path.
            #
            # Why it exists: the dead-stock rule below keys on
            # days_since_delivery, which some POS backends cannot supply (RXL has
            # no SM_LAST_RECV_DT). When that field defaults to 0, `> 200` is never
            # true and the guard is silently inert. This rule holds on stock and
            # sales alone — data every POS has — so it cannot be disabled by a
            # missing column.
            if self.thresholds.get('block_discontinued', False):
                _stock_now = float(p.get('current_stock')
                                   if p.get('current_stock') is not None
                                   else p.get('current_stocks', 0) or 0)
                if _stock_now <= 0 and float(sales_90d or 0) <= 0 and not is_halo_protected:
                    rec['reasoning'] = "Blocked: Discontinued (no stock, no sales in 90d)"
                    recommendations.append(rec)
                    continue
            
            # G4 Fix: Configurable thresholds (can be loaded from DB/Settings)
            fresh_stale_days = self.thresholds.get('fresh_stale_days', 120)
            dry_dead_days = self.thresholds.get('dry_dead_days', 200)
            dry_dead_min_sales = self.thresholds.get('dry_dead_min_sales', 5)
            
            # Fresh Stale Logic
            if is_fresh and days_since_delivery > fresh_stale_days:
                if sales_90d == 0 and not is_halo_protected:
                    rec['reasoning'] = f"Blocked: Stale Fresh (>{fresh_stale_days}d, No Sales)"
                    recommendations.append(rec)
                    continue 
                # Else: Cap logic handled in safety guards, but we can be proactive
            
            # Dry Dead Stock Logic
            if not is_fresh and days_since_delivery > dry_dead_days:
                if sales_90d < dry_dead_min_sales and not is_halo_protected:
                    if sales_90d == 0:
                        rec['reasoning'] = f"Blocked: Dead Stock (>{dry_dead_days}d, No Sales)"
                        recommendations.append(rec)
                        continue

            # 2. NET REQUIREMENT CALCULATION
            # Target Stock = Reorder Point (Coverage Days * Velocity)
            # But we might want to respect the STORE CONFIG for "Safety Days" if provided?
            
            avg_daily_sales = p.get('avg_daily_sales', 0)

            # G3 Fix: ROP Fallback — if reorder_point is 0 or missing (no intelligence data),
            # calculate a dynamic fallback instead of treating 0 as real ROP.
            # F4: in newsvendor mode the fallback (or, in 'newsvendor-all', every
            # ROP) is the statistically-correct μ_LTD + z·σ_LTD instead of the
            # flat heuristic — demand-variance-aware at the service level.
            reorder_point = p.get('reorder_point', 0)
            use_newsvendor = (self._rop_mode == "newsvendor-all"
                              or (self._rop_mode == "newsvendor" and reorder_point <= 0))
            if use_newsvendor and avg_daily_sales > 0:
                from math import sqrt

                from . import risk_baseline as RB
                # THE HORIZON IS P = R + L, NOT L.
                #
                # A reorder point has to cover demand until stock can next
                # ARRIVE, and under periodic review that is the review period
                # plus the lead time. Using L alone protects only the delivery
                # window and silently drops R — the one horizon in this engine
                # with a derivation, and the term measured at 2.07x working
                # capital.
                #
                # The omission does not look like an error, it looks like a
                # tighter reorder point: on this book it halved ROP (31 -> 16.6
                # units on a 10/day line) and stopped replenishment on every
                # line holding more than ~1.7 days of cover. Flipping
                # OASIS_ROP_MODE off 'heuristic' with L-only maths would have
                # quietly stopped ordering across 6,313 lines.
                _R = _ou.review_period(str(p.get('supplier_name') or '').upper(),
                                       self._review_schedule)
                horizon = max(1.0, float(lead_time) + float(_R))
                mu_ltd = avg_daily_sales * horizon
                sigma_ltd = cv * avg_daily_sales * sqrt(horizon)
                reorder_point = RB.reorder_point(mu_ltd, sigma_ltd,
                                                 self._service_level)
                rec['reasoning'] += (f" [ROP Newsvendor: SL{self._service_level:.0%}"
                                     f" P={horizon:.1f}d]")
            elif reorder_point <= 0 and avg_daily_sales > 0:
                # Fallback ROP = ADS * (lead_time + base_safety)
                fallback_rop = avg_daily_sales * (lead_time + (base_safety * (1 + cv)))
                reorder_point = fallback_rop
                rec['reasoning'] += " [ROP Fallback: intelligence data missing]"
            
            current_stock = p.get('current_stock', p.get('current_stocks', 0))
            on_order = p.get('on_order_qty', 0)

            # THE TRIGGER MUST PROTECT THE SAME HORIZON AS THE TARGET.
            #
            # This engine is (s, S): the check below decides whether a line is
            # looked at, and only lines at or under it reach the order-up-to
            # level. So the trigger is not a detail of the target -- it is the
            # gate in front of it, and if it protects a SHORTER horizon there
            # is a band where a line is already short and the engine never
            # runs the arithmetic that would notice.
            #
            # It did. The enriched reorder_point is
            # sales_velocity * (lead_time + safety_days), a median 10.0 days
            # of cover, while S protects R + L, a median 14.0 -- so 93.3% of
            # lines had a trigger shorter than the target it gates.
            #
            # THE CLAIM IS ABOUT REVIEW DAYS, NOT ABOUT EVERY DAY. A position
            # below d*(R+L) mid-cycle is normal and is what cycle stock is
            # for; 421 lines sat in that band on one real shelf, and most were
            # simply between deliveries. The ones that matter are the lines
            # reaching their OWN ordering day still above the reorder point:
            # the schedule offers them their one chance, the trigger declines
            # to look, and they carry less than the demand that must last them
            # to the next delivery.
            #
            # Measured on that population: 193 lines, KES 269,751, on a single
            # day's shelf. All 193 were released by this floor and none was
            # schedule-blocked, while 205 of the 228 it left alone were simply
            # not due. QUENCHER 300ML at 42/day and EXE 2KG FLOUR at 28/day
            # are in the first group.
            #
            # The rule is not an opinion about how much stock to hold: below
            # d*(R+L) the line cannot survive to its next delivery, whatever
            # quantity model then runs. So this is a FLOOR, never a reduction
            # -- a supplier-specific ROP that already protects more keeps it --
            # and it stays on the shared side of the model fork, so a
            # classic-vs-derived comparison still differs only in the quantity
            # decision.
            #
            # The same error is warned about a few lines above, for the
            # newsvendor ROP: "THE HORIZON IS P = R + L, NOT L ... silently
            # drops R". That branch was fixed; the enriched reorder_point that
            # actually fires on most lines kept it.
            if avg_daily_sales > 0:
                _R_trigger = _ou.review_period(
                    str(p.get('supplier_name') or '').upper(),
                    self._review_schedule)
                _P_trigger = max(1.0, float(lead_time) + float(_R_trigger))
                _protection = avg_daily_sales * _P_trigger
                if _protection > reorder_point:
                    reorder_point = _protection
                    rec['reasoning'] += (
                        f" [ROP floored at the protection interval "
                        f"R+L={_P_trigger:.0f}d]")

            # Check reorder trigger
            if current_stock <= reorder_point:
                # ── DERIVED MODEL, behind OASIS_ORDER_MODEL ────────────────
                #
                # S = d(R+L) + z·sqrt((R+L)·sigma_d^2 + d^2·sigma_L^2)
                #
                # The classic path below computes the horizon from the OBSERVED
                # order gap and then multiplies it by five factors. This one
                # takes R from the client's declared order schedule, adds the
                # supplier's measured lead-time variance — which the classic
                # path omits entirely, though it is the larger of the two
                # variance terms — and buys one explicit service level instead
                # of an implicit product of multipliers.
                #
                # The TRIGGER above is deliberately shared, so a difference in
                # the result is attributable to the quantity decision alone.
                if _ou.is_enabled():
                    # TASK A FIX: was `self.engine.databases.get(
                    # 'supplier_patterns', {})` -- supplier_patterns_2025.json,
                    # which has no `lead_time_stdev`-shaped key, so every
                    # lookup missed and `sigma_lead()` silently returned the
                    # 2.22d chain constant for every supplier. Use the
                    # correctly-keyed, measured cache instead (see __init__).
                    terms = _ou.recommend(
                        p, schedule=self._review_schedule,
                        patterns=self._lead_patterns)
                    q = float(terms.get("quantity") or 0)
                    if q > 0:
                        rec['recommended_quantity'] = q
                        rec['order_up_to_terms'] = terms
                        rec['target_coverage_days'] = (
                            terms["S"] / avg_daily_sales if avg_daily_sales > 0 else 0)
                        rec['reasoning'] += (
                            f" [order-up-to: R={terms['R']:.0f}d L={terms['L']:.0f}d "
                            f"z={terms['z']:.2f} sigmaL={terms['sigma_lead']:.1f}d "
                            f"S={terms['S']:.0f}]")
                    else:
                        # A ZERO IS NOT ALWAYS A ZERO.
                        # order_up_to_terms used to be attached only when q > 0,
                        # so a line the engine deliberately REFUSED -- one pack
                        # exceeding 60 days of cover, a special-order or
                        # transfer decision rather than replenishment -- left
                        # this function indistinguishable from a healthy line
                        # that simply did not need stock. The transfer module
                        # then had to rediscover the population by accident,
                        # from raw cover, and only while the shelf happened to
                        # read zero. The reason is the useful part; carry it.
                        rec['order_up_to_terms'] = terms
                        if terms.get('auto_order_suppressed'):
                            rec['auto_order_suppressed'] = True
                            rec['suppress_reason'] = terms.get('suppress_reason')
                            rec['transfer_candidate'] = True
                            rec['reasoning'] += (
                                f" [SUPPRESSED: {terms.get('suppress_reason')}]")
                        elif terms.get('reason'):
                            # A LINE WITH NO DEMAND SIGNAL IS NOT A COVERED
                            # ONE. recommend() returns early on d <= 0 with
                            # {"quantity": 0, "reason": "no measured sales
                            # rate"} -- no S, no P, no terms at all -- and this
                            # branch used to answer that with "position already
                            # covers P". There is no P. The engine was reporting
                            # a healthy, well-covered line on every SKU whose
                            # demand it could not measure: 10,967 of them on the
                            # live store, 27.6% of the catalogue.
                            #
                            # The reason it already computed is the true one, so
                            # say that instead of inventing a reassurance.
                            rec['ads_missing'] = True
                            rec['reasoning'] += f" [no order: {terms['reason']}]"
                        else:
                            rec['reasoning'] += " [order-up-to: position already covers P]"
                    recommendations.append(rec)
                    continue

                # Calculate Target Stock
                target_coverage_days = p.get('target_coverage_days', 7)
                
                # --- CYCLE STOCK CORRECTION & DOUBLE-STACKING FIX ---
                # For fresh items, we trust the highly optimized DDoS precision target computed during enrichment.
                # Daily fresh items (weekly schedule/rhythm <= 1.5d) should be strictly limited to 1.2 days coverage.
                if is_fresh:
                    if target_coverage_days <= 0:
                        target_coverage_days = 1.2

                    # TASK C FIX: fresh lines used to take target_coverage_days
                    # straight from enrichment -- `safety_buffer` never entered
                    # this branch at all, so LATA moved the TRIGGER
                    # (critical_thresh, above) for a fresh line but never its
                    # SIZE. A daily, reliable supplier should carry LESS safety
                    # than an erratic one, not the same (zero) either way --
                    # the spec's own language is "shrinks the Safety Stock to
                    # free up working capital", and shrinking needs something
                    # to shrink FROM. Added in days, the same units
                    # target_coverage_days already uses.
                    #
                    # Bounded by the MEASURED shelf life so an unreliable
                    # supplier can inflate the safety term without inflating
                    # the order past what the product can physically survive
                    # on the shelf -- the shelf life is a ceiling on what can
                    # be HELD, not a target; see order_up_to.clamp_level and
                    # shelf_life_days.json for the same rule applied to the
                    # derived model.
                    fresh_with_safety = target_coverage_days + safety_buffer
                    shelf_life = _ou.shelf_life_for(
                        p.get('department') or p.get('product_category') or '')
                    if shelf_life > 0 and fresh_with_safety > shelf_life:
                        target_coverage_days = shelf_life
                        rec['reasoning'] += (
                            f" [LATA Fresh Safety +{safety_buffer:.2f}d -> "
                            f"{fresh_with_safety:.2f}d, shelf-life clamped to "
                            f"{shelf_life:.1f}d]")
                    else:
                        target_coverage_days = fresh_with_safety
                        rec['reasoning'] += (
                            f" [LATA Fresh Safety +{safety_buffer:.2f}d -> "
                            f"{target_coverage_days:.2f}d]")
                    rec['reasoning'] += f" [Fresh DDoS Target: {target_coverage_days:.2f}d]"
                else:
                    gap_days = int(p.get('median_gap_days', 7))
                    if gap_days < 1: gap_days = 1
                    lead_time = int(p.get('lead_time_days', 1) or 1)
                    
                    min_cycle_coverage = gap_days + lead_time + safety_buffer
                    target_coverage_days = max(target_coverage_days, min_cycle_coverage)
                    rec['reasoning'] += f" [DDoS Target: {target_coverage_days:.2f}d]"

                target_stock = avg_daily_sales * target_coverage_days
                
                # Net Requirement
                net_req = target_stock - (current_stock + on_order)
                
                if net_req > 0:
                    # 3. KEY SKU BOOST
                    is_top_sku = p.get('is_top_sku', False) or p.get('sales_rank', 999) < 500
                    if is_top_sku:
                        net_req *= 1.20
                        rec['reasoning'] += " [Key SKU Boost +20%]"
                    
                    rec['recommended_quantity'] = net_req
                    rec['reasoning'] += f" [Net Req: {net_req:.1f} (Tgt {target_stock:.1f} - Cur {current_stock} - Ord {on_order})]"
                else:
                     rec['reasoning'] += " [Adequate Coverage]"
            else:
                 # "[Above ROP 0.0]" on a line with no measured demand is true
                 # and useless: it reads as a healthy, well-stocked SKU when
                 # the engine has no idea whether it should be stocked at all.
                 # Say which it is. Note this branch ASSIGNS rather than
                 # appends, so anything set earlier is discarded here -- the
                 # flag above is on rec, not in the string, for that reason.
                 if rec.get('ads_missing'):
                     rec['reasoning'] = (
                         f" [no demand signal: never sold here, "
                         f"{current_stock:.0f} on hand — review or delist]")
                 else:
                     rec['reasoning'] = f" [Above ROP {reorder_point:.1f}]"

            recommendations.append(rec)

        # TASK A: report the chain-wide join rate for THIS batch's order
        # lines -- how many actually resolved a measured sigma_L rather than
        # the fallback constant. Logs (see order_up_to.check_join_rate); does
        # not raise, because a thin batch (a handful of SKUs, a new store) is
        # expected to look this way and should not crash a live scan -- the
        # probe (devkit/probe_lata_wiring.py) is where a floor breach should
        # fail a build.
        if _ou.is_enabled():
            _ou.check_join_rate()

        return recommendations

    def finalize_orders(self, recommendations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Apply strict safety guards (Rounding, Caps, Etc.)
        """
        # Need to rebuild product map for guards
        products_map = {r['product_name']: r for r in recommendations}
        return apply_safety_guards(recommendations, products_map, allocation_mode="replenishment")

    def apply_minimum_order_gate(self, finalized_recs: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Two-stage Minimum Order Gate.
        
        Stage 1: SKU-level MOQ/MOP screening.
                 Filters out individual items that do not meet the minimum SKU-level
                 order quantity or price. Routes them to transfers (TRANSFER_FIRST).
                 
        Stage 2: Supplier-level MOT screening.
                 Groups remaining items by supplier and verifies if the entire PO meets
                 the supplier-level unit or value threshold. Below-threshold suppliers
                 are routed to transfers (TRANSFER_FIRST).
                 
        Returns:
            {
                'po_recs': [items passing both stages — keep on PO],
                'transfer_recs': [items routed to transfers — Stage 1 or 2],
                'no_order': [items with quantity <= 0],
                'supplier_summary': {supplier: {units, value, status, item_count}}
            }
        """
        min_units = self.thresholds.get('min_order_units', 10)
        min_value = self.thresholds.get('min_order_value_kes', 5000)
        
        po_candidate_recs = []
        transfer_recs = []
        
        # --- STAGE 1: SKU-level MOQ/MOP gate ---
        for rec in finalized_recs:
            qty = rec.get('recommended_quantity', 0)
            if qty <= 0:
                continue
                
            is_fresh = rec.get('is_fresh', False)
            pack_size = max(1, int(rec.get('pack_size', 1)))
            cost_price = float(rec.get('cost_price', rec.get('sell_price', rec.get('selling_price', 0))))
            
            # Determine Item MOQ (Minimum Order Quantity)
            # Default fallback MOQ: pack_size for fresh, max(pack_size, 5) for dry
            db_moq = rec.get('moq_floor', 0)
            if db_moq > 0:
                item_moq = max(pack_size, db_moq)
            else:
                item_moq = pack_size if is_fresh else max(pack_size, 2)
                
            # Determine Item MOP (Minimum Order Price / Order Value)
            # Default fallback MOP: KES 200 for fresh, KES 100 for dry
            #
            # CONFIGURABLE, because it is a POLICY in shillings applied to
            # every store regardless of size. min_order_units and
            # min_order_value_kes below have always been settable; these two
            # were literals, and being absolute they do not adapt: a small
            # branch generates a smaller order from the same SKU and so fails
            # a fixed floor more often. Measured across the book, this gate
            # removed 239 lines at 0.3x against 65 at 2.5x.
            #
            # The defaults are unchanged, so nothing moves for an operator who
            # does not set them.
            item_mop = float(self.thresholds.get(
                'min_item_value_fresh_kes', 200.0) if is_fresh
                else self.thresholds.get('min_item_value_dry_kes', 100.0))
            
            # Evaluate individual item eligibility
            order_val = qty * cost_price
            meets_moq = qty >= item_moq
            meets_mop = order_val >= item_mop
            
            if not meets_moq or not meets_mop:
                # Routed to transfers at Stage 1 (SKU Gate)
                rec['fulfillment'] = 'TRANSFER_FIRST'
                fail_reason = []
                if not meets_moq:
                    fail_reason.append(f"{qty:.0f} units < MOQ {item_moq}")
                if not meets_mop:
                    fail_reason.append(f"value KES {order_val:,.0f} < MOP KES {item_mop:.0f}")
                
                rec['reasoning'] = (
                    rec.get('reasoning', '') +
                    f" [Item MOQ/MOP Gate: {', '.join(fail_reason)} — routed to transfer]"
                )
                transfer_recs.append(rec)
            else:
                # Eligible for PO grouping
                po_candidate_recs.append(rec)
                
        # --- STAGE 2: Supplier-level MOT gate ---
        supplier_groups: Dict[str, List[Dict[str, Any]]] = {}
        for rec in po_candidate_recs:
            supplier = rec.get('supplier_name', rec.get('supplier', 'UNKNOWN'))
            if supplier not in supplier_groups:
                supplier_groups[supplier] = []
            supplier_groups[supplier].append(rec)
            
        po_recs = []
        supplier_summary = {}
        
        for supplier, recs in supplier_groups.items():
            total_units = sum(r.get('recommended_quantity', 0) for r in recs)
            total_value = sum(
                r.get('recommended_quantity', 0) * r.get('cost_price', r.get('sell_price', r.get('selling_price', 0)))
                for r in recs
            )
            
            meets_units = total_units >= min_units
            meets_value = total_value >= min_value
            
            if meets_units or meets_value:
                # Meets supplier MOT — remains on PO
                for r in recs:
                    r['fulfillment'] = 'SUPPLIER_PO'
                po_recs.extend(recs)
                supplier_summary[supplier] = {
                    'units': total_units, 'value': total_value,
                    'status': 'PO', 'item_count': len(recs)
                }
            else:
                # Fails supplier MOT — routed to transfer at Stage 2
                for r in recs:
                    r['fulfillment'] = 'TRANSFER_FIRST'
                    r['reasoning'] = (
                        r.get('reasoning', '') +
                        f" [Below MOT: {total_units:.0f} units / KES {total_value:,.0f}"
                        f" — threshold {min_units} units or KES {min_value:,.0f}"
                        f" — routed to transfer]"
                    )
                transfer_recs.extend(recs)
                supplier_summary[supplier] = {
                    'units': total_units, 'value': total_value,
                    'status': 'TRANSFER', 'item_count': len(recs)
                }
                
        # Items with 0 quantity (no order needed) pass through unchanged
        no_order = [r for r in finalized_recs if r.get('recommended_quantity', 0) <= 0]
        
        return {
            'po_recs': po_recs,
            'transfer_recs': transfer_recs,
            'no_order': no_order,
            'supplier_summary': supplier_summary,
        }
