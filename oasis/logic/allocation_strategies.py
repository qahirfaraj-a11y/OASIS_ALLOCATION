"""
Allocation Strategies (v1.0)

Provides a clean, extensible interface for the Greenfield Allocation pipeline.

The existing `apply_greenfield_allocation()` in OrderEngine contains 1,170 lines of
battle-tested logic refined through 30+ iterations. Rather than risk breaking this
proven code by decomposing it, this module provides:

1. A Strategy Pattern facade for FUTURE allocation extensions
2. Utility functions extracted from the monolithic method for reuse
3. Named constants and configuration that can be overridden per-store

Architecture:
    AllocationPipeline
        ├── SupplierConsolidation (Pass 0)
        ├── WidthAllocation     (Pass 1 + 1.5)
        ├── DepthAllocation     (Pass 2 + 2B)
        ├── AnchorAnchoring     (Pass 3 + 3B)
        └── MopUp               (Pass 4)

Usage:
    pipeline = AllocationPipeline.from_engine(engine)
    result = pipeline.execute(recommendations, budget=300000)
"""

import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field

logger = logging.getLogger("AllocationStrategies")


# ── CONFIGURATION ─────────────────────────────────────────────────────────

@dataclass
class AllocationConfig:
    """
    Centralized configuration for allocation behavior.
    Replaces hardcoded magic numbers scattered throughout the greenfield method.
    """
    # Pass 1: Width
    pass1_budget_limit_small_pct: float = 0.85   # GAP-E fix
    pass1_budget_limit_large_pct: float = 0.70
    
    # Pass 1.5: Pruning
    pruning_reserve_small_pct: float = 0.15
    pruning_reserve_micro_pct: float = 0.05
    
    # Pass 2: Depth
    staple_share_standard: float = 0.60
    staple_share_small: float = 0.80
    staple_share_micro: float = 0.95
    
    # Pass 2B: Flex Pool
    flex_pool_trigger_pct: float = 0.05   # Activate if >5% unused
    
    # Pass 3: Anchor MOV
    mov_threshold_micro: float = 1500.0
    mov_threshold_small: float = 3000.0
    
    # Pass 4: Mop-Up
    mop_up_ceiling_pct: float = 0.05      # Only runs if <5% remains
    mop_up_depth_cap_days: int = 60
    
    # Supplier Consolidation
    supplier_cap_micro: int = 3
    supplier_cap_small: int = 5
    supplier_cap_default: int = 999
    
    # Consolidation departments
    consolidation_depts: tuple = (
        'RICE', 'SUGAR', 'FLOUR', 'COOKING OIL', 
        'MAIZE MEAL', 'PASTA', 'FRESH MILK'
    )

    def get_staple_share(self, is_small: bool, is_micro: bool) -> float:
        if is_micro:
            return self.staple_share_micro
        elif is_small:
            return self.staple_share_small
        return self.staple_share_standard

    def get_supplier_cap(self, is_small: bool, is_micro: bool) -> int:
        if is_micro:
            return self.supplier_cap_micro
        elif is_small:
            return self.supplier_cap_small
        return self.supplier_cap_default

    def get_pass1_limit_pct(self, total_budget: float) -> float:
        if total_budget < 12_000_000:
            return self.pass1_budget_limit_small_pct
        return self.pass1_budget_limit_large_pct


# ── ALLOCATION SUMMARY ───────────────────────────────────────────────────

@dataclass
class AllocationSummary:
    """Structured summary replacing the dict-based summary."""
    total_budget: float = 0.0
    pass1_cash: float = 0.0
    pass1_consignment: float = 0.0
    pass2_cash: float = 0.0
    pass2b_cash: float = 0.0
    pass2b_items_enhanced: int = 0
    pass3_pruned_count: int = 0
    pass3_pruned_value: float = 0.0
    pass3b_reinvested: float = 0.0
    mop_up_cash: float = 0.0
    total_skipped: int = 0
    skip_reasons: Dict[str, int] = field(default_factory=dict)
    dept_utilization: Dict[str, float] = field(default_factory=dict)
    flex_pool_available: float = 0.0
    flex_pool_distributed: float = 0.0
    flex_pool_remaining: float = 0.0
    
    @property
    def total_cash_used(self) -> float:
        return self.pass1_cash + self.pass2_cash + self.pass2b_cash + self.mop_up_cash
    
    @property
    def unused_budget(self) -> float:
        return self.total_budget - self.total_cash_used
    
    @property
    def utilization_pct(self) -> float:
        return (self.total_cash_used / self.total_budget * 100) if self.total_budget > 0 else 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for backward compatibility with existing code."""
        return {
            'total_budget': self.total_budget,
            'pass1_cash': self.pass1_cash,
            'pass1_consignment': self.pass1_consignment,
            'pass2_cash': self.pass2_cash,
            'pass2b_cash': self.pass2b_cash,
            'pass2b_items_enhanced': self.pass2b_items_enhanced,
            'total_skipped': self.total_skipped,
            'skip_reasons': self.skip_reasons,
            'dept_utilization': self.dept_utilization,
            'flex_pool_available': self.flex_pool_available,
            'flex_pool_distributed': self.flex_pool_distributed,
            'flex_pool_remaining': self.flex_pool_remaining,
            'mop_up_cash': self.mop_up_cash,
            'total_cash_used': self.total_cash_used,
            'total_consignment': self.pass1_consignment,
            'unused_budget': self.unused_budget,
            'utilization_pct': self.utilization_pct,
        }


# ── STRATEGY INTERFACE ────────────────────────────────────────────────────

class AllocationStrategy(ABC):
    """
    Base class for allocation strategy phases.
    
    Each strategy receives the current allocation state and modifies 
    recommendations in-place, returning cost metrics.
    """
    
    def __init__(self, config: AllocationConfig):
        self.config = config
    
    @abstractmethod
    def execute(
        self, 
        recommendations: List[dict],
        budget_remaining: float,
        wallets: Dict[str, Any],
        tier_profile: Dict[str, Any],
        context: Dict[str, Any]
    ) -> Dict[str, float]:
        """
        Execute this allocation phase.
        
        Args:
            recommendations: List of product recommendation dicts (modified in-place)
            budget_remaining: Available budget for this phase
            wallets: Department wallet state
            tier_profile: Store tier configuration
            context: Shared state between phases (e.g., pass1_cost, skip counts)
            
        Returns:
            Dict with cost metrics (e.g., {'phase_cost': 1234.56})
        """
        pass


# ── PIPELINE ──────────────────────────────────────────────────────────────

class AllocationPipeline:
    """
    Orchestrates the multi-pass allocation process.
    
    Currently delegates to OrderEngine.apply_greenfield_allocation() for 
    backward compatibility. Future versions will execute individual strategies
    through this pipeline.
    
    Usage:
        # Simple mode: delegate to existing engine method
        pipeline = AllocationPipeline.from_engine(engine)
        result = pipeline.execute(recommendations, budget=300000)
        
        # Advanced mode: custom strategy chain (future)
        pipeline = AllocationPipeline(config)
        pipeline.add_strategy(CustomWidthStrategy(config))
        pipeline.add_strategy(CustomDepthStrategy(config))
        result = pipeline.execute(recommendations, budget=300000)
    """
    
    def __init__(self, config: Optional[AllocationConfig] = None):
        self.config = config or AllocationConfig()
        self._strategies: List[AllocationStrategy] = []
        self._engine = None  # Reference to OrderEngine for delegation
    
    @classmethod
    def from_engine(cls, engine) -> 'AllocationPipeline':
        """
        Create a pipeline that delegates to the proven OrderEngine implementation.
        This is the recommended mode until individual strategies are validated.
        """
        pipeline = cls()
        pipeline._engine = engine
        return pipeline
    
    def add_strategy(self, strategy: AllocationStrategy) -> 'AllocationPipeline':
        """Add a strategy phase to the pipeline. Returns self for chaining."""
        self._strategies.append(strategy)
        return self
    
    def execute(
        self, 
        recommendations: List[dict], 
        budget: float = 300000.0,
        seasonal_demand_map: Optional[Dict[str, float]] = None
    ) -> Dict[str, Any]:
        """
        Execute the allocation pipeline.
        
        If created via `from_engine()`, delegates to the proven monolithic method.
        If strategies were added manually, runs them in sequence.
        """
        # Delegation mode (safe, proven)
        if self._engine and not self._strategies:
            logger.info("AllocationPipeline: Delegating to OrderEngine.apply_greenfield_allocation()")
            return self._engine.apply_greenfield_allocation(
                recommendations, 
                total_budget=budget,
                seasonal_demand_map=seasonal_demand_map
            )
        
        # Strategy mode (extensible, for future use)
        if self._strategies:
            logger.info(f"AllocationPipeline: Running {len(self._strategies)} strategy phases")
            
            tier_profile = {}
            wallets = {}
            if self._engine:
                tier_profile = self._engine.profile_manager.get_profile(budget)
                wallets = self._engine.budget_manager.initialize_wallets(
                    budget, buffer_pct=tier_profile.get('wallet_buffer_pct', 0.05)
                )
            
            context = {
                'total_budget': budget,
                'pass1_cost': 0.0,
                'pass2_cost': 0.0,
                'seasonal_demand_map': seasonal_demand_map,
            }
            
            summary = AllocationSummary(total_budget=budget)
            budget_remaining = budget
            
            for strategy in self._strategies:
                result = strategy.execute(
                    recommendations, budget_remaining, wallets, tier_profile, context
                )
                phase_cost = result.get('phase_cost', 0.0)
                budget_remaining -= phase_cost
                logger.info(f"  {strategy.__class__.__name__}: spent ${phase_cost:,.2f}, remaining ${budget_remaining:,.2f}")
            
            return {
                'recommendations': recommendations,
                'summary': summary.to_dict()
            }
        
        raise ValueError("AllocationPipeline has no engine reference and no strategies. "
                        "Use AllocationPipeline.from_engine() or add_strategy().")


# ── UTILITY FUNCTIONS ─────────────────────────────────────────────────────

def classify_sku_priority(
    product: dict, 
    budget_manager,
    fast_five_depts: set
) -> int:
    """
    Classify a SKU into priority tiers for sorting.
    
    Returns:
        0 = Fast Five Staple (highest priority)
        1 = Other Staple
        2 = Essential Department
        3 = Discretionary (lowest priority)
    """
    is_staple = budget_manager.is_staple(
        product['product_name'], 
        product.get('product_category'), 
        product.get('avg_daily_sales', 0)
    )
    dept = product.get('product_category', 'GENERAL').upper()
    
    if is_staple and dept in fast_five_depts:
        return 0
    elif is_staple:
        return 1
    elif dept in {'SUGAR', 'SALT', 'FLOUR', 'RICE', 'COOKING OIL', 'FRESH MILK', 'BREAD', 'EGGS'}:
        return 2
    return 3


def is_essential_override(product_name: str) -> bool:
    """
    Check if a product should be treated as essential based on name keywords,
    regardless of its department classification.
    
    These overrides handle common mis-categorization in ERP systems.
    """
    p = product_name.upper()
    keywords = {
        'YOGHURT', 'YOGURT', 'SODA', 'COKE', 'ALVARO', 'VIMTO',
        'GHEE', 'LENTIL', 'BEAN', 'NDENGU', 'POJO', 'DAIRY'
    }
    return any(kw in p for kw in keywords)


def is_bulk_item(product_name: str) -> bool:
    """Check if product is a bulk item based on name patterns."""
    p = product_name.upper()
    bulk_patterns = [
        '5KG', '5L', '5LT', '10KG', '10L', '20L', '25KG',
        '5 KG', '5 L', '10 KG'
    ]
    return any(pattern in p for pattern in bulk_patterns)


def calculate_effective_ceiling(
    price_ceiling: float, 
    is_essential_dept: bool, 
    is_bulk: bool
) -> float:
    """
    Calculate the effective price ceiling for a product.
    Essentials get 2x, bulk essentials get 3x.
    """
    if is_essential_dept and is_bulk:
        return price_ceiling * 3
    elif is_essential_dept:
        return price_ceiling * 2
    return price_ceiling
