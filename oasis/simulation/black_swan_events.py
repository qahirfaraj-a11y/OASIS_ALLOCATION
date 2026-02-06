"""
Black Swan Event Definitions for Retail Simulation
==================================================
Provides structured event classes for:
1. Supplier Failures (critical node disruption)
2. Competitive Environment Changes (market pressure)
3. External Shocks (future: economic, weather, etc.)

Usage:
    from oasis.simulation.black_swan_events import SupplierFailureEvent, CompetitiveEvent, SCENARIO_TEMPLATES
    
    # Create a custom supplier failure
    failure = SupplierFailureEvent(
        supplier_name="BROOKSIDE",
        start_day=10,
        duration_days=14
    )
    
    # Use a predefined competitive scenario
    carrefour = SCENARIO_TEMPLATES["carrefour_100m"]
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import Enum

logger = logging.getLogger("BlackSwanEvents")


class FailureMode(Enum):
    """How severely the supplier failure impacts supply."""
    COMPLETE = "complete"      # No supply at all
    PARTIAL = "partial"        # 50% capacity reduction
    DELAYED = "delayed"        # 2x lead times


class EventType(Enum):
    """Types of competitive/market events."""
    SUPPLIER_FAILURE = "supplier_failure"
    NEW_COMPETITOR = "new_competitor"
    COMPETITOR_EXIT = "competitor_exit"
    PRICE_WAR = "price_war"


# Department sensitivity to competitive pressure
# Higher = more vulnerable to competitor entry
DEPARTMENT_SENSITIVITY = {
    'FRESH MILK': 1.4,      # High traffic driver
    'BREAD': 1.3,           # Daily necessity
    'VEGETABLES': 1.5,      # Fresh preference
    'FRUITS': 1.4,          # Fresh preference
    'FRESH GOURMET': 1.3,   # Quality sensitive
    'EGGS': 1.2,            # Convenience
    'BEER': 1.2,            # Convenience purchase
    'SOFT DRINKS': 1.2,     # Impulse/convenience
    'MINERAL WATER': 1.1,   # Convenience
    'CIGARETTES': 0.6,      # Brand loyalty
    'COSMETICS': 0.7,       # Brand loyalty
    'HAIR CARE': 0.7,       # Brand loyalty
    'COOKING OIL': 0.9,     # Price sensitive but habit
    'RICE': 0.9,            # Staple, less price elastic
    'FLOUR': 0.85,          # Staple
    'SUGAR': 0.85,          # Staple
    'HOUSEHOLD': 0.8,       # Less frequent purchase
    'CLEANING': 0.8,        # Less frequent
    'DEFAULT': 1.0          # Neutral impact
}


@dataclass
class SupplierFailureEvent:
    """
    Models a critical supplier failure.
    
    Use Cases:
    - Supplier bankruptcy
    - Transportation strike
    - Factory fire/closure
    - Import ban/port congestion
    
    Attributes:
        supplier_name: Name of the failing supplier
        start_day: Simulation day when failure begins
        duration_days: How long the failure lasts (14 = 2 weeks)
        mode: COMPLETE (no supply), PARTIAL (50%), or DELAYED (2x lead time)
        department_filter: Only fail this department's SKUs (None = all)
    """
    supplier_name: str
    start_day: int
    duration_days: int = 14
    mode: FailureMode = FailureMode.COMPLETE
    department_filter: Optional[str] = None
    
    # Auto-populated when triggered
    affected_skus: List[str] = field(default_factory=list)
    original_lead_times: Dict[str, int] = field(default_factory=dict)
    estimated_lost_revenue: float = 0.0
    is_active: bool = False
    
    def is_active_on_day(self, day: int) -> bool:
        """Check if failure is active on a given day."""
        return self.start_day <= day < (self.start_day + self.duration_days)
    
    def get_end_day(self) -> int:
        """Return the day when failure ends."""
        return self.start_day + self.duration_days


@dataclass
class CompetitiveEvent:
    """
    Models competitive pressure on sales with gradual erosion.
    
    Use Cases:
    - New hypermarket opening nearby (Carrefour, Naivas)
    - Competitor closure (opportunity)
    - Price war in the market
    
    Attributes:
        event_type: NEW_COMPETITOR, COMPETITOR_EXIT, or PRICE_WAR
        competitor_name: Name of the competitor
        distance_meters: How close (affects impact severity)
        impact_pct: YoY sales change (negative = loss, positive = gain)
        start_day: When the event begins
        ramp_up_days: Days to reach full impact (gradual erosion)
        department_impacts: Override sensitivity by department
    """
    event_type: EventType
    competitor_name: str
    distance_meters: int
    impact_pct: float  # -6.0 = 6% decline, +4.0 = 4% gain
    start_day: int = 1
    ramp_up_days: int = 30
    department_impacts: Dict[str, float] = field(default_factory=dict)
    
    def get_multiplier_for_day(self, day: int, department: str = None) -> float:
        """
        Calculate demand multiplier with gradual ramp-up and department sensitivity.
        
        Args:
            day: Current simulation day
            department: Product department for sensitivity adjustment
            
        Returns:
            Multiplier (e.g., 0.94 for 6% decline at full ramp)
            
        Example:
            Day 1:  1.0 * 0.02 progress = 0.12% impact → multiplier ~0.999
            Day 15: 1.0 * 0.50 progress = 3.0% impact → multiplier ~0.970
            Day 30: 1.0 * 1.00 progress = 6.0% impact → multiplier ~0.940
        """
        if day < self.start_day:
            return 1.0
        
        # Calculate ramp progress (0.0 to 1.0)
        days_active = day - self.start_day
        ramp_progress = min(1.0, days_active / max(1, self.ramp_up_days))
        
        # Base impact at full ramp (e.g., -0.06 for 6% decline)
        base_impact = self.impact_pct / 100.0
        
        # Current impact based on ramp progress
        current_impact = base_impact * ramp_progress
        
        # Department sensitivity multiplier
        if department:
            # Check custom overrides first, then global defaults
            if department.upper() in self.department_impacts:
                sensitivity = self.department_impacts[department.upper()]
            else:
                sensitivity = DEPARTMENT_SENSITIVITY.get(department.upper(), 
                             DEPARTMENT_SENSITIVITY['DEFAULT'])
        else:
            sensitivity = 1.0
        
        # Apply sensitivity (higher sensitivity = bigger impact)
        adjusted_impact = current_impact * sensitivity
        
        # Return multiplier (1.0 + negative impact = less than 1.0)
        return max(0.5, 1.0 + adjusted_impact)  # Floor at 50% to avoid unrealistic collapse
    
    def get_cumulative_impact(self, days: int) -> float:
        """
        Calculate total cumulative demand lost over simulation period.
        Useful for impact reporting.
        """
        total_impact = 0.0
        for day in range(1, days + 1):
            mult = self.get_multiplier_for_day(day)
            daily_loss = 1.0 - mult
            total_impact += daily_loss
        return total_impact


class SupplierRiskAnalyzer:
    """
    Analyzes supplier concentration and identifies critical suppliers.
    """
    
    def __init__(self):
        self.concentration_cache: Dict[str, Dict] = {}
    
    def analyze_department_concentration(self, 
                                         inventory: Dict[str, Any]) -> Dict[str, Dict]:
        """
        Calculate supplier share within each department.
        
        Returns:
            {
                'FRESH MILK': {
                    'BROOKSIDE': {'share_pct': 45.2, 'sku_count': 12, 'revenue_potential': 500000},
                    'KINANGOP': {'share_pct': 28.1, 'sku_count': 8, 'revenue_potential': 280000}
                },
                'BREAD': {...}
            }
        """
        dept_supplier_data: Dict[str, Dict[str, Dict]] = {}
        dept_totals: Dict[str, float] = {}
        
        for sku, data in inventory.items():
            dept = str(data.get('department', data.get('product_category', 'UNKNOWN'))).upper()
            supplier = str(data.get('supplier', 'UNKNOWN')).upper().strip()
            
            # Calculate potential revenue (ADS * Price)
            ads = float(data.get('avg_daily_sales', 0))
            price = float(data.get('price', data.get('selling_price', 0)))
            potential = ads * price * 30  # Monthly potential
            
            if dept not in dept_supplier_data:
                dept_supplier_data[dept] = {}
                dept_totals[dept] = 0.0
            
            if supplier not in dept_supplier_data[dept]:
                dept_supplier_data[dept][supplier] = {
                    'sku_count': 0,
                    'revenue_potential': 0.0,
                    'skus': []
                }
            
            dept_supplier_data[dept][supplier]['sku_count'] += 1
            dept_supplier_data[dept][supplier]['revenue_potential'] += potential
            dept_supplier_data[dept][supplier]['skus'].append(sku)
            dept_totals[dept] += potential
        
        # Calculate shares
        result = {}
        for dept, suppliers in dept_supplier_data.items():
            result[dept] = {}
            dept_total = dept_totals.get(dept, 1.0)
            
            for supplier, data in suppliers.items():
                share = (data['revenue_potential'] / dept_total * 100) if dept_total > 0 else 0
                result[dept][supplier] = {
                    'share_pct': round(share, 1),
                    'sku_count': data['sku_count'],
                    'revenue_potential': round(data['revenue_potential'], 0),
                    'skus': data['skus']
                }
        
        self.concentration_cache = result
        return result
    
    def identify_critical_suppliers(self, 
                                    inventory: Dict[str, Any],
                                    min_share_pct: float = 30.0,
                                    min_revenue_potential: float = 100000) -> List[Dict]:
        """
        Find suppliers that would cause significant disruption if they failed.
        
        Critical = >30% share of any department OR >100K monthly revenue potential
        
        Returns:
            [
                {
                    'supplier': 'BROOKSIDE',
                    'department': 'FRESH MILK',
                    'share_pct': 45.2,
                    'revenue_at_risk': 500000,
                    'sku_count': 12
                },
                ...
            ]
        """
        if not self.concentration_cache:
            self.analyze_department_concentration(inventory)
        
        critical = []
        
        for dept, suppliers in self.concentration_cache.items():
            for supplier, data in suppliers.items():
                if supplier == 'UNKNOWN':
                    continue
                    
                is_critical = (
                    data['share_pct'] >= min_share_pct or 
                    data['revenue_potential'] >= min_revenue_potential
                )
                
                if is_critical:
                    critical.append({
                        'supplier': supplier,
                        'department': dept,
                        'share_pct': data['share_pct'],
                        'revenue_at_risk': data['revenue_potential'],
                        'sku_count': data['sku_count']
                    })
        
        # Sort by revenue at risk (highest first)
        critical.sort(key=lambda x: x['revenue_at_risk'], reverse=True)
        return critical
    
    def get_top_supplier_for_department(self, 
                                        inventory: Dict[str, Any],
                                        department: str) -> Optional[str]:
        """
        Get the supplier with highest share in a department.
        Useful for "fail the top supplier" scenarios.
        """
        if not self.concentration_cache:
            self.analyze_department_concentration(inventory)
        
        dept_data = self.concentration_cache.get(department.upper(), {})
        if not dept_data:
            return None
        
        # Find supplier with highest share (excluding UNKNOWN)
        top_supplier = None
        top_share = 0.0
        
        for supplier, data in dept_data.items():
            if supplier != 'UNKNOWN' and data['share_pct'] > top_share:
                top_share = data['share_pct']
                top_supplier = supplier
        
        return top_supplier
    
    def calculate_hhi(self, inventory: Dict[str, Any], 
                      department: str = None) -> float:
        """
        Calculate Herfindahl-Hirschman Index for supplier concentration.
        
        HHI = Sum of (Market Share %)^2
        Range: 0 (Perfect Diversity) to 10,000 (Monopoly)
        
        Interpretation:
        - <1500: Competitive/Low Concentration
        - 1500-2500: Moderate Concentration
        - >2500: High Concentration (risky)
        """
        if not self.concentration_cache:
            self.analyze_department_concentration(inventory)
        
        if department:
            # Single department HHI
            dept_data = self.concentration_cache.get(department.upper(), {})
            hhi = sum(data['share_pct'] ** 2 for data in dept_data.values())
        else:
            # Overall store HHI (aggregate across all departments)
            all_shares = []
            total_revenue = sum(
                sum(s['revenue_potential'] for s in dept.values())
                for dept in self.concentration_cache.values()
            )
            
            if total_revenue <= 0:
                return 0.0
            
            supplier_totals: Dict[str, float] = {}
            for dept_data in self.concentration_cache.values():
                for supplier, data in dept_data.items():
                    if supplier not in supplier_totals:
                        supplier_totals[supplier] = 0.0
                    supplier_totals[supplier] += data['revenue_potential']
            
            hhi = sum((rev / total_revenue * 100) ** 2 for rev in supplier_totals.values())
        
        return round(hhi, 2)


# Predefined Scenario Templates
SCENARIO_TEMPLATES = {
    "carrefour_100m": CompetitiveEvent(
        event_type=EventType.NEW_COMPETITOR,
        competitor_name="Carrefour",
        distance_meters=100,
        impact_pct=-6.0,
        start_day=1,
        ramp_up_days=30,
        department_impacts={
            'FRESH MILK': 1.4,
            'BREAD': 1.3,
            'VEGETABLES': 1.5,
            'FRUITS': 1.4,
            'BEER': 1.2,
            'SOFT DRINKS': 1.2,
        }
    ),
    
    "naivas_200m": CompetitiveEvent(
        event_type=EventType.NEW_COMPETITOR,
        competitor_name="Naivas",
        distance_meters=200,
        impact_pct=-4.5,
        start_day=1,
        ramp_up_days=30
    ),
    
    "quickmart_500m": CompetitiveEvent(
        event_type=EventType.NEW_COMPETITOR,
        competitor_name="QuickMart",
        distance_meters=500,
        impact_pct=-3.0,
        ramp_up_days=45
    ),
    
    "competitor_exit_nearby": CompetitiveEvent(
        event_type=EventType.COMPETITOR_EXIT,
        competitor_name="Tuskys",
        distance_meters=200,
        impact_pct=+4.0,  # Positive = traffic gain
        start_day=1,
        ramp_up_days=14   # Faster ramp for exit (customers shift quickly)
    ),
    
    "price_war_aggressive": CompetitiveEvent(
        event_type=EventType.PRICE_WAR,
        competitor_name="Market",
        distance_meters=0,
        impact_pct=-8.0,
        ramp_up_days=7,
        department_impacts={
            'COOKING OIL': 1.5,  # Price wars hit commodities hard
            'RICE': 1.4,
            'SUGAR': 1.4,
            'FLOUR': 1.3,
        }
    )
}

# Predefined Supplier Failure Scenarios
SUPPLIER_FAILURE_TEMPLATES = {
    "major_dairy_failure": SupplierFailureEvent(
        supplier_name="BROOKSIDE",  # Will be auto-selected if None
        start_day=10,
        duration_days=14,
        mode=FailureMode.COMPLETE,
        department_filter="FRESH MILK"
    ),
    
    "logistics_disruption": SupplierFailureEvent(
        supplier_name="",  # To be filled dynamically
        start_day=5,
        duration_days=7,
        mode=FailureMode.DELAYED  # 2x lead times, not complete failure
    ),
    
    "import_ban": SupplierFailureEvent(
        supplier_name="",  # Typically affects imported goods suppliers
        start_day=1,
        duration_days=30,
        mode=FailureMode.COMPLETE
    )
}
