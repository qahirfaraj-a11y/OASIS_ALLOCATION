
import random
import logging
from typing import List, Dict, Any, Tuple

logger = logging.getLogger("SimulationEngine")

class SalesSimulator:
    """
    Simulates daily sales behavior using Monte Carlo methods based on 
    product velocity (Mean) and volatility (CV).
    """
    def __init__(self, seed: int = 42):
        random.seed(seed)
        # Day Factors: Multipliers for each day of the week (0=Mon, 6=Sun)
        self.day_factors = {
            0: 0.9,  # Mon (Slow)
            1: 0.95, # Tue
            2: 1.0,  # Wed
            3: 1.05, # Thu
            4: 1.2,  # Fri (Payday/Party)
            5: 1.3,  # Sat (Shopping)
            6: 0.8   # Sun (Quiet)
        }
        
    def get_day_factor(self, day_index: int) -> float:
        """Returns the demand multiplier for a given day index."""
        # Assuming Simulation Day 0 is Monday
        dow = day_index % 7
        return self.day_factors.get(dow, 1.0)

    def simulate_daily_unit_sales(self, 
                                avg_daily_sales: float, 
                                cv: float = 0.5, 
                                day_index: int = 0,
                                month_factor: float = 1.0,
                                trend_multiplier: float = 1.0,
                                store_scale_factor: float = 1.0) -> int:
        """
        Generates a stochastic sales number for a single day.
        Logic: Normal Distribution (Mean=Daily * Factors * Scale, StdDev=Mean*CV).
        Returns: Integer units sold (>= 0).
        """
        if avg_daily_sales <= 0:
            return 0
            
        # 1. Apply Multipliers
        day_factor = self.get_day_factor(day_index)
        
        # Base Mean modulated by Calendar, Seasonality, Item Trend AND Store Scale
        adjusted_mean = avg_daily_sales * day_factor * month_factor * trend_multiplier * store_scale_factor
        
        # 2. Calculate Volatility
        # If CV is missing/zero, assume a default volatility (e.g. 0.4)
        if cv <= 0: cv = 0.4
        std_dev = adjusted_mean * cv
        
        # 3. Generate Random Sample (Monte Carlo)
        daily_units = random.gauss(adjusted_mean, std_dev)
        
        # 4. Enforce Physical Reality
        final_units = max(0, int(round(daily_units)))
        
        return final_units


class InventoryTracker:
    """
    Manages the 'Physical State' of the store during simulation.
    Tracks Stock Levels, Lost Sales, and Replenishment Status.
    """
    def __init__(self):
        self.inventory: Dict[str, Dict[str, Any]] = {} # SKU -> {stock, lost_sales, etc}
        self.pending_orders: List[Dict] = [] # List of {sku, qty, arrival_day}
        self.total_lost_revenue = 0.0
        self.total_revenue = 0.0
        
    def initialize_stock(self, recommendations: List[Dict[str, Any]]):
        """
        Loads the 'Day 1 Allocation' into the inventory system.
        """
        count = 0
        for rec in recommendations:
            sku = rec['product_name']
            qty = rec.get('recommended_quantity', 0)
            
            if qty > 0:
                self.inventory[sku] = {
                    'current_stock': qty,
                    'max_stock': qty, # Initial allocation sets the 'Planogram Cap'
                    'avg_daily_sales': rec.get('avg_daily_sales', 0),
                    'price': rec.get('selling_price', 0),
                    'cv': rec.get('coefficient_of_variation', 0.5), # Assuming passed in rec
                    'supplier': rec.get('supplier_name', 'UNKNOWN'),
                    'department': rec.get('product_category', 'UNKNOWN'),  # Added for risk analysis
                    'lead_time_days': rec.get('lead_time_days', 2),
                    'trend_multiplier': rec.get('trend_multiplier', 1.0),
                    'total_sold': 0,
                    'lost_sales_units': 0
                }
                count += 1
        logger.info(f"InventoryTracker: Initialized {count} SKUs from allocation.")

    def receive_stock(self, day_index: int) -> int:
        """
        Checks pending orders and receives stock if arrival_day <= current_day.
        Returns number of SKUs updated.
        """
        received_count = 0
        remaining_orders = []
        
        for order in self.pending_orders:
            if order['arrival_day'] <= day_index:
                sku = order['sku']
                qty = order['qty']
                if sku in self.inventory:
                    self.inventory[sku]['current_stock'] += qty
                    received_count += 1
            else:
                remaining_orders.append(order)
                
        self.pending_orders = remaining_orders
        return received_count

    def process_daily_sales(self, simulator: SalesSimulator, day_index: int, 
                          month_factor: float = 1.0, 
                          store_scale_factor: float = 1.0) -> Dict[str, Any]:
        """
        Runs one day of sales for ALL items in inventory.
        Returns daily summary.
        """
        daily_revenue = 0.0
        daily_lost_revenue = 0.0
        units_sold = 0
        stockouts = 0
        
        for sku, data in self.inventory.items():
            # 1. Generate Demand
            # Use item-specific trend stored in data (default 1.0)
            item_trend = data.get('trend_multiplier', 1.0)
            
            demand = simulator.simulate_daily_unit_sales(
                avg_daily_sales=data['avg_daily_sales'], 
                cv=data['cv'], 
                day_index=day_index,
                month_factor=month_factor,
                trend_multiplier=item_trend,
                store_scale_factor=store_scale_factor
            )
            
            # 2. Fulfill Demand
            current_stock = data['current_stock']
            
            if current_stock >= demand:
                # Full sale
                sold = demand
                data['current_stock'] -= sold
            else:
                # Partial sale + Stockout
                sold = current_stock
                lost = demand - current_stock
                data['current_stock'] = 0
                
                # Create Loss Record
                data['lost_sales_units'] += lost
                loss_val = lost * data['price']
                daily_lost_revenue += loss_val
                stockouts += 1
            
            # 3. Update Totals
            data['total_sold'] += sold
            daily_revenue += (sold * data['price'])
            units_sold += sold
            
        self.total_revenue += daily_revenue
        self.total_lost_revenue += daily_lost_revenue
        
        return {
            'day': day_index,
            'revenue': daily_revenue,
            'lost_revenue': daily_lost_revenue,
            'units_sold': units_sold,
            'stockouts': stockouts
        }
    
    def get_stock_status(self) -> List[Dict]:
        """Returns snapshot of current inventory."""
        snapshot = []
        for sku, data in self.inventory.items():
            snapshot.append({
                'sku': sku,
                'stock': data['current_stock'],
                'lost_sales': data['lost_sales_units']
            })
        return snapshot


class RiskModel:
    """
    Analyzes Supplier Concentration and injects Black Swan events.
    Now integrates with black_swan_events module for comprehensive risk modeling.
    """
    def __init__(self):
        from .black_swan_events import SupplierRiskAnalyzer, SupplierFailureEvent, CompetitiveEvent, FailureMode
        self.risk_analyzer = SupplierRiskAnalyzer()
        self.active_supplier_failures: Dict[str, Dict] = {}  # {supplier: {event, original_lead_times}}
        self.competitive_event: Any = None
        
    def calculate_hhi_concentration(self, inventory: Dict[str, Any], department: str = None) -> float:
        """
        Calculates Herfindahl-Hirschman Index (HHI) for Supplier Concentration.
        HHI = Sum of (Market Share %)^2.
        Range: 0 (Perfect Diversity) to 10,000 (Monopoly).
        
        Args:
            inventory: SKU inventory dict
            department: Optional - calculate HHI for specific department only
        """
        return self.risk_analyzer.calculate_hhi(inventory, department)
    
    def analyze_department_concentration(self, inventory: Dict[str, Any]) -> Dict:
        """
        Analyze supplier concentration by department.
        Returns share percentages for each supplier in each department.
        """
        return self.risk_analyzer.analyze_department_concentration(inventory)
    
    def identify_critical_suppliers(self, inventory: Dict[str, Any],
                                    min_share_pct: float = 30.0,
                                    min_revenue_potential: float = 100000) -> List[Dict]:
        """
        Find suppliers that pose concentration risk.
        Critical if >30% share of any department OR >100K revenue potential.
        """
        return self.risk_analyzer.identify_critical_suppliers(
            inventory, min_share_pct, min_revenue_potential
        )
    
    def get_top_supplier_for_department(self, inventory: Dict[str, Any], 
                                        department: str) -> str:
        """Get the dominant supplier for a specific department."""
        return self.risk_analyzer.get_top_supplier_for_department(inventory, department)

    def trigger_supplier_failure(self, inventory: Dict[str, Any], 
                                 supplier_name: str = None,
                                 department: str = None,
                                 duration_days: int = 14,
                                 failure_mode: str = "COMPLETE") -> Dict:
        """
        Injects a supplier failure into the simulation.
        
        Args:
            inventory: SKU inventory dict
            supplier_name: Specific supplier to fail (e.g., "BROOKSIDE")
            department: If supplier_name is None, auto-select top supplier of this dept
            duration_days: How long the failure lasts
            failure_mode: "COMPLETE" (no supply), "PARTIAL" (50%), or "DELAYED" (2x lead time)
            
        Returns:
            Impact summary with affected SKUs and estimated revenue at risk
        """
        from .black_swan_events import FailureMode
        
        # Auto-select top supplier if not specified
        if not supplier_name and department:
            supplier_name = self.get_top_supplier_for_department(inventory, department)
            if not supplier_name:
                logger.warning(f"No supplier found for department {department}")
                return {'blocked_skus': [], 'revenue_at_risk': 0}
        
        if not supplier_name:
            logger.warning("No supplier specified for failure event")
            return {'blocked_skus': [], 'revenue_at_risk': 0}
        
        supplier_upper = supplier_name.upper().strip()
        blocked_skus = []
        original_lead_times = {}
        revenue_at_risk = 0.0
        
        # Parse failure mode
        mode = FailureMode.COMPLETE
        if failure_mode.upper() == "PARTIAL":
            mode = FailureMode.PARTIAL
        elif failure_mode.upper() == "DELAYED":
            mode = FailureMode.DELAYED
        
        # Apply failure to matching SKUs
        for sku, data in inventory.items():
            sku_supplier = str(data.get('supplier', '')).upper().strip()
            
            if sku_supplier == supplier_upper:
                # Store original lead time for restoration
                original_lead_times[sku] = data.get('lead_time_days', 7)
                
                # Apply failure effect
                if mode == FailureMode.COMPLETE:
                    data['lead_time_days'] = 9999  # Effectively infinite
                    data['is_blocked'] = True
                elif mode == FailureMode.PARTIAL:
                    data['supply_capacity'] = 0.5  # 50% capacity
                elif mode == FailureMode.DELAYED:
                    data['lead_time_days'] = original_lead_times[sku] * 2
                
                blocked_skus.append(sku)
                revenue_at_risk += data.get('avg_daily_sales', 0) * data.get('price', 0) * 30
        
        # Track active failure
        self.active_supplier_failures[supplier_upper] = {
            'original_lead_times': original_lead_times,
            'duration_days': duration_days,
            'mode': mode,
            'blocked_skus': blocked_skus
        }
        
        logger.info(f"⚠️ SUPPLIER FAILURE: {supplier_name} - {len(blocked_skus)} SKUs blocked, ${revenue_at_risk:,.0f} at risk")
        
        return {
            'supplier': supplier_name,
            'blocked_skus': blocked_skus,
            'revenue_at_risk': revenue_at_risk,
            'mode': failure_mode
        }
    
    def restore_supplier(self, inventory: Dict[str, Any], supplier_name: str) -> int:
        """
        Restore a failed supplier back to normal operation.
        
        Returns:
            Number of SKUs restored
        """
        supplier_upper = supplier_name.upper().strip()
        
        if supplier_upper not in self.active_supplier_failures:
            logger.warning(f"Supplier {supplier_name} not in active failures")
            return 0
        
        failure_data = self.active_supplier_failures[supplier_upper]
        original_lead_times = failure_data['original_lead_times']
        restored_count = 0
        
        for sku, data in inventory.items():
            if sku in original_lead_times:
                data['lead_time_days'] = original_lead_times[sku]
                data['is_blocked'] = False
                data.pop('supply_capacity', None)
                restored_count += 1
        
        del self.active_supplier_failures[supplier_upper]
        logger.info(f"✓ SUPPLIER RESTORED: {supplier_name} - {restored_count} SKUs back online")
        
        return restored_count
    
    def set_competitive_event(self, event) -> None:
        """
        Set a competitive event (e.g., Carrefour opening nearby).
        The event's get_multiplier_for_day() will be used to erode demand.
        """
        self.competitive_event = event
        logger.info(f"🏪 COMPETITIVE EVENT: {event.competitor_name} - {event.impact_pct:+.1f}% impact over {event.ramp_up_days} days")
    
    def get_demand_multiplier(self, day_index: int, department: str = None) -> float:
        """
        Get the combined demand multiplier for a given day.
        Incorporates competitive pressure if an event is active.
        """
        if self.competitive_event:
            return self.competitive_event.get_multiplier_for_day(day_index, department)
        return 1.0
    
    # === NEW: Supplier Dropdown Methods for Interactive Selection ===
    
    def get_top_suppliers_for_dropdown(self, 
                                       inventory: Dict[str, Any],
                                       department: str,
                                       top_n: int = 10) -> List[Dict]:
        """
        Returns top N suppliers for a department dropdown.
        
        Args:
            inventory: SKU inventory dict from simulation
            department: Target department
            top_n: Number of suppliers to return
            
        Returns:
            List of dicts: [
                {'supplier': 'BROOKSIDE', 'share_pct': 45.2, 'sku_count': 12, 'revenue_potential': 250000},
                ...
            ]
        """
        dept_upper = department.upper().strip()
        
        # Group SKUs by supplier for this department
        supplier_stats = {}
        total_revenue = 0.0
        
        for sku, data in inventory.items():
            sku_dept = str(data.get('department', '')).upper().strip()
            if sku_dept != dept_upper:
                continue
                
            supplier = str(data.get('supplier', 'Unknown')).strip()
            ads = data.get('avg_daily_sales', 0)
            price = data.get('price', 0)
            revenue_potential = ads * price * 30  # Monthly proxy
            
            if supplier not in supplier_stats:
                supplier_stats[supplier] = {
                    'supplier': supplier,
                    'sku_count': 0,
                    'revenue_potential': 0.0
                }
            
            supplier_stats[supplier]['sku_count'] += 1
            supplier_stats[supplier]['revenue_potential'] += revenue_potential
            total_revenue += revenue_potential
        
        # Calculate share percentages
        for stats in supplier_stats.values():
            if total_revenue > 0:
                stats['share_pct'] = (stats['revenue_potential'] / total_revenue) * 100
            else:
                stats['share_pct'] = 0.0
        
        # Sort by revenue potential (descending) and take top N
        sorted_suppliers = sorted(
            supplier_stats.values(),
            key=lambda x: x['revenue_potential'],
            reverse=True
        )[:top_n]
        
        return sorted_suppliers
    
    def analyze_supplier_failure_impact(self,
                                        inventory: Dict[str, Any],
                                        supplier_name: str,
                                        department: str = None) -> Dict:
        """
        Pre-simulation analysis: What happens if this supplier fails?
        Provides impact preview before running full simulation.
        
        Returns:
            {
                'affected_skus': [...],
                'affected_sku_count': int,
                'revenue_at_risk': float,
                'coverage_loss_pct': float,
                'substitute_availability': float (0-1),
                'estimated_stockout_days': int,
                'severity': str ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')
            }
        """
        supplier_upper = supplier_name.upper().strip()
        dept_upper = department.upper().strip() if department else None
        
        affected_skus = []
        revenue_at_risk = 0.0
        total_revenue = 0.0
        
        # Other suppliers in department for substitute analysis
        other_suppliers_skus = 0
        
        for sku, data in inventory.items():
            sku_supplier = str(data.get('supplier', '')).upper().strip()
            sku_dept = str(data.get('department', '')).upper().strip()
            
            # Filter by department if specified
            if dept_upper and sku_dept != dept_upper:
                continue
            
            ads = data.get('avg_daily_sales', 0)
            price = data.get('price', 0)
            monthly_revenue = ads * price * 30
            total_revenue += monthly_revenue
            
            if sku_supplier == supplier_upper:
                affected_skus.append(sku)
                revenue_at_risk += monthly_revenue
            else:
                other_suppliers_skus += 1
        
        # Calculate metrics
        affected_count = len(affected_skus)
        coverage_loss_pct = (revenue_at_risk / total_revenue * 100) if total_revenue > 0 else 0
        
        # Substitute availability: ratio of other SKUs that could sub
        total_affected_dept_skus = affected_count + other_suppliers_skus
        substitute_availability = (other_suppliers_skus / total_affected_dept_skus) if total_affected_dept_skus > 0 else 0
        
        # Estimated stockout days based on coverage loss
        if coverage_loss_pct >= 50:
            estimated_stockout_days = 14  # Severe
        elif coverage_loss_pct >= 30:
            estimated_stockout_days = 10
        elif coverage_loss_pct >= 15:
            estimated_stockout_days = 7
        else:
            estimated_stockout_days = 3
        
        # Severity rating
        if coverage_loss_pct >= 40:
            severity = 'CRITICAL'
        elif coverage_loss_pct >= 20:
            severity = 'HIGH'
        elif coverage_loss_pct >= 10:
            severity = 'MEDIUM'
        else:
            severity = 'LOW'
        
        return {
            'affected_skus': affected_skus[:20],  # Limit list size
            'affected_sku_count': affected_count,
            'revenue_at_risk': revenue_at_risk,
            'coverage_loss_pct': coverage_loss_pct,
            'substitute_availability': substitute_availability,
            'estimated_stockout_days': estimated_stockout_days,
            'severity': severity
        }


class OnlineStoreDemandModifier:
    """
    Modifies demand patterns for KENYAN online grocery archetype.
    Key differences from physical retail:
    
    1. Fresh/Artisanal BOOST: Online = premium fresh produce destination
    2. Daily Essentials: High-frequency orders for everyday items
    3. Supplier Concentration: Fewer suppliers = higher disruption risk
    4. Predictable Demand: Regular customers, recurring baskets
    """
    
    # Categories that see HIGHER demand online in Kenya
    FRESH_BOOST_CATEGORIES = ['FRESH MILK', 'BREAD', 'VEGETABLES', 'FRUITS', 'CHEESE', 'YOGHURT']
    ARTISANAL_CATEGORIES = ['CHEESE', 'BAKERY FOODPLUS', 'CAKES', 'DELI', 'BAKERY']
    DAILY_ESSENTIALS = ['FRESH MILK', 'BREAD', 'EGGS', 'COOKING OIL', 'RICE & PASTA']
    
    def __init__(self, store_config: dict):
        self.is_online = store_config.get('is_online', False)
        self.fresh_boost = store_config.get('fresh_demand_boost', 1.4)  # +40%
        self.artisanal_boost = store_config.get('artisanal_demand_boost', 1.3)  # +30%
        self.supplier_risk = store_config.get('supplier_concentration_risk', 1.5)  # +50% impact
    
    def apply_online_demand_adjustment(self, department: str, base_demand: float) -> float:
        """Adjust demand for Kenyan online grocery patterns."""
        if not self.is_online:
            return base_demand
            
        adjusted = base_demand
        dept_upper = department.upper().strip()
        
        # 1. BOOST fresh categories (opposite of typical e-commerce!)
        fresh_match = any(cat in dept_upper for cat in self.FRESH_BOOST_CATEGORIES)
        if fresh_match:
            adjusted *= self.fresh_boost
        
        # 2. Boost artisanal/specialty items
        artisanal_match = any(cat in dept_upper for cat in self.ARTISANAL_CATEGORIES)
        if artisanal_match:
            adjusted *= self.artisanal_boost
        
        return adjusted
    
    def get_supplier_failure_impact_multiplier(self) -> float:
        """Online stores more vulnerable due to concentrated suppliers."""
        if self.is_online:
            return self.supplier_risk
        return 1.0


class ReplenishmentLogic:
    """
    Automated Ordering System ("The Autopilot").
    Checks stock levels against ROP and generates draft orders.
    """
    def __init__(self, check_frequency_days: int = 1):
        self.check_frequency = check_frequency_days
        
    def check_for_reorder(self, inventory: Dict[str, Any], day_index: int, month_factor: float = 1.0) -> List[Dict]:
        """
        Scans inventory for items below ROP.
        ROP = (LeadTime + SafetyDays) * AvgSales * MonthFactor
        """
        orders = []
        
        # Only check on periodic days
        if day_index % self.check_frequency != 0:
            return []
            
        for sku, data in inventory.items():
            # Skip if already has order incoming? (Simplification: Ignore pending for now)
            
            lead_time = data.get('lead_time_days', 2) # Default 2 days
            safety_stock_days = 3 # Policy
            
            avg_daily = data.get('avg_daily_sales', 0)
            if avg_daily <= 0: continue
            
            # v5.5 FIX: Seasonality-Aware ROP
            # If Jan demand is 3x, we need 3x the ROP trigger.
            adjusted_sales = avg_daily * month_factor
            
            rop = (lead_time + safety_stock_days) * adjusted_sales
            
            if data['current_stock'] <= rop:
                # ORDER!
                # EOQ or Min Max? Let's use simple "Fill to Max" (Order up to 7 days + Safety)
                target_stock = adjusted_sales * (lead_time + safety_stock_days + 7) # +7 Cycle Stock
                qty_needed = target_stock - data['current_stock']
                
                # Round to pack size (if we had it)
                qty_order = max(1, int(qty_needed))
                
                orders.append({
                    'sku': sku,
                    'qty': qty_order,
                    'est_cost': qty_order * (data['price'] * 0.8), # Approx Cost
                    'supplier': data.get('supplier', 'UNKNOWN'),
                    'lead_time_days': lead_time
                })
        
        return orders

