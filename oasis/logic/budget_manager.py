import json
import csv
import logging
import os
from typing import Dict, Any
from .department_constants import ESSENTIAL_DEPARTMENTS

logger = logging.getLogger("BudgetManager")

class BudgetManager:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.dept_ratios = {}
        self.staples = set()
        self.scaling_ratios = {}
        
        self.load_reference_data()

    def load_reference_data(self):
        """Loads Department Ratios and Golden File (Staples)."""
        # 1. Load Staples (Golden File)
        staple_path = os.path.join(self.data_dir, "staple_products.json")
        if os.path.exists(staple_path):
            try:
                with open(staple_path, 'r', encoding='utf-8') as f:
                    self.staples = set(json.load(f))
                logger.info(f"Loaded {len(self.staples)} staples from Golden File.")
            except Exception as e:
                logger.error(f"Failed to load staples: {e}")
        else:
            logger.warning(f"Staple file not found at {staple_path}")

        # 2. Load Department Scaling Ratios
        ratio_path = os.path.join(self.data_dir, "department_scaling_ratios.csv")
        if os.path.exists(ratio_path):
            try:
                with open(ratio_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        dept = row.get('Department', '').upper().strip()
                        try:
                            weight = float(row.get('Capital_Weight', 0.0))
                            self.scaling_ratios[dept] = weight
                        except ValueError:
                            continue
                logger.info(f"Loaded scaling ratios for {len(self.scaling_ratios)} departments.")
            except Exception as e:
                logger.error(f"Failed to load scaling ratios: {e}")
        else:
            logger.warning(f"Scaling ratios file not found at {ratio_path}")
    def is_staple(self, product_name: str, category: str = None, velocity: float = 0.0) -> bool:
        """
        Checks if product is in the Golden File (Staple list).
        v3.11 (APS-2): Added Heuristic Fallback.
        If missing from Golden File, checks:
        1. Category is Critical (Essential Departments from constants)
        2. Velocity is meaningful (> 0.5 unit/day)
        """
        name_clean = product_name.strip().upper()
        if name_clean in self.staples:
            return True
            
        # Fallback Heuristic
        # FIX H2: Removed self-referential threshold. Use a fixed 0.5 threshold
        # for essential departments — any item with meaningful velocity (>0.5/day)
        # in a critical category qualifies as a staple.
        if category:
            dept = category.strip().upper()
            if dept in ESSENTIAL_DEPARTMENTS and velocity >= 0.5:
                 return True
                 
        return False

    def initialize_wallets(self, total_budget: float, buffer_pct: float = 0.10) -> Dict[str, Dict[str, float]]:
        """
        Creates the master wallet structure partitioned by Department.
        Returns: { 'DEPARTMENT_NAME': { 'budget': X, 'spent': 0, 'buffer_pct': Y } }
        
        v3.2 Enhancement: Provides minimum allocation for departments with 0 weight
        """
        wallets = {}
        
        # Count departments with zero weight for dynamic minimum calculation
        zero_weight_count = sum(1 for w in self.scaling_ratios.values() if w == 0.0)
        
        # v10.9 Enhancement: Dynamic Capital Rebalancing
        # If budget > 20M, we auto-expand the spillover pools to allow for greater 
        # assortment depth in non-staple categories.
        is_large_scale = total_budget >= 20_000_000
        
        # Reserve 2.5% for Liquidity / Flex Pool (Pass 2B) [v10.0 Parity]
        LIQUIDITY_RESERVE_PCT = 0.025
        liquidity_pool = round(total_budget * LIQUIDITY_RESERVE_PCT, 4)
        
        # Reserve for zero-weight departments (split among them)
        # v3.11: Dynamic Orphan Scaling - increase pool for large store builds
        ORPHAN_RESERVE_PCT = 0.05 if is_large_scale else 0.02
        orphan_pool = round(total_budget * ORPHAN_RESERVE_PCT, 4)
        orphan_min = (orphan_pool / max(1, zero_weight_count)) if zero_weight_count > 0 else 0
        
        # Calculate Base Department Pot from Scaling Ratios
        for dept, weight in self.scaling_ratios.items():
            if weight > 0:
                allocated = round(total_budget * weight, 4)
            else:
                # v3.2 FIX (GAP 4): Orphan departments get minimum allocation
                allocated = orphan_min
            
            wallets[dept] = {
                'allocated_budget': allocated,
                'max_budget': round(allocated * (1.0 + buffer_pct), 4),
                'spent': 0.0,
                # BUG 6 FIX: Start at allocated_budget, not max_budget.
                # The buffer is an overdraft allowance, not starting capital.
                'remaining': allocated
            }
            
        # FIX 8: Dynamic General Pool
        # For large budgets, General serves as the primary absorption layer for depth.
        general_allocated = round(total_budget * (0.20 if is_large_scale else 0.10), 4)
        wallets['GENERAL'] = {
            'allocated_budget': general_allocated,
            'max_budget': round(general_allocated * 2.0, 4),
            'spent': 0.0,
            'remaining': round(general_allocated * 2.0, 4)
        }
        
        # Explicit Flex Pool Wallet
        wallets['FLEX_POOL'] = {
            'allocated_budget': liquidity_pool,
            'max_budget': liquidity_pool,
            'spent': 0.0,
            'remaining': liquidity_pool
        }
        
        return wallets


    def check_wallet_availability(self, wallets: Dict[str, Any], department: str, cost: float) -> bool:
        """Checks if the department wallet has enough funds."""
        dept = department.upper().strip()
        if dept not in wallets:
            dept = 'GENERAL'
            
        wallet = wallets[dept]
        return wallet['remaining'] >= cost

    def spend_from_wallet(self, wallets: Dict[str, Any], department: str, cost: float):
        """Deducts cost from the specific wallet. FIX H3: Guards against negative balance."""
        dept = department.upper().strip()
        if dept not in wallets:
            dept = 'GENERAL'
            
        wallet = wallets[dept]
        wallet['spent'] = round(float(wallet['spent']) + float(cost), 2)
        wallet['remaining'] = round(float(wallet['remaining']) - float(cost), 2)
        
        # FIX H3: Warn if wallet goes negative (indicates upstream check was skipped)
        if wallet['remaining'] < 0:
            logger.debug(f"Wallet '{dept}' overdrawn by {abs(wallet['remaining']):.2f}. Clamping to 0.")
            wallet['remaining'] = 0.0
