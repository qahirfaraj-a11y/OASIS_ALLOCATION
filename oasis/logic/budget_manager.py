import json
import csv
import logging
import os
from typing import Dict, List, Any
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
        1. Category is Critical (Rice, Sugar, Flour, Oil, Milk, Maize Meal)
        2. Velocity is High (> 1.0 unit/day) - Implying it's a fast mover in a staple category.
        """
        name_clean = product_name.strip().upper()
        if name_clean in self.staples:
            return True
            
        # Fallback Heuristic
        if category:
            dept = category.strip().upper()
            # critical_depts = ['RICE', 'SUGAR', 'FLOUR', 'COOKING OIL', 'FRESH MILK', 'MAIZE MEAL']
            if dept in ESSENTIAL_DEPARTMENTS and velocity >= 1.0:
                 # High velocity item in a critical department -> Treat as Staple
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
        
        # Reserve 2% of budget for zero-weight departments (split among them)
        ORPHAN_RESERVE_PCT = 0.02
        orphan_min = (total_budget * ORPHAN_RESERVE_PCT / max(1, zero_weight_count)) if zero_weight_count > 0 else 0
        
        # Calculate Base Department Pot from Scaling Ratios
        for dept, weight in self.scaling_ratios.items():
            if weight > 0:
                allocated = total_budget * weight
            else:
                # v3.2 FIX (GAP 4): Orphan departments get minimum allocation
                allocated = orphan_min
                logger.debug(f"Orphan dept {dept} allocated minimum: ${allocated:.2f}")
            
            wallets[dept] = {
                'allocated_budget': allocated,
                'max_budget': allocated * (1.0 + buffer_pct),
                'spent': 0.0,
                'remaining': allocated * (1.0 + buffer_pct) # Start with max available including buffer
            }
            
        # Default bucket for unknown departments (not in scaling ratios at all)
        wallets['GENERAL'] = {
            'allocated_budget': total_budget * 0.05, # 5% contingency
            'max_budget': total_budget * 0.10,
            'spent': 0.0,
            'remaining': total_budget * 0.10
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
        """Deducts cost from the specific wallet."""
        dept = department.upper().strip()
        if dept not in wallets:
            dept = 'GENERAL'
            
        wallets[dept]['spent'] += cost
        wallets[dept]['remaining'] -= cost
