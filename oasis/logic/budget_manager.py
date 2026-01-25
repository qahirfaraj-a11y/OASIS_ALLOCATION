import json
import csv
import logging
import os
from typing import Dict, List, Any

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

    def is_staple(self, product_name: str) -> bool:
        """Checks if product is in the Golden File (Staple list)."""
        return product_name.strip().upper() in self.staples

    def initialize_wallets(self, total_budget: float, buffer_pct: float = 0.10) -> Dict[str, Dict[str, float]]:
        """
        Creates the master wallet structure partitioned by Department.
        Returns: { 'DEPARTMENT_NAME': { 'budget': X, 'spent': 0, 'buffer_pct': Y } }
        """
        wallets = {}
        
        # Calculate Base Department Pot from Scaling Ratios
        for dept, weight in self.scaling_ratios.items():
            allocated = total_budget * weight
            wallets[dept] = {
                'allocated_budget': allocated,
                'max_budget': allocated * (1.0 + buffer_pct),
                'spent': 0.0,
                'remaining': allocated * (1.0 + buffer_pct) # Start with max available including buffer
            }
            
        # Default bucket for unknown departments
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
