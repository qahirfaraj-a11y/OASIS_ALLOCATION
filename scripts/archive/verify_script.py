import os
import sys

class DummyEngine: pass

if __name__ == '__main__':
    from oasis.logic.procurement_mixin import ProcurementMixin
    class Engine(ProcurementMixin):
        def __init__(self):
            super().__init__()
            class DummyBudget:
                def spend_from_wallet(self, w, d, c): pass
            self.budget_manager = DummyBudget()
            self.simulation_mode = True
            
    engine = Engine()
    
    recs = [
        {
            'itm_cd': 'TEST001',
            'product_name': 'PREMIUM AIR FRYER 12L',
            'pack_size': 12,
            'cost_est': 15000.0,
            'price_est': 20000.0,
            'moq_floor': 1,
            'min_display_qty': 3,
            'category': 'ELECTRONICS',
            'department': 'ELECTRONICS',
            'is_fresh': False
        },
        {
            'itm_cd': 'TEST002',
            'product_name': 'CHEAP SNACK',
            'pack_size': 24,
            'cost_est': 50.0,
            'price_est': 100.0,
            'moq_floor': 24,
            'min_display_qty': 12,
            'category': 'GROCERY',
            'department': 'GROCERY',
            'is_fresh': False
        }
    ]
    
    try:
        res = engine.apply_greenfield_allocation(recs, total_budget=100000.0)
        print("AIR FRYER TARGET:", res['orders'][0]['target_qty'], res['orders'][0]['reasoning'])
        print("SNACK TARGET:", res['orders'][1]['target_qty'], res['orders'][1]['reasoning'])
    except Exception as e:
        print(f"Error: {e}")
