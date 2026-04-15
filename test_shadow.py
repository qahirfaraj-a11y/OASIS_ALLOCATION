from oasis.logic.shadow_mode import ShadowModeEngine
from shadow_report_generator import generate_shadow_report
import os

base_dir = os.path.dirname(os.path.abspath(__file__))
sc_path = os.path.join(base_dir, 'Full_Product_Allocation_Scorecard_v7.csv')
hum_path = os.path.join(base_dir, 'mock_human_po.csv')

engine = ShadowModeEngine(base_dir)
engine.run_shadow_cycle(sc_path)
engine.ingest_human_orders(hum_path)
comp = engine.generate_comparison()
stats = engine.get_summary_stats()

print("Stats:", stats)
doc_bytes = generate_shadow_report(comp, stats)

with open('test_shadow_report.docx', 'wb') as f:
    f.write(doc_bytes.read())

print("Test complete. Generated test_shadow_report.docx")
