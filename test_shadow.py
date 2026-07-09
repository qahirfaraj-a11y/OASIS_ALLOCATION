from oasis.logic.shadow_mode import ShadowModeEngine
from shadow_report_generator import generate_shadow_report
import os

base_dir = os.path.dirname(os.path.abspath(__file__))
sc_path = os.path.join(base_dir, 'Full_Product_Allocation_Scorecard_v7.csv')
hum_paths = [
    os.path.join(base_dir, 'oasis', 'data', 'po_3-4.xlsx'),
    os.path.join(base_dir, 'oasis', 'data', 'po_5-6.xlsx'),
    os.path.join(base_dir, 'oasis', 'data', 'po_7-8.xlsx'),
    os.path.join(base_dir, 'oasis', 'data', 'po_9-10.xlsx'),
    os.path.join(base_dir, 'oasis', 'data', 'po_1-2.xlsx'),
    os.path.join(base_dir, 'oasis', 'data', 'po_1-11.xlsx')
]

engine = ShadowModeEngine(base_dir)
engine.run_shadow_cycle(sc_path)
engine.ingest_human_orders(hum_paths)
comp = engine.generate_comparison()
stats = engine.get_summary_stats()

print("Stats:", stats)
doc_bytes = generate_shadow_report(comp, stats)

with open('test_shadow_report.docx', 'wb') as f:
    f.write(doc_bytes.read())

print("Test complete. Generated test_shadow_report.docx")
