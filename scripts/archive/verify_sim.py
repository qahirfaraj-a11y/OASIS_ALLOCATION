
from intraday_sim import IntraDaySimulator
import time
import os

db_path = 'oasis/data/mock_pos_erp.db'
if not os.path.exists(db_path):
    print(f"Error: {db_path} not found")
    exit(1)

print("Initialising Simulator...")
start = time.time()
sim = IntraDaySimulator.from_db(db_path)
print(f"Simulator initialised in {time.time()-start:.2f}s")

print("Advancing to hour 14...")
start = time.time()
state = sim.advance_to_hour(14)
print(f"Advanced in {time.time()-start:.2f}s")

print(f"Sales rows: {len(state['sales_rows'])}")
print(f"Stockouts: {len(state['stockouts'])}")
print(f"Transfers: {len(state['transfers'])}")

# Check specific store
selected_org = "ORG001"
store_sales = [r for r in state['sales_rows'] if r['org_cd'] == selected_org]
print(f"Sales for {selected_org}: {len(store_sales)}")
