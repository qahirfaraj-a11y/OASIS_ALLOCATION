import sys, os, traceback
sys.path.insert(0, r'C:\Users\iLink\.gemini\antigravity\scratch')
from intraday_sim import IntraDaySimulator

DB_PATH = os.path.join(r'C:\Users\iLink\.gemini\antigravity\scratch\oasis\data', 'mock_pos_erp_showcase.db')
try:
    sim = IntraDaySimulator.from_db(DB_PATH)
    print('Created sim OK')
    state = sim.advance_to_hour(10)
    print('advance_to_hour(10) keys:', list(state.keys()))
    print('  stockouts:', len(state['stockouts']))
    print('  transfers:', len(state['transfers']))
    for t in state['transfers'][:3]:
        print('  sample transfer:', t.product_name, t.transfer_qty, t.urgency)
except Exception as e:
    print('ERROR:', e)
    traceback.print_exc()
