import os
import shutil
import time
import subprocess
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KUBER.Demo")

def run_cmd(cmd: str):
    logger.info(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Command Failed: {result.stderr}")
    else:
        print(result.stdout)
    return result.stdout

def run_demo():
    data_dir = "./oasis/data_demo/"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.makedirs(data_dir)
    
    base_cmd = f"python run_kuber_exchange.py --data-dir {data_dir}"
    
    print("\n--- STEP 1: SETUP EXCHANGE ---")
    run_cmd(f"{base_cmd} --action setup")
    
    print("\n--- STEP 2: REGISTER & FUND INVESTOR A (Alice) ---")
    run_cmd(f"{base_cmd} --action fund --investor Alice --capital 50000")
    
    print("\n--- STEP 3: INITIAL STATUS ---")
    run_cmd(f"{base_cmd} --action status")
    
    print("\n--- STEP 4: SIMULATE 15 SECONDS OF SALES ---")
    # We'll run the POS action which is programmed to run for 30s, 
    # but we'll terminate it early or just let it run.
    # For the demo, let's just trigger a few manual sales if we could, 
    # but run_pos_simulation is hardcoded to 30s. Let's just let it run.
    run_cmd(f"{base_cmd} --action pos")
    
    print("\n--- STEP 5: STATUS AFTER SALES ---")
    status_output = run_cmd(f"{base_cmd} --action status")
    
    # Extract a Pos ID to trade
    pos_id = None
    for line in status_output.split('\n'):
        if "POS_" in line:
            pos_id = line.split('(')[0].strip().split()[-1]
            break
            
    if not pos_id:
        # Fallback: find it in the registry manually
        with open(os.path.join(data_dir, "kuber_registry.json"), 'r') as f:
            reg = json.load(f)
            pos_id = list(reg["active_positions"].keys())[0]

    print(f"\n--- STEP 6: ALICE LISTS POSITION {pos_id} ON SECONDARY MARKET (5% Discount) ---")
    run_cmd(f"{base_cmd} --action sell-p2p --pos-id {pos_id} --discount 0.05")
    
    print("\n--- STEP 7: REGISTER INVESTOR B (Bob) & BUY POSITION ---")
    run_cmd(f"{base_cmd} --action fund --investor Bob --capital 20000")
    run_cmd(f"{base_cmd} --action buy-p2p --investor Bob --pos-id {pos_id}")
    
    print("\n--- STEP 8: FINAL STATUS ---")
    run_cmd(f"{base_cmd} --action status")
    
    print("\n--- STEP 9: BOB EXITS VIA SHADOW BANK ---")
    # Find another position if pos_id was sold out
    run_cmd(f"{base_cmd} --action exit-shadow-bank --investor Bob --pos-id {pos_id}")
    
    print("\n--- STEP 10: END OF DEMO STATUS ---")
    run_cmd(f"{base_cmd} --action status")

if __name__ == "__main__":
    import json
    run_demo()
