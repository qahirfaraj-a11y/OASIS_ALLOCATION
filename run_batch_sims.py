
import subprocess
import os
import sys

# Define profiles
profiles = [
    {"name": "Nano_50k", "budget": 50000},
    {"name": "Nano_170k_FIX", "budget": 170000},
    {"name": "Micro_200k", "budget": 200000},
    {"name": "Micro_500k", "budget": 500000},
    {"name": "Micro_1.2M", "budget": 1200000},
    {"name": "Small_1.6M_FIX", "budget": 1600000},
    {"name": "Mini-Mart_3.5M", "budget": 3570000},
    {"name": "Med_4.1M_FIX", "budget": 4100000},
    {"name": "Medium_5M_FIX", "budget": 5000000},
    {"name": "Supermarket_10M", "budget": 10000000},
    {"name": "Mega_20M", "budget": 20000000}
]

days = 10
month = "FEB"
script_path = "run_simulation_scenario.py"

print(f"--- STARTING BATCH SIMULATION (Duration: {days} Days, Month: {month}) ---")

for p in profiles:
    print(f"\n>> Running Profile: {p['name']} (Budget: ${p['budget']:,.0f})")
    cmd = [
        "python", script_path,
        "--scenario", p['name'],
        "--budget", str(p['budget']),
        "--days", str(days),
        "--month", month
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        # Parse output for Fill Rate Summary
        lines = result.stdout.split('\n')
        # Print Day 1-10 summary lines
        for l in lines:
             if "Day " in l and "Fill Rate" in l:
                  print(f"   {l.strip()}")
        
    except subprocess.CalledProcessError as e:
        print(f"!! CRASHED: {p['name']}")
        print(e.stderr)

print("\n--- BATCH COMPLETE ---")
