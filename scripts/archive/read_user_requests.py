import json
import os

def read_requests():
    log_path = r"C:\Users\iLink\.gemini\antigravity\brain\1bac4dd1-6749-46b0-9be2-c691593f61e0\.system_generated\logs\overview.txt"
    with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            try:
                data = json.loads(line)
                if data.get("source") == "USER_EXPLICIT":
                    print(f"Step {data.get('step_index')}: {data.get('content')}")
                    print("-" * 50)
            except Exception as e:
                pass

read_requests()
