import os
import json

overview_path = r"C:\Users\iLink\.gemini\antigravity\brain\1bac4dd1-6749-46b0-9be2-c691593f61e0\.system_generated\logs\overview.txt"
with open(overview_path, 'r', encoding='utf-8', errors='ignore') as f:
    lines = f.readlines()

for idx in range(125, 146):
    if idx < len(lines):
        line = lines[idx]
        try:
            data = json.loads(line)
            # print in a nice format
            print(f"Step {data.get('step_index')}: {data.get('source')} - {data.get('type')}")
            if data.get('content'):
                print(f"  Content: {data.get('content')[:300]}")
            if data.get('tool_calls'):
                print(f"  Tool Calls: {data.get('tool_calls')}")
        except Exception as e:
            print(f"L{idx} Error: {e}, Raw: {line[:200]}")
