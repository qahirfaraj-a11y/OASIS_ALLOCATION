import json
import re

# Since JSON is truncated, we'll try to extract dictionaries using regex
with open('c:\\Users\\iLink\\.gemini\\antigravity\\scratch\\problems.json', 'r', encoding='utf-8') as f:
    text = f.read()

# Extract all JSON objects manually using a regex to bypass truncation issues
objects = re.findall(r'\{"path":"[^"]+","message":".*?","severity":"[^"]+","startLine":\d+,"endLine":\d+\}', text)

parsed = []
for obj_str in objects:
    try:
        parsed.append(json.loads(obj_str))
    except json.JSONDecodeError:
        pass

import_errors = 0
type_errors = []

for p in parsed:
    msg = p["message"]
    if "Could not find import" in msg:
        import_errors += 1
    elif "is not supported between" in msg and ("Unknown" in msg or "@" in msg or "Error" in msg):
        pass # Likely cascaded from unknown types
    elif "No matching overload found for function" in msg and ("round" in msg or "__getitem__" in msg or "__add__" in msg):
        pass # Mostly cascaded from Any/Unknown types
    elif "Object of class `NoneType`" in msg:
        type_errors.append(f"{p['path'].split('scratch\\\\')[-1]}:{p['startLine']} - {msg}")
    elif "is not assignable to" in msg and ("Unknown" in msg or "@" in msg or "Error" in msg):
        pass
    else:
        type_errors.append(f"{p['path'].split('scratch\\\\')[-1]}:{p['startLine']} - {msg}")

with open('c:\\Users\\iLink\\.gemini\\antigravity\\scratch\\type_errors_summary.txt', 'w', encoding='utf-8') as out:
    out.write(f"Total extracted problems: {len(parsed)}\n")
    out.write(f"Import errors (and their cascades ignored): {import_errors} ignored+cascaded\n")
    out.write(f"Real Type Errors ({len(type_errors)}):\n")
    for t in type_errors:
        out.write(f"{t}\n")
