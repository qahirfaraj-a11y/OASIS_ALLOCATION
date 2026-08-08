import sys
import re

with open('entrypoint.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Strip dev modes from DASHBOARD_MAP
content = re.sub(r'\s*"shadow":\s*"shadow_dashboard\.py",\n', '\n', content)
content = re.sub(r'\s*"approval":\s*"approval_dashboard\.py",\n', '\n', content)
content = re.sub(r'\s*"integrated":\s*"integrated_app\.py",\n', '\n', content)
content = re.sub(r'\s*"pitch":\s*"pitch_app_v2\.py",\n', '\n', content)

# 2. Strip dev modes from choices list in argparse
content = content.replace('"showcase",\n', '')
content = content.replace('"shadow", "simulation", ', '')
content = content.replace('"preflight", ', '')

with open('entrypoint.py', 'w', encoding='utf-8') as f:
    f.write(content)
