import os
path = r"c:\Users\iLink\.gemini\antigravity\scratch\stores_network.json"
print(f"Checking path: {path}")
print(f"Exists: {os.path.exists(path)}")
if os.path.exists(path):
    print(f"Size: {os.path.getsize(path)}")
print(f"CWD: {os.getcwd()}")
print(f"Root files: {os.listdir(os.getcwd())}")
