import glob
import os

print("=== Search for Kapa or transport in C:/Oasis python files ===")
for f in glob.glob("C:/Oasis/*.py"):
    try:
        with open(f, 'r', encoding='utf-8') as file_obj:
            content = file_obj.read()
            if "KAPA" in content.upper() or "TRANSPORT" in content.upper() or "LOGISTICS" in content.upper():
                print(f"Match in: {os.path.basename(f)}")
                lines = content.split('\n')
                for i, line in enumerate(lines):
                    if any(x in line.upper() for x in ["KAPA", "TRANSPORT", "LOGISTICS", "FREIGHT", "DELIVERY_COST", "TRANSPORT_COST"]):
                        print(f"  Line {i+1}: {line.strip()}")
    except Exception as e:
        pass
