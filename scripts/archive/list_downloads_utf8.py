import os
import glob

print("Files in C:/Users/iLink/Downloads:")
for f in glob.glob(r"C:\Users\iLink\Downloads\*"):
    try:
        print(f.encode('utf-8', errors='ignore').decode('cp1252', errors='ignore'))
    except Exception as e:
        print(f"Error printing: {e}")
