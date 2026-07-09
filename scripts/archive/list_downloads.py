import os
import glob

print("Files in C:/Users/iLink/Downloads:")
for f in glob.glob(r"C:\Users\iLink\Downloads\*"):
    print(f)
