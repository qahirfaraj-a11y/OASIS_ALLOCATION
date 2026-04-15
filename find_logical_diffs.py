import difflib
import sys
import re

# Set output to utf-8
if sys.stdout.encoding != 'utf-8':
    try:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except:
        pass

def normalize_logic(lines):
    # Remove whitespace-only lines and trailing whitespace
    # Also ignore comments for logic comparison? Maybe keep them but normalize
    normalized = []
    for line in lines:
        # Strip and ignore if empty
        clean = line.strip()
        if clean:
            # We keep code lines. We can also normalize internal spacing if we want.
            # But let's just do strip() and keep as is for now.
            normalized.append(clean + '\n')
    return normalized

def compare_files(file1: str, file2: str):
    with open(file1, 'r', encoding='utf-8') as f1, open(file2, 'r', encoding='utf-8') as f2:
        lines1 = f1.readlines()
        lines2 = f2.readlines()
    
    norm1 = normalize_logic(lines1)
    norm2 = normalize_logic(lines2)
    
    diff = difflib.unified_diff(norm1, norm2, fromfile=str(file1), tofile=str(file2), n=0)
    return list(diff)

file_oasis = r'C:\Users\iLink\.gemini\antigravity\scratch\oasis\logic\order_engine.py'
file_fixed = r'C:\Users\iLink\Downloads\paste_fixed.py'

diff_output = compare_files(file_oasis, file_fixed)
if not diff_output:
    print("Files are logically identical (ignoring whitespace).")
else:
    print(f"Found {len(diff_output)} logic differences:")
    for line in diff_output:
        print(line, end='')
