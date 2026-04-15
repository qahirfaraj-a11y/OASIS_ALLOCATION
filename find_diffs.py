import difflib
import sys

# Set output to utf-8 to handle special characters in diffs
if sys.stdout.encoding != 'utf-8':
    try:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except:
        pass

def compare_files(file1: str, file2: str):
    with open(file1, 'r', encoding='utf-8') as f1, open(file2, 'r', encoding='utf-8') as f2:
        lines1 = f1.readlines()
        lines2 = f2.readlines()
    
    diff = difflib.unified_diff(lines1, lines2, fromfile=str(file1), tofile=str(file2))
    return list(diff)

file_oasis = r'C:\Users\iLink\.gemini\antigravity\scratch\oasis\logic\order_engine.py'
file_fixed = r'C:\Users\iLink\Downloads\paste_fixed.py'

diff_output = compare_files(file_oasis, file_fixed)
if not diff_output:
    print("Files are identical.")
else:
    for line in diff_output:
        print(line, end='')
