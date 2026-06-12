import os

file_path = r"C:\Users\iLink\.gemini\antigravity\scratch\allocation_app.py"

with open(file_path, 'r') as f:
    lines = f.readlines()

new_lines = []
for i, line in enumerate(lines):
    # Lines 183 to 297 (0-indexed 182 to 296) need to be unindented by 4 spaces
    # Actually wait, let's just do it by checking if we are past line 183
    if 183 <= i <= 297:
        if line.startswith("    "):
            new_lines.append(line[4:])
        else:
            new_lines.append(line)
    elif i == 298 or i == 299:
        # lines 299 and 300 are the else block
        # we can change this to elif st.session_state.get('has_allocation')
        if line.startswith("    else:"):
            new_lines.append("elif st.session_state.get('has_allocation'):\n")
        elif line.startswith("        st.warning"):
            new_lines.append("    st.warning(\"No allocation generated. Check data files or budget settings.\")\n")
        else:
            new_lines.append(line)
    else:
        new_lines.append(line)

with open(file_path, 'w') as f:
    f.writelines(new_lines)

print("Fixed indentation.")
