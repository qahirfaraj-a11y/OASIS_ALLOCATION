import streamlit as st

# Initialize session state
if 'input_lat' not in st.session_state:
    st.session_state.input_lat = -1.3000

print(f"DEBUG: Start of script, session_state.input_lat = {st.session_state.input_lat}")

# Number input with format
val = st.number_input("Lat", format="%.4f", key="input_lat")
print(f"DEBUG: After number_input, val = {val}, session_state.input_lat = {st.session_state.input_lat}")

# Mock map update
if st.button("Simulate Map Click"):
    new_lat = -1.2921356
    print(f"DEBUG: Simulating map click with {new_lat}")
    st.session_state.input_lat = new_lat
    st.rerun()
