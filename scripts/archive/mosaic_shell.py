import streamlit as st

st.set_page_config(
    page_title="OASIS Mosaic Portal", 
    page_icon="🧊", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Unified CSS injection for a cohesive Next.js-like visual framework
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&family=JetBrains+Mono:wght@400;700&display=swap');
    
    :root {
        --neon-emerald: #10b981;
        --deep-midnight: #0a0b10;
        --glass-bg: rgba(255, 255, 255, 0.03);
    }

    .stApp {
        background-color: var(--deep-midnight);
        color: #d1d1d1;
        font-family: 'Outfit', sans-serif;
    }

    /* Override Sidebar Elements to unify navigation */
    [data-testid="stSidebar"] {
        background-color: #050508;
        border-right: 1px solid rgba(255,255,255,0.05);
    }
</style>
""", unsafe_allow_html=True)

st.title("🧊 O.A.S.I.S. Unified Mosaic Portal")
st.markdown("### The Executive Command Layer")

st.info("👈 Please select a Mosaic Engine from the Navigation Sidebar.")

st.markdown("""
Welcome to the fully decoupled **O.A.S.I.S. Architecture**.

This single unified window connects directly to the lightweight `FastAPI` microservices running securely in the background. 
- You no longer have multiple terminals.
- You no longer face SQLite database locks.
- Memory consumption is fundamentally consolidated.
""")
