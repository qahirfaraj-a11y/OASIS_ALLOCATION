import streamlit as st

def format_kes(amount: float) -> str:
    """Standardized KES formatting for the exchange."""
    return f"KES {amount:,.2f}"

def render_risk_badge(risk_tranche: str):
    """Renders a color-coded risk badge with premium fintech styling."""
    tranche_map = {
        "Tier 1: High Yield/Perishables": ("RADON RUBY", "#ff4444", "rgba(255, 68, 68, 0.2)"),
        "Tier 2: Medium Risk/Traffic": ("NEON AMBER", "#ffaa00", "rgba(255, 170, 0, 0.2)"),
        "Tier 3: Low Yield/Staple": ("TERMINAL LIME", "#00ff41", "rgba(0, 255, 65, 0.2)")
    }
    
    label, color, bg_glow = tranche_map.get(risk_tranche, ("UNKNOWN", "#888", "rgba(136, 136, 136, 0.1)"))
    
    st.markdown(f"""
    <div style="display: inline-block; padding: 4px 12px; border-radius: 4px; 
                background: {bg_glow}; border: 1px solid {color}; 
                color: {color}; font-family: 'JetBrains Mono', monospace; 
                font-size: 0.7em; font-weight: 700; letter-spacing: 1px;">
        {label}
    </div>
    """, unsafe_allow_html=True)

def calculate_gpp_health(registry_data: dict) -> float:
    """
    Calculates a GPP health score (0.0 - 1.0).
    Based on Coverage Ratio (GPP Balance / Wp Exposure).
    A score of 1.0 means Coverage >= 3.0x (Optimal).
    """
    ledger = registry_data["global_ledger"]
    active_pos = registry_data["active_positions"]
    
    total_wp_exposure = sum(p["total_cost"] * p["wp_score"] for p in active_pos.values())
    gpp_balance = ledger["gpp_balance"]
    
    if total_wp_exposure == 0: return 1.0
    
    coverage = gpp_balance / total_wp_exposure
    # Health scale: 0.0 Coverage = 0.0 Health, 3.0+ Coverage = 1.0 Health
    health_score = min(1.0, coverage / 3.0)
    return health_score

def render_sparkline(data: list):
    """Renders a simplified sparkline using Streamlit's area chart."""
    if not data: data = [0] * 10
    st.area_chart(data, height=50, use_container_width=True)

def format_mpesa_id() -> str:
    """Generates a mock M-Pesa transaction ID."""
    import random
    import string
    chars = string.ascii_uppercase + string.digits
    return "".join(random.choices(chars, k=10))
