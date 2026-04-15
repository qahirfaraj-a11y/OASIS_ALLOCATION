# Application Ecosystem

O.A.S.I.S. provides multiple interfaces tailored to different user roles and operational tasks.

## 1. Ops Dashboard (Streamlit)
- **File**: `ops_dashboard.py`
- **Role**: The central "Operations Command Center".
- **Features**:
    - Live sales monitoring (Simulated).
    - Transfer intelligence (Inter-store stock balancing).
    - One-click PO generation.

## 2. ST-GAT Market Pulse (GNN)
- **File**: `st_gat_dashboard.py`
- **Role**: Strategic intelligence using Spatio-Temporal Graph Attention Networks.
- **Features**:
    - Risk triage (high-risk store identification).
    - Attention mapping (influence between store locations).
    - Traffic friction analysis.

## 3. O.A.S.I.S. Main App (Flet)
- **File**: `oasis/main.py`
- **Role**: Local desktop utility for batch processing.
- **Features**:
    - File upload and enrichment.
    - Rule-based or Local LLM (llama-cpp) analysis of SKU lists.

## 4. Simulation Lab
- **File**: `oasis/simulation/simulation_engine.py`
- **Role**: Sandbox for stress-testing.
- **Features**:
    - Black Swan event injection (Supplier failure).
    - Monte Carlo sales simulation.

[[Architecture_Overview|Back to Overview]]
