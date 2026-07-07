"""
O.A.S.I.S. — Operations Console (U3).

The operational front door: one login, the SYS v2.9 theme, journey-driven
navigation to the transactional surfaces (ordering, approvals, transfers,
allocation, suppliers, shadow, analytics, settings).

Run:  streamlit run app.py      (or: python entrypoint.py --mode shell)

See app_intel.py for the Intelligence Console (monitoring / alerts).
"""

import os
import sys

import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oasis.ui import shell

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.getenv(
    "OASIS_DB_PATH",
    os.path.join(PROJECT_ROOT, "oasis", "data", "mock_pos_erp.db"),
)

st.set_page_config(page_title="O.A.S.I.S. — Operations", page_icon="◎",
                   layout="wide", initial_sidebar_state="expanded")

shell.run_console(
    st,
    registry=shell.build_registry(),
    db_path=DB_PATH,
    project_root=PROJECT_ROOT,
    app_title="Operations Console",
    license_module="core",
)
