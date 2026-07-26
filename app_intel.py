"""
O.A.S.I.S. — Intelligence Console.

The monitoring counterpart to the Operations Console (app.py). Same
foundation (theme, auth, role model, telemetry) via shell.run_console, with
a monitoring-focused page registry: Pulse, Velocity Alerts, Stock Review,
Live Sales, Network Intelligence, Executive ROI, Simulation Lab.

Run:  streamlit run app_intel.py   (or: python entrypoint.py --mode intel)
"""

import os
import sys

import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oasis.ui import shell
from oasis.ui.intel import build_intel_registry

from oasis.logic.onboarding import resolved_db_path

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = resolved_db_path(PROJECT_ROOT)

st.set_page_config(page_title="O.A.S.I.S. — Intelligence", page_icon="⚡",
                   layout="wide", initial_sidebar_state="expanded")

shell.run_console(
    st,
    registry=build_intel_registry(),
    db_path=DB_PATH,
    project_root=PROJECT_ROOT,
    app_title="Intelligence Console",
    license_module="core",
    console_key="intel",        # which console this is (suite bar skips self)
)
