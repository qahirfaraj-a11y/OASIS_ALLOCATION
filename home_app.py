"""
O.A.S.I.S. Home — suite launcher entry point.

Run:  streamlit run home_app.py      (or: python entrypoint.py --mode home)

One front door: console cards with live/offline status, license posture,
and the store-system snapshot. The consoles themselves stay separate apps.
"""

import os
import sys

import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oasis.ui.home import render_home_page

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

st.set_page_config(page_title="O.A.S.I.S. — Home", page_icon="🏠",
                   layout="wide", initial_sidebar_state="collapsed")

render_home_page(st, PROJECT_ROOT)
