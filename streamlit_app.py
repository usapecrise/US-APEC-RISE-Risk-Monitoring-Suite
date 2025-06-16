import streamlit as st
import os
import sys

# === Setup paths to sub-apps ===
sys.path.append(os.path.join(os.path.dirname(__file__), 'media-monitor'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'scenario-simulator'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'institutional-map'))

import app as media_monitor_app
import app as scenario_simulator_app
import app as institutional_map_app

# === Page config ===
st.set_page_config(page_title="USAPEC-RISE Risk Monitoring Suite", layout="wide")

# === Tabs for each app ===
st.title("🧠 US APEC-RISE Risk Monitoring Suite")

tab1, tab2, tab3 = st.tabs(["📡 Media Monitor", "🧭 Scenario Simulator", "🏛 Institutional Map"])

with tab1:
    media_monitor_app.main()

with tab2:
    scenario_simulator_app.main()

with tab3:
    institutional_map_app.main()
