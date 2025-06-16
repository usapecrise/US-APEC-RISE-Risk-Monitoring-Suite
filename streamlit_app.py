import streamlit as st
import os
import sys

# Add sub-app directories to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'media-monitor'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'scenario-simulator'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'institutional-map'))

import app as media_monitor_app
import app as scenario_simulator_app
import app as institutional_map_app

# Page setup
st.set_page_config(page_title="APEC-RISE Monitoring Suite", layout="wide")

# Top-level title and intro
st.title("🧠 US APEC-RISE Risk Monitoring Suite")
st.markdown("""
Welcome to the **US APEC-RISE Risk Monitoring Suite**, a set of integrated tools for tracking risks, assessing reform alignment, and visualizing institutional dynamics across APEC economies.
""")

# Tabs for tools
tab1, tab2, tab3 = st.tabs([
    "📡 Media Monitor",
    "🧭 Scenario Simulator",
    "🏛 Institutional Map"
])

# === Media Monitor
with tab1:
    st.subheader("📡 Media Monitor")
    st.markdown("""
Tracks leadership changes, institutional priorities, and alignment with U.S. objectives through real-time media analysis.
""")
    media_monitor_app.main()

# === Scenario Simulator
with tab2:
    st.subheader("🧭 Scenario Simulator")
    st.markdown("""
Explore how changing risk conditions shift assumptions from baseline to optimistic or pessimistic — and what that means for APEC-RISE strategy.
""")
    scenario_simulator_app.main()

# === Institutional Mapping Tool
with tab3:
    st.subheader("🏛 Institutional Mapping Tool")
    st.markdown("""
Visualize networks of influence, reform ownership, and institutional coordination across stakeholders.
""")
    institutional_map_app.main()
