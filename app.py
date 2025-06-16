import streamlit as st

st.set_page_config(page_title="US APEC-RISE Risk Monitoring Suite", layout="centered")

st.title("🌐 US APEC-RISE Risk Monitoring Suite")
st.markdown("""
Welcome to the integrated monitoring dashboard for the APEC-RISE initiative.

Use the tools below to track political and economic signals, assess scenario states, and explore institutional engagement across APEC economies.
""")

st.markdown("### 🧭 Scenario Simulator")
st.markdown("[➡️ Open in new tab](apps/scenario_simulator/app.py)", unsafe_allow_html=True)

st.markdown("### 📡 Media Monitor")
st.markdown("[➡️ Open in new tab](apps/media_monitor/app.py)", unsafe_allow_html=True)

st.markdown("### 🏛️ Institutional Mapping Tool")
st.markdown("[➡️ Open in new tab](apps/institutional_mapping_tool/app.py)", unsafe_allow_html=True)

st.markdown("---")
st.caption("All tools share a common data folder and update automatically via GitHub Actions.")
