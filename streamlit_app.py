import streamlit as st

# ── Page config ──
st.set_page_config(page_title="APEC-RISE Risk Monitoring Suite", layout="wide")

# ── Landing page ──
st.title("🚨 APEC-RISE Risk Monitoring Suite")
st.write("Select a tool below to open it in a new tab:")

# Full URLs for each deployed sub-app
links = {
    "📡 Media Monitor": "https://us-apec-rise-media-monitor.streamlit.app/",
    "🧭 Scenario Simulator": "https://us-apec-rise-risk-monitoring-suite-scenario-simulator.streamlit.app/",
    "🏛 Institutional Mapping Tool": "https://us-apec-rise-risk-monitoring-suite-institutional-map.streamlit.app/"
}

# Render links as clickable headings
for title, url in links.items():
    st.markdown(
        f'<a href="{url}" target="_blank" style="font-size:24px; text-decoration:none;">{title}</a>',
        unsafe_allow_html=True
    )
