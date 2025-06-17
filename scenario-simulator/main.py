
import streamlit as st
import os

# Page config
st.set_page_config(page_title="US-APEC-RISE Risk Monitoring Suite", layout="wide")

st.title("🇺🇸 US-APEC-RISE Risk Monitoring Suite")
st.markdown("Integrated tools to monitor reform risk, media sentiment, institutional shifts, and scenario adaptation.")

# Define apps
apps = {
    "📡 Media Monitor": {
        "description": "Tracks leadership changes, media sentiment, and alignment with U.S. objectives across APEC economies.",
        "path": "media-monitor/app.py"
    },
    "🧭 Scenario Shift Monitor": {
        "description": "Explore risk-informed shifts from baseline to optimistic or pessimistic conditions.",
        "path": "scenario-shift-monitor/app.py"
    },
    "🏛 Institutional Mapping Tool": {
        "description": "Visualize reform ownership, coordination, and institutional alignment across APEC economies.",
        "path": "institutional-map/app.py"
    }
}

# Display app cards
for name, app in apps.items():
    st.subheader(name)
    st.markdown(f"_{app['description']}_")
    st.markdown(f"[🔗 Launch {name}]({app['path']})")
    st.markdown("---")
