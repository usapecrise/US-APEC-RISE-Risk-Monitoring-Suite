
import streamlit as st
import pandas as pd

# Page config
st.set_page_config(page_title="🧭 APEC-RISE Scenario Shift Monitor", layout="wide")

# Title block
st.title("🧭 APEC-RISE Scenario Shift Monitor")
st.markdown("Use this tool to detect and explore scenario shifts based on media sentiment and risk signals from across APEC economies.")

# Summary intro
st.markdown("### 🔍 What This Tool Does")
st.markdown("""
This monitor helps identify whether current trends in political and institutional conditions suggest a shift from **baseline** to either **optimistic** or **pessimistic** scenarios for key APEC-RISE reform areas.
It integrates automatically generated risk signals informed by recent media coverage.
""")

# Load signals
try:
    df = pd.read_csv("data/risk_signals.csv")
    df.columns = df.columns.str.strip()
except Exception as e:
    st.error(f"❌ Could not load risk signals: {e}")
    st.stop()

# Show signal overview
st.markdown("### 📊 Current Risk Signal Summary")
st.dataframe(df, use_container_width=True)

# Scenario breakdown
st.markdown("### 🧭 Scenario Breakdown")
summary = df.groupby(["Economy", "Workstream", "Scenario"]).size().reset_index(name="Signals")
st.dataframe(summary, use_container_width=True)

# CTA to apply filters
st.markdown("---")
st.markdown("👉 Use the sidebar to filter by economy, workstream, signal strength, or critical assumption.")
