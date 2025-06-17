import streamlit as st
import pandas as pd

# Page config
st.set_page_config(page_title="🧭 Scenario Simulator", layout="wide")
st.title("🧭 APEC-RISE Scenario Planning Simulator")
st.markdown("Explore risk triggers and adaptation strategies across 21 APEC economies.")

# Load signal data
try:
    signal_df = pd.read_csv("data/risk_signals.csv")
except Exception as e:
    st.error(f"Error loading signal data: {e}")
    st.stop()

# Sidebar filters
with st.sidebar:
    st.header("🔍 Filter Signals")
    economies = sorted(signal_df["Economy"].dropna().unique())
    selected_economy = st.selectbox("Select Economy", ["All"] + economies)

    workstreams = sorted(signal_df["Workstream"].dropna().unique())
    selected_workstream = st.selectbox("Select Workstream", ["All"] + workstreams)

    scenarios = sorted(signal_df["Scenario"].dropna().unique())
    selected_scenario = st.selectbox("Select Scenario", ["All"] + scenarios)

if "Signal Strength" in signal_df.columns:
    strengths = sorted(signal_df["Signal Strength"].dropna().unique())
    selected_strength = st.sidebar.selectbox("Select Signal Strength", ["All"] + list(strengths))
    filtered = filtered[filtered["Signal Strength"] == selected_strength] if selected_strength != "All" else filtered
else:
    st.warning("⚠️ 'Signal Strength' column missing in risk_signals.csv")
    selected_strength = "All

# Apply filters
filtered = signal_df.copy()

if selected_economy != "All":
    filtered = filtered[filtered["Economy"] == selected_economy]
if selected_workstream != "All":
    filtered = filtered[filtered["Workstream"] == selected_workstream]
if selected_scenario != "All":
    filtered = filtered[filtered["Scenario"] == selected_scenario]
if selected_strength != "All":
    filtered = filtered[filtered["Signal Strength"] == selected_strength]

# Show filtered results
st.subheader("📊 Scenario Signals")
if filtered.empty:
    st.info("No matching signals found.")
else:
    st.dataframe(filtered, use_container_width=True)

# Summary stats
st.markdown("### 🔢 Scenario Counts")
summary = filtered.groupby(["Economy", "Workstream", "Scenario"]).size().reset_index(name="Count")
st.dataframe(summary, use_container_width=True)
