
import streamlit as st
import pandas as pd

# Page config
st.set_page_config(page_title="🧭 APEC-RISE Scenario Shift Monitor", layout="wide")

# Title block
st.title("🧭 APEC-RISE Scenario Shift Monitor")
st.markdown("Use this tool to detect and explore scenario shifts based on media sentiment and risk signals from across APEC economies.")

# Intro section
st.markdown("### 🔍 What This Tool Does")
st.markdown("""
This monitor helps identify whether current trends in political and institutional conditions suggest a shift from **baseline** to either **optimistic** or **pessimistic** scenarios for key APEC-RISE reform areas.
It integrates automatically generated risk signals informed by recent media coverage.
""")

# Load risk signals
try:
    signal_df = pd.read_csv("data/risk_signals.csv")
    signal_df.columns = signal_df.columns.str.strip()
except Exception as e:
    st.error(f"❌ Could not load risk signals: {e}")
    st.stop()

# Load scenario matrix
try:
    matrix_df = pd.read_csv("scenario-shift-monitor/data/full_apec_rise_scenario_matrix.csv")
    matrix_df.columns = matrix_df.columns.str.strip()
except:
    matrix_df = pd.DataFrame()

# Sidebar filters
with st.sidebar:
    st.header("🔍 Filter Risk Signals")

    # Assumption filter
    assumptions = sorted(signal_df["Assumption"].dropna().unique())
    selected_assumption = st.selectbox("Select Assumption", ["All"] + assumptions)

    # Economy filter
    economies = sorted(signal_df["Economy"].dropna().unique())
    selected_economy = st.selectbox("Select Economy", ["All"] + economies)

    # Workstream filter
    workstreams = sorted(signal_df["Workstream"].dropna().unique())
    selected_workstream = st.selectbox("Select Workstream", ["All"] + workstreams)

    # Scenario filter (forced options)
    all_scenarios = ["Baseline", "Optimistic", "Pessimistic"]
    scenarios = sorted(set(all_scenarios).union(signal_df["Scenario"].dropna().unique()))
    selected_scenario = st.selectbox("Select Scenario", ["All"] + scenarios)

    # Signal Strength filter
    strengths = sorted(signal_df["Signal Strength"].dropna().unique())
    selected_strength = st.selectbox("Select Signal Strength", ["All"] + list(strengths))

# Apply filters
filtered = signal_df.copy()

if selected_assumption != "All":
    filtered = filtered[filtered["Assumption"] == selected_assumption]
if selected_economy != "All":
    filtered = filtered[filtered["Economy"] == selected_economy]
if selected_workstream != "All":
    filtered = filtered[filtered["Workstream"] == selected_workstream]
if selected_scenario != "All":
    filtered = filtered[filtered["Scenario"] == selected_scenario]
if selected_strength != "All":
    filtered = filtered[filtered["Signal Strength"] == selected_strength]

# Display filtered signals
st.markdown("### 📋 Filtered Scenario Signals")
if filtered.empty:
    st.info("No matching signals found.")
else:
    st.dataframe(filtered, use_container_width=True)

# Scenario breakdown summary
st.markdown("### 🧭 Scenario Breakdown")
summary = filtered.groupby(["Economy", "Workstream", "Scenario"]).size().reset_index(name="Signals")
st.dataframe(summary, use_container_width=True)

# Optional scenario matrix display
if not matrix_df.empty:
    st.markdown("### 🧩 Scenario Matrix Reference")
    st.dataframe(matrix_df, use_container_width=True)
