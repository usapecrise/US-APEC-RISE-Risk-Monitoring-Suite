
import streamlit as st
import pandas as pd

st.set_page_config(page_title="🧭 APEC-RISE Scenario Shift Monitor", layout="wide")
st.title("🧭 APEC-RISE Scenario Shift Monitor")
st.markdown("Use this tool to detect and explore scenario shifts based on media sentiment and risk signals from across APEC economies.")

st.markdown("### 🔍 What This Tool Does")
st.markdown("""
This monitor helps identify whether current trends in political and institutional conditions suggest a shift from **baseline** to either **optimistic** or **pessimistic** scenarios for key APEC-RISE reform areas.
It integrates automatically generated risk signals informed by recent media coverage.
""")

# Load signal data
try:
    signal_df = pd.read_csv("data/risk_signals.csv")
    signal_df.columns = signal_df.columns.str.strip()
except Exception as e:
    st.error(f"❌ Could not load risk_signals.csv: {e}")
    st.stop()

# Load scenario matrix (corrected path)
try:
    matrix_df = pd.read_csv("data/full_apec_rise_scenario_matrix.csv")
    matrix_df.columns = matrix_df.columns.str.strip()
except Exception as e:
    matrix_df = pd.DataFrame()
    st.warning(f"⚠️ Could not load scenario matrix: {e}")

# Sidebar filters
with st.sidebar:
    st.header("🔍 Filter Risk Signals")

    if "Assumption" in signal_df.columns:
        assumptions = sorted(signal_df["Assumption"].dropna().unique())
        selected_assumption = st.selectbox("Select Assumption", ["All"] + assumptions)
    else:
        selected_assumption = "All"

    economies = sorted(signal_df["Economy"].dropna().unique())
    selected_economy = st.selectbox("Select Economy", ["All"] + economies)

    workstreams = sorted(signal_df["Workstream"].dropna().unique())
    selected_workstream = st.selectbox("Select Workstream", ["All"] + workstreams)

    all_scenarios = ["Baseline", "Optimistic", "Pessimistic"]
    scenarios = sorted(set(all_scenarios).union(signal_df["Scenario"].dropna().unique()))
    selected_scenario = st.selectbox("Select Scenario", ["All"] + scenarios)

    if "Confidence" in signal_df.columns:
        confidences = sorted(signal_df["Confidence"].dropna().unique())
        selected_confidence = st.selectbox("Select Confidence Level", ["All"] + list(confidences))
    else:
        selected_confidence = "All"
        st.warning("⚠️ 'Confidence' column not found.")

# Clear rogue debug output
st.empty()

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
if selected_confidence != "All" and "Confidence" in signal_df.columns:
    filtered = filtered[filtered["Confidence"] == selected_confidence]

# Show filtered data
st.markdown("### 📋 Current Risk Signals")
if filtered.empty:
    st.info("No matching signals found.")
else:
    st.dataframe(filtered, use_container_width=True)

# Scenario summary
st.markdown("### 📊 Scenario Summary")
summary = filtered.groupby(["Economy", "Workstream", "Scenario"]).size().reset_index(name="Signals")
st.dataframe(summary, use_container_width=True)

# Scenario matrix reference
if not matrix_df.empty:
    st.markdown("### 🧩 Scenario Matrix Reference")
    st.dataframe(matrix_df, use_container_width=True)
