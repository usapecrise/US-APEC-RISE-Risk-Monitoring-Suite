import streamlit as st
import pandas as pd

st.set_page_config(page_title="🧭 APEC-RISE Scenario Simulator", layout="wide")
st.title("🧭 APEC-RISE Scenario Simulator")

# === Load data ===
try:
    df = pd.read_csv("data/risk_signals.csv")
except Exception as e:
    st.error(f"❌ Error loading risk_signals.csv: {e}")
    st.stop()

# === Sidebar Filters ===
st.sidebar.header("🔍 Filter Scenario Signals")
selected_economy = st.sidebar.selectbox("Economy", sorted(df["Economy"].unique()))
selected_workstream = st.sidebar.selectbox("Workstream", sorted(df["Workstream"].unique()))
selected_assumption = st.sidebar.selectbox("Assumption", sorted(df["Assumption"].unique()))

# Optional: Signal Strength filter with "All" option
signal_strengths = ["All"] + sorted(df["Signal Strength"].dropna().unique().tolist())
selected_strength = st.sidebar.selectbox("Signal Strength", signal_strengths)

# Apply filters
filtered = df[
    (df["Economy"] == selected_economy) &
    (df["Workstream"] == selected_workstream) &
    (df["Assumption"] == selected_assumption)
]

if selected_strength != "All":
    filtered = filtered[filtered["Signal Strength"] == selected_strength]

# === Display Filtered Table ===
st.markdown("### 📊 Filtered Scenario Signals")
display_cols = ["Economy", "Workstream", "Assumption", "Scenario", "Justification", "Signal Strength"]
st.dataframe(filtered[display_cols])
