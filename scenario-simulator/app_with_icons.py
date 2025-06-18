
import streamlit as st
import pandas as pd

st.set_page_config(page_title="🧭 APEC-RISE Scenario Simulator", layout="wide")
st.title("🧭 APEC-RISE Scenario Simulator")

# === Load data ===
try:
    df_signals = pd.read_csv("data/risk_signals.csv")
    df_matrix = pd.read_csv("data/full_apec_rise_scenario_matrix.csv")
except Exception as e:
    st.error(f"❌ Error loading data files: {e}")
    st.stop()

# Strip column names just in case
df_signals.columns = df_signals.columns.str.strip()
df_matrix.columns = df_matrix.columns.str.strip()

# === Sidebar Filters ===
st.sidebar.header("🔍 Filter Scenario Signals")
selected_economy = st.sidebar.selectbox("Economy", sorted(df_signals["Economy"].unique()))
selected_workstream = st.sidebar.selectbox("Workstream", sorted(df_signals["Workstream"].unique()))
selected_assumption = st.sidebar.selectbox("Assumption", sorted(df_signals["Assumption"].unique()))

signal_strengths = ["All"] + sorted(df_signals["Signal Strength"].dropna().unique().tolist())
selected_strength = st.sidebar.selectbox("Signal Strength", signal_strengths)

# === Apply Filters ===
filtered = df_signals[
    (df_signals["Economy"] == selected_economy) &
    (df_signals["Workstream"] == selected_workstream) &
    (df_signals["Assumption"] == selected_assumption)
]
if selected_strength != "All":
    filtered = filtered[filtered["Signal Strength"] == selected_strength]

# === Merge with Scenario Matrix ===
merged = pd.merge(
    filtered,
    df_matrix,
    how="left",
    on=["Economy", "Assumption", "Scenario"]
)

# === Add Icons and Tooltips ===
def add_scenario_icon(scenario):
    icons = {
        "Optimistic": "🟢 Optimistic",
        "Baseline": "🟡 Baseline",
        "Pessimistic": "🔴 Pessimistic"
    }
    return icons.get(scenario, scenario)

def add_strength_icon(strength):
    icons = {
        "High": "🔺 High",
        "Medium": "▶️ Medium",
        "Low": "▪️ Low"
    }
    return icons.get(strength, strength)

merged["Scenario"] = merged["Scenario"].apply(add_scenario_icon)
merged["Signal Strength"] = merged["Signal Strength"].apply(add_strength_icon)

# === Display Table ===
st.markdown("### 📊 Filtered Scenario Signals with Adaptation Strategies")
display_cols = ["Economy", "Workstream", "Assumption", "Scenario", "Justification", "Signal Strength", "Adaptation Strategy", "Timeline"]
st.dataframe(merged[display_cols], use_container_width=True)

# === Export Button ===
csv = merged[display_cols].to_csv(index=False).encode('utf-8')
st.download_button("⬇️ Download Filtered Results as CSV", csv, "scenario_signals.csv", "text/csv")
