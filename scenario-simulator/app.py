import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="🧭 US APEC-RISE Scenario Simulator",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🧭 APEC-RISE Scenario Simulator")

# === Load data ===
try:
    df_signals = pd.read_csv("data/risk_signals.csv")
    strat_df = pd.read_csv("data/scenario_strategies.csv")
except Exception as e:
    st.error(f"❌ Error loading data files: {e}")
    st.stop()

# Strip column names to avoid whitespace issues
df_signals.columns = df_signals.columns.str.strip()
strat_df.columns = strat_df.columns.str.strip()

# === All economies list & missing warning ===
all_economies = sorted([
    "Australia", "Brunei", "Canada", "Chile", "China", "Hong Kong", "Indonesia", "Japan", "Korea", "Malaysia",
    "Mexico", "New Zealand", "Papua New Guinea", "Peru", "Philippines", "Russia", "Singapore", "Chinese Taipei",
    "Thailand", "United States", "Vietnam"
])
present_economies = sorted(df_signals["Economy"].dropna().unique().tolist())
missing_economies = sorted(set(all_economies) - set(present_economies))
if missing_economies:
    st.warning(f"⚠️ Missing economies in signals: {', '.join(missing_economies)}")

# === Sidebar Debug: Strategy Lookup Preview ===
st.sidebar.subheader("🔧 Strategy Lookup Preview")
st.sidebar.dataframe(strat_df)

# === Sidebar Filters ===
st.sidebar.header("🔍 Filter Scenario Signals")
selected_economy = st.sidebar.selectbox("Economy", all_economies)
selected_workstream = st.sidebar.selectbox(
    "Workstream",
    sorted(df_signals["Workstream"].dropna().unique())
)
selected_assumption = st.sidebar.selectbox(
    "Assumption",
    sorted(df_signals["Assumption"].dropna().unique())
)
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

# === Merge with universal strategy lookup ===
merged = filtered.merge(
    strat_df,
    on=["Assumption", "Scenario"],
    how="left"
)

# === Check for missing strategies ===
if merged["Recommended Adaptation Strategy"].isnull().all():
    st.error("❌ No adaptation strategies found for the selected Assumption/Scenario. Available combos:")
    combos = strat_df[["Assumption", "Scenario"]].drop_duplicates()
    st.table(combos)
    st.stop()

# === Add Icons ===
def add_scenario_icon(scenario):
    icons = {"Optimistic": "🟢 Optimistic", "Baseline": "🟡 Baseline", "Pessimistic": "🔴 Pessimistic"}
    return icons.get(scenario, scenario)

def add_strength_icon(strength):
    icons = {"High": "🔺 High", "Medium": "▶️ Medium", "Low": "▪️ Low"}
    return icons.get(strength, strength)

merged["Scenario"] = merged["Scenario"].apply(add_scenario_icon)
merged["Signal Strength"] = merged["Signal Strength"].apply(add_strength_icon)

# Rename strategy column for display
merged = merged.rename(columns={"Recommended Adaptation Strategy": "Adaptation Strategy"})

# === Display Results ===
st.markdown("### 📊 Filtered Scenario Signals with Adaptation Strategies")
display_cols = [
    "Economy", "Workstream", "Assumption", "Scenario",
    "Justification", "Signal Strength", "Adaptation Strategy"
]
st.dataframe(merged[display_cols], use_container_width=True)

# === Download Button ===
csv = merged[display_cols].to_csv(index=False).encode('utf-8')
st.download_button(
    label="⬇️ Download Filtered Results as CSV",
    data=csv,
    file_name="scenario_signals.csv",
    mime="text/csv"
)

