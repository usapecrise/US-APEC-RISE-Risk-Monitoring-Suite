import streamlit as st
import pandas as pd
import os

@st.cache_data
def load_scenario_matrix():
    base_dir = os.path.dirname(__file__)
    filepath = os.path.join(base_dir, "data", "full_apec_rise_scenario_matrix.csv")
    
    if not os.path.exists(filepath):
        st.warning("⚠️ Scenario matrix CSV not found. Please upload it to `scenario-simulator/data/full_apec_rise_scenario_matrix.csv`")
        return pd.DataFrame()
    
    return pd.read_csv(filepath)

def main():
    st.header("🧭 APEC-RISE Scenario Simulator")
    df = load_scenario_matrix()
    
    if df.empty:
        st.info("No scenario data available.")
    else:
        st.dataframe(df)

if __name__ == "__main__":
    main()


st.divider()
st.caption("U.S. APEC-RISE M&E Scenario Simulator")

