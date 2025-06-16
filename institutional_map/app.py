# institutional-map/app.py
import streamlit as st
import os
import pandas as pd

@st.cache_data
def load_mapping_data():
    base = os.path.dirname(__file__)
    filepath = os.path.join(base, "data", "institutional_network.csv")
    if not os.path.exists(filepath):
        # File isn’t there → return empty DataFrame
        return pd.DataFrame()
    return pd.read_csv(filepath)

def main():
    st.header("🏛 APEC-RISE Institutional Mapping Tool")

    df = load_mapping_data()
    if df.empty:
        st.info("No mapping data available. Upload your `institutional_network.csv` to the `institutional-map/data/` folder.")
    else:
        st.subheader("Institutional Network Data")
        st.dataframe(df)
        # …later you can add your network graph code here…

if __name__ == "__main__":
    main()
