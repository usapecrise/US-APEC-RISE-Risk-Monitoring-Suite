import streamlit as st
import os
import pandas as pd

@st.cache_data
def load_mapping_data():
    base = os.path.dirname(__file__)
    fp   = os.path.join(base, "data", "institutional_network.csv")
    if not os.path.exists(fp):
        return pd.DataFrame()   # safe fallback
    return pd.read_csv(fp)

def main():
    st.header("🏛 APEC-RISE Institutional Mapping Tool")
    df = load_mapping_data()
    if df.empty:
        st.info("No mapping data available.\nUpload `institutional_network.csv` to `institutional-map/data/`.")
    else:
        st.dataframe(df)
        # your network graph code goes here

if __name__ == "__main__":
    main()
