import streamlit as st
import os
import json
import pandas as pd

@st.cache_data
def load_articles():
    base_dir = os.path.dirname(__file__)
    filepath = os.path.join(base_dir, "data", "processed_articles.json")
    if not os.path.exists(filepath):
        # don’t crash—just return an empty list
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)

def main():
    st.header("📡 APEC-RISE Media Monitor")
    articles = load_articles()                # <-- define it here
    df = pd.DataFrame(articles)               # now articles exists
    if df.empty:
        st.info("No articles available.")
    else:
        st.dataframe(df)

if __name__ == "__main__":
    main()
