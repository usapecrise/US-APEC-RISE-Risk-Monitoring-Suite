# media-monitor/app.py
import streamlit as st
import os, json
import pandas as pd

@st.cache_data
def load_articles():
    fp = os.path.join(os.path.dirname(__file__), "data", "processed_articles.json")
    if not os.path.exists(fp):
        return []
    with open(fp, encoding="utf-8") as f:
        return json.load(f)

def main():
    st.header("📡 Media Monitor")
    articles = load_articles()
    df = pd.DataFrame(articles)
    if df.empty:
        st.info("No articles available.")
    else:
        st.dataframe(df)

if __name__=="__main__":
    main()
