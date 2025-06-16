# app.py

import streamlit as st
import pandas as pd
import json

st.set_page_config(page_title="📡 APEC-RISE Media Monitor", layout="wide")
st.title("📡 APEC-RISE Media Monitor")

# === Load data ===
@st.cache_data
def load_articles():
    with open("data/processed_articles.json", "r", encoding="utf-8") as f:
        return json.load(f)

articles = load_articles()
if not articles:
    st.warning("No articles available.")
    st.stop()

df = pd.DataFrame(articles)

# === Only APEC economies ===
APEC_ECONOMIES = [
    "Australia", "Brunei", "Canada", "Chile", "China", "Hong Kong",
    "Indonesia", "Japan", "South Korea", "Malaysia", "Mexico", "New Zealand",
    "Papua New Guinea", "Peru", "Philippines", "Russia", "Singapore",
    "Chinese Taipei", "Thailand", "United States", "Vietnam"
]
df = df[df["economy"].isin(APEC_ECONOMIES)]

# === Sidebar Filters ===
with st.sidebar:
    st.header("🔎 Filters")

    economies = ["All"] + APEC_ECONOMIES
    selected_economy = st.selectbox("Economy", economies)

    workstreams = ["All"] + sorted(df["workstreams"].unique().tolist())
    selected_workstream = st.selectbox("Workstream", workstreams)

    sentiments = ["All"] + sorted(df["sentiment"].unique().tolist())
    selected_sentiment = st.selectbox("Sentiment", sentiments)

    source_types = ["All"] + sorted(df["source_type"].unique().tolist())
    selected_source = st.selectbox("Source Type", source_types)

# === Apply Filters ===
filtered = df.copy()
if selected_economy != "All":
    filtered = filtered[filtered["economy"] == selected_economy]
if selected_workstream != "All":
    filtered = filtered[filtered["workstreams"].str.contains(selected_workstream, na=False)]
if selected_sentiment != "All":
    filtered = filtered[filtered["sentiment"] == selected_sentiment]
if selected_source != "All":
    filtered = filtered[filtered["source_type"] == selected_source]

# === Display ===
st.markdown(f"### Showing {len(filtered)} of {len(df)} APEC articles")
for _, row in filtered.iterrows():
    st.markdown(f"#### [{row['title']}]({row['link']})")
    st.markdown(f"📅 {row['published']} • 🌐 {row['economy']} • 🧭 {row['workstreams']} • 📊 {row['sentiment']} • 📰 {row['source_type']}")
    st.markdown(f"> {row['summary']}\n---")
