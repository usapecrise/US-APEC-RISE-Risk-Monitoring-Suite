import streamlit as st
import pandas as pd
import json
from streamlit.runtime.scriptrunner import get_script_run_ctx

# === Page config ===
st.set_page_config(
    page_title="📡 US APEC-RISE Media Monitor",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("📡 US APEC-RISE Media Monitor")
st.markdown(
    "Use this tool to track leadership changes, policy alignment, and reform risks across APEC economies based on media sentiment, tagging, and other M&E inputs."
)

# === Loader with 24h TTL ===
@st.cache_data(ttl=24 * 3600)
def load_articles():
    with open("data/processed_articles.json", "r", encoding="utf-8") as f:
        return json.load(f)

# === Sidebar: Manual Refresh ===
if st.sidebar.button("🔄 Refresh Data"):
    load_articles.clear()
    st.sidebar.success("Cache cleared, loading fresh data…")

# === Load and process ===
articles = load_articles()
df = pd.DataFrame(articles)

if df.empty:
    st.warning("No articles found. Please check the update script or data file.")
    st.stop()

# === Sidebar Filters ===
st.sidebar.header("🔍 Filter Articles")
economies = sorted(df["economy"].dropna().unique())
workstreams = sorted({w.strip() for ws in df["workstreams"].dropna() for w in ws.split(",")})
sentiments = sorted(df["sentiment"].dropna().unique())
source_types = sorted(df["source_type"].dropna().unique())

selected_economy = st.sidebar.selectbox("🌐 Economy", ["All"] + economies)
selected_workstream = st.sidebar.selectbox("🧩 Workstream", ["All"] + workstreams)
selected_sentiment = st.sidebar.selectbox("📈 Sentiment", ["All"] + sentiments)
selected_source = st.sidebar.selectbox("🏛 Source Type", ["All"] + source_types)

# === Apply Filters ===
filtered = df.copy()
if selected_economy != "All":
    filtered = filtered[filtered["economy"] == selected_economy]
if selected_workstream != "All":
    filtered = filtered[filtered["workstreams"].str.contains(selected_workstream)]
if selected_sentiment != "All":
    filtered = filtered[filtered["sentiment"] == selected_sentiment]
if selected_source != "All":
    filtered = filtered[filtered["source_type"] == selected_source]

st.markdown(f"### 📰 Showing {len(filtered)} Article(s)")

# === Display Articles ===
for _, row in filtered.iterrows():
    with st.container():
        st.markdown(f"**[{row['title']}]({row['link']})**")
        st.markdown(
            f"_{row['published']} • {row['economy']} • {row['source_type']} • Sentiment: `{row['sentiment']}`_"
        )
        st.markdown(row['summary'][:400] + "...")
        st.markdown(
            f"`Workstreams:` {row['workstreams']} | `Aligned with U.S.:` {row['aligned_with_us']}"
        )
        st.divider()
