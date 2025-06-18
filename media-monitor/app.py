import streamlit as st
import pandas as pd
import json

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

# === Load and process ===
articles = load_articles()
df = pd.DataFrame(articles)

if df.empty:
    st.warning("No articles found. Please check the update script or data file.")
    st.stop()

# === Fixed lists for filtering ===
all_workstreams = [
    "Digital Trade",
    "Services",
    "Supply Chain Connectivity",
    "Emerging Technology Standards",
    "Cloud Computing",
    "Cybersecurity",
    "Water Quality",
    "Good Regulatory Practices",
    "Technical Barriers to Trade",
    "Free Trade Area of the Asia-Pacific"
]
all_source_types = [
    "Media",
    "Government",
    "Think Tank",
    "Multilateral",
    "Private Sector"
]

# === Sidebar Filters ===
st.sidebar.header("🔍 Filter Articles")

economies = sorted(df["economy"].dropna().unique())
sentiments = sorted(df["sentiment"].dropna().unique())

selected_economy = st.sidebar.selectbox("🌐 Economy", ["All"] + economies)
selected_workstream = st.sidebar.selectbox("🧩 Workstream", ["All"] + all_workstreams)
selected_sentiment = st.sidebar.selectbox("📈 Sentiment", ["All"] + sentiments)
selected_source = st.sidebar.selectbox("🏛 Source Type", ["All"] + all_source_types)

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

