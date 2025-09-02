import feedparser
import json
import os
from datetime import datetime
from urllib.parse import urlparse
import pandas as pd
from textblob import TextBlob
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

# === Ensure /data folder exists ===
os.makedirs("data", exist_ok=True)

# === RSS Feeds ===
FEEDS = [
    # 🌐 Media
    {"url": "https://www.smh.com.au/rss/world.xml", "source_type": "Media"},
    {"url": "https://www.straitstimes.com/news/world/rss.xml", "source_type": "Media"},
    {"url": "https://www.straitstimes.com/news/asia/rss.xml", "source_type": "Media"},
    {"url": "https://feeds.bbci.co.uk/news/rss.xml", "source_type": "Media"},
    {"url": "https://feeds.content.dowjones.io/public/rss/RSSWorldNews", "source_type": "Media"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/AsiaPacific.xml", "source_type": "Media"},
    {"url": "https://www.channelnewsasia.com/rssfeeds/8395986", "source_type": "Media"},
    {"url": "https://www.bangkokpost.com/rss/data/topstories.xml", "source_type": "Media"},
    {"url": "https://asia.nikkei.com/rss", "source_type": "Media"},
    {"url": "https://vietnamnews.vn/rss/world.rss", "source_type": "Media"},
    {"url": "https://www.philstar.com/rss/world", "source_type": "Media"},
    {"url": "https://www.rnz.co.nz/rss/pacific.xml", "source_type": "Media"},
    {"url": "https://www.rnz.co.nz/rss/world.xml", "source_type": "Media"},
    {"url": "https://thediplomat.com/feed/", "source_type": "Media"},

    # 🏛 Government
    {"url": "https://www.state.gov/rss-feed/east-asia-and-the-pacific/feed/", "source_type": "Government"},
    {"url": "https://au.usembassy.gov/feed/", "source_type": "Government"},
    {"url": "https://bn.usembassy.gov/feed/", "source_type": "Government"},
    {"url": "https://ca.usembassy.gov/feed/", "source_type": "Government"},
    {"url": "https://cl.usembassy.gov/feed/", "source_type": "Government"},
    {"url": "https://china.usembassy-china.org.cn/feed/", "source_type": "Government"},
    {"url": "https://id.usembassy.gov/feed/", "source_type": "Government"},
    {"url": "https://jp.usembassy.gov/feed/", "source_type": "Government"},
    {"url": "https://kr.usembassy.gov/feed/", "source_type": "Government"},
    {"url": "https://my.usembassy.gov/feed/", "source_type": "Government"},
    {"url": "https://mx.usembassy.gov/feed/", "source_type": "Government"},
    {"url": "https://nz.usembassy.gov/feed/", "source_type": "Government"},
    {"url": "https://pg.usembassy.gov/feed/", "source_type": "Government"},
    {"url": "https://pe.usembassy.gov/feed/", "source_type": "Government"},
    {"url": "https://ph.usembassy.gov/feed/", "source_type": "Government"},
    {"url": "https://sg.usembassy.gov/feed/", "source_type": "Government"},
    {"url": "https://www.ait.org.tw/feed/", "source_type": "Government"},
    {"url": "https://th.usembassy.gov/feed/", "source_type": "Government"},
    {"url": "https://vn.usembassy.gov/feed/", "source_type": "Government"},
    {"url": "https://hk.usconsulate.gov/feed/", "source_type": "Government"},

    # 🧠 Think Tanks
    {"url": "https://www.brookings.edu/topic/asia/feed/", "source_type": "Think Tank"},
    {"url": "https://carnegieendowment.org/rss/topic/3001", "source_type": "Think Tank"},
    {"url": "https://www.lowyinstitute.org/the-interpreter/feed", "source_type": "Think Tank"},
    {"url": "https://www.chathamhouse.org/rss/all", "source_type": "Think Tank"},
    {"url": "https://www.eastasiaforum.org/feed/", "source_type": "Think Tank"},
    {"url": "https://asiafoundation.org/feed/", "source_type": "Think Tank"},

    # 🌍 Multilateral
    {"url": "https://www.apec.org/feeds/rss", "source_type": "Multilateral"},
    {"url": "https://news.un.org/feed/subscribe/en/news/region/asia-pacific/feed/rss.xml", "source_type": "Multilateral"},
    {"url": "https://www.aseanbriefing.com/news/feed/", "source_type": "Multilateral"},
    {"url": "https://www.wto.org/english/news_e/news_e.rss", "source_type": "Multilateral"},
    {"url": "https://unctad.org/rss/news.xml", "source_type": "Multilateral"},
    {"url": "https://www.oecd.org/newsroom/rss.xml", "source_type": "Multilateral"},

    # 💼 Private Sector
    {"url": "https://www.supplychaindive.com/rss/", "source_type": "Private Sector"},
    {"url": "https://www.zdnet.com/news/rss.xml", "source_type": "Private Sector"},
    {"url": "https://www.itnews.com.au/rss", "source_type": "Private Sector"},
    {"url": "https://www.digitaljournal.com/feed", "source_type": "Private Sector"},
    {"url": "https://techwireasia.com/feed/", "source_type": "Private Sector"},
    {"url": "https://www.reutersagency.com/feed/?best-topics=trade&post_type=best", "source_type": "Private Sector"}
]

# === APEC & Workstream Setup ===
APEC_ECONOMIES = [
    "Australia", "Brunei", "Canada", "Chile", "China", "Hong Kong",
    "Indonesia", "Japan", "Korea", "Malaysia", "Mexico", "New Zealand",
    "Papua New Guinea", "Peru", "Philippines", "Russia", "Singapore",
    "Chinese Taipei", "Thailand", "United States", "Vietnam"
]

WORKSTREAM_KEYWORDS = {
    "Digital Trade": ["digital trade", "e-commerce", "data flow", "cross-border data"],
    "Services": ["services trade", "service liberalization"],
    "Supply Chain Connectivity": ["supply chain", "logistics", "port reform"],
    "Emerging Technology Standards": ["standards", "5G", "AI governance"],
    "Cloud Computing": ["cloud", "data center"],
    "Cybersecurity": ["cybersecurity", "data breach", "hacking"],
    "Water Quality": ["water quality", "wastewater", "pollution"],
    "Good Regulatory Practices": ["regulatory reform", "stakeholder consultation"],
    "Technical Barriers to Trade": ["TBT", "technical barriers"],
    "FTAAP": ["free trade area", "FTAAP", "regional trade"]
}

# === Assumption Keyword Sets ===
ASSUMPTION_KEYWORDS = {
    "Stakeholder alignment with U.S. focus areas": {
        "optimistic": ["endorsed", "cooperation", "joint statement", "technical assistance", "mou signed"],
        "pessimistic": ["rejected", "boycott", "refused", "pushback", "opposed"]
    },
    "Political and institutional continuity": {
        "optimistic": ["inauguration", "appointed", "incoming administration"],
        "pessimistic": ["resign", "step down", "shakeup", "instability", "snap election", "vote of no confidence", "coup", "protest"]
    },
    "Supply chain and trade flow resilience": {
        "optimistic": ["logistics agreement", "port expansion", "customs reform", "reduced tariffs"],
        "pessimistic": ["bottleneck", "port closure", "tariff increase", "supply chain risk", "shortages"]
    },
    "Private sector and stakeholder engagement": {
        "optimistic": ["ppp", "public-private partnership", "investment", "business forum", "private sector interest"],
        "pessimistic": ["lack of buy-in", "investment withdrawal", "low turnout", "resistance", "disengagement"]
    }
}

# === Helper Functions ===
def detect_economy(text):
    for econ in APEC_ECONOMIES:
        if econ.lower() in text.lower():
            return econ
    return "Unknown"

def tag_workstreams(text):
    tags = []
    for ws, keywords in WORKSTREAM_KEYWORDS.items():
        if any(k in text.lower() for k in keywords):
            tags.append(ws)
    return ", ".join(tags) if tags else "Uncategorized"

def classify_sentiment(text):
    score = TextBlob(text).sentiment.polarity
    return "Positive" if score > 0.2 else "Negative" if score < -0.2 else "Neutral"

# === Load previous data if it exists ===
processed_path = "data/processed_articles.json"
existing_links = set()
if os.path.exists(processed_path):
    with open(processed_path, "r", encoding="utf-8") as f:
        existing_data = json.load(f)
        existing_links = {item["link"] for item in existing_data}
else:
    existing_data = []

# === Fetch new articles ===
articles = []
print("🛰 Fetching articles...")
for feed in FEEDS:
    url = feed["url"]
    source_type = feed["source_type"]

    parsed = feedparser.parse(url)
    for entry in parsed.entries[:15]:
        title = entry.get("title", "").strip()
        summary = entry.get("summary", "").strip() or entry.get("description", "").strip()
        link = entry.get("link", "").strip()
        pub = entry.get("published", datetime.utcnow().strftime("%Y-%m-%d"))

        if not link or link in existing_links:
            continue

        combined_text = f"{title} {summary}"

        articles.append({
            "title": title,
            "link": link,
            "published": pub,
            "summary": summary,
            "source": urlparse(link).netloc,
            "source_type": source_type,
            "sentiment": classify_sentiment(combined_text),
            "economy": detect_economy(combined_text),
            "workstreams": tag_workstreams(combined_text),
            "timestamp": datetime.utcnow().isoformat()
        })

# === Save merged raw output ===
all_articles = articles + existing_data
all_articles = sorted(all_articles, key=lambda x: x["timestamp"], reverse=True)

with open(processed_path, "w", encoding="utf-8") as f:
    json.dump(all_articles, f, indent=2, ensure_ascii=False)

df = pd.DataFrame(all_articles)
df.to_csv("data/media_log.csv", index=False)

print(f"✅ Added {len(articles)} new articles. Total: {len(all_articles)}")

# === Generate Risk Signals ===
signals = []
for _, row in df.iterrows():
    text = f"{row.get('title', '')} {row.get('summary', '')}".lower()
    economy = row.get("economy", "Unknown")
    workstream = row.get("workstreams", "Uncategorized")

    for assumption, patterns in ASSUMPTION_KEYWORDS.items():
        matched_optimistic = [kw for kw in patterns["optimistic"] if kw in text]
        matched_pessimistic = [kw for kw in patterns["pessimistic"] if kw in text]

        if matched_pessimistic:
            scenario = "Pessimistic"
            justification = ", ".join(matched_pessimistic)
            strength = "High"
        elif matched_optimistic:
            scenario = "Optimistic"
            justification = ", ".join(matched_optimistic)
            strength = "Medium"
        else:
            scenario = "Baseline"
            justification = "No signal keywords detected."
            strength = "Low"

        signals.append({
            "Date": row.get("published", ""),
            "Economy": economy,
            "Workstream": workstream,
            "Assumption": assumption,
            "Scenario": scenario,
            "Justification": justification,
            "Signal Strength": strength
        })

risk_signals = pd.DataFrame(signals)
risk_signals.to_csv("data/risk_signals.csv", index=False)
print(f"✅ Signals saved: {len(risk_signals)}")

# === Roll-up Assumption Status ===
priority = {"Pessimistic": 3, "Optimistic": 2, "Baseline": 1}
risk_signals["priority"] = risk_signals["Scenario"].map(priority)

assumptions_status = (
    risk_signals.sort_values("priority", ascending=False)
    .groupby("Assumption")
    .first()
    .reset_index()[["Assumption", "Scenario", "Date"]]
)
assumptions_status.to_csv("data/assumptions_status.csv", index=False)

print("✅ Assumptions roll-up saved.")

# === QA Dashboard ===
try:
    df["published"] = pd.to_datetime(df["published"], errors="coerce")

    by_date = df.groupby(df["published"].dt.date).size().reset_index(name="articles")
    fig1 = px.line(by_date, x="published", y="articles", title="Articles Over Time")

    sent_econ = df.groupby(["economy", "sentiment"]).size().reset_index(name="count")
    fig2 = px.bar(sent_econ, x="economy", y="count", color="sentiment",
                  title="Sentiment by Economy", barmode="stack")

    src = df.groupby("source_type").size().reset_index(name="count").sort_values("count", ascending=False)
    fig3 = px.bar(src, x="source_type", y="count", title="Articles by Source Type")

    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=("Articles Over Time", "Sentiment by Economy", "Articles by Source Type", "")
    )
    for trace in fig1.data:
        fig.add_trace(trace, row=1, col=1)
    for trace in fig2.data:
        fig.add_trace(trace, row=1, col=2)
    for trace in fig3.data:
        fig.add_trace(trace, row=2, col=1)

    fig.update_layout(title_text="📰 Media Monitor QA Dashboard", showlegend=True)
    fig.write_html("data/media_dashboard.html")

    print("✅ QA dashboard saved → data/media_dashboard.html")
except Exception as e:
    print(f"⚠️ Could not generate dashboard: {e}")
