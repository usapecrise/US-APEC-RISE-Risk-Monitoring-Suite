import feedparser
import json
import os
from datetime import datetime
from urllib.parse import urlparse
import pandas as pd
from textblob import TextBlob

# === Ensure /data folder exists ===
os.makedirs("data", exist_ok=True)

# === RSS Feeds to Monitor ===
FEEDS = [
    "https://thediplomat.com/feed/",
    "https://www.apec.org/News/news-rss",
    "https://www.channelnewsasia.com/rssfeeds/8395986",
    "https://www.bangkokpost.com/rss/data/topstories.xml"
]

# === Economy + Workstream Keywords ===
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

SOURCE_TYPES = {
    "gov": "Government",
    "go.id": "Government",
    "org": "Multilateral",
    "reuters.com": "Media",
    "apnews.com": "Media",
    "worldbank.org": "Multilateral",
    "bloomberg.com": "Media"
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

def get_source_type(url):
    domain = urlparse(url).netloc
    for key, label in SOURCE_TYPES.items():
        if key in domain:
            return label
    return "Other"

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
for url in FEEDS:
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
            "source_type": get_source_type(link),
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
