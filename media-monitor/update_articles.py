import feedparser
import json
import re
import os
from datetime import datetime
from textblob import TextBlob
from urllib.parse import urlparse

# === APEC & Workstream Setup ===
APEC_ECONOMIES = [
    "Australia", "Brunei Darussalam", "Canada", "Chile", "China", "Hong Kong",
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

# === RSS Feeds to Monitor ===
FEEDS = [
    "https://thediplomat.com/feed/",
    "https://www.apec.org/News/news-rss",
    "https://www.channelnewsasia.com/rssfeeds/8395986",
    "https://www.bangkokpost.com/rss/data/topstories.xml"
]

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

print("🛰 Fetching articles from RSS feeds...")
for url in FEEDS:
    parsed = feedparser.parse(url)
    feed_count = 0
    for entry in parsed.entries[:15]:  # limit per feed
        title = entry.get("title", "").strip()
        summary = entry.get("summary", "").strip() or entry.get("description", "").strip()
        link = entry.get("link", "").strip()
        pub = entry.get("published", "")
        source = urlparse(link).netloc

        if not link or link in existing_links:
            continue  # skip blank or duplicate

        combined_text = f"{title} {summary}"

        articles.append({
            "title": title,
            "link": link,
            "published": pub,
            "summary": summary,
            "source": source,
            "source_type": get_source_type(link),
            "sentiment": classify_sentiment(combined_text),
            "economy": detect_economy(combined_text),
            "workstreams": tag_workstreams(combined_text),
            "aligned_with_us": "Unclear",
            "timestamp": datetime.utcnow().isoformat()
        })
        feed_count += 1

    print(f"📡 {url} → {feed_count} new articles")

# === Save merged output ===
all_articles = articles + existing_data
all_articles = sorted(all_articles, key=lambda x: x["timestamp"], reverse=True)

with open(processed_path, "w", encoding="utf-8") as f:
    json.dump(all_articles, f, indent=2, ensure_ascii=False)

print(f"\n✅ Added {len(articles)} new articles. Total in file: {len(all_articles)}")
