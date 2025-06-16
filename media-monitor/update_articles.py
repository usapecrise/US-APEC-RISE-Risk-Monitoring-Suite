import feedparser
import json
from datetime import datetime
from textblob import TextBlob
import spacy
import pandas as pd
from collections import defaultdict

# Load spaCy model for NER
nlp = spacy.load("en_core_web_sm")

# === RSS feeds ===
rss_feeds = {
    "APEC Newsroom": "https://www.apec.org/Press/News-Releases",
    "APEC Events Calendar": "https://www.apec.org/Press/Calendar",
    "U.S. Department of State – RSS": "https://www.state.gov/rss-feeds/",
    "U.S. Department of State – EAP": "https://www.state.gov/rss.xml",
    "U.S. Embassy – Canberra Press Releases": "https://au.usembassy.gov/tag/press-releases/feed/",
    "U.S. Embassy – Tokyo Press Releases": "https://jp.usembassy.gov/category/press-releases/feed/",
    "U.S. Embassy – Beijing Press Releases": "https://china.usembassy.gov/category/press-releases/feed/",
    "U.S. Embassy – Singapore Press Releases": "https://sg.usembassy.gov/category/press-releases/feed/",
    "Nikkei Asia": "https://asia.nikkei.com/rss",
    "The Diplomat": "https://thediplomat.com/feed/",
    "South China Morning Post": "https://www.scmp.com/rss",
    "Reuters Asia Pacific": "http://feeds.reuters.com/reuters/asiaPacificNews",
    "Channel News Asia": "https://www.channelnewsasia.com/rss",
    "Straits Times": "https://www.straitstimes.com/news/singapore/rss.xml",
    "CSIS – Asia Program": "https://www.csis.org/rss.xml",
    "Lowy Institute": "https://www.lowyinstitute.org/rss.xml",
    "Google News – APEC Reforms": "https://news.google.com/rss/search?q=APEC+reform+digital+policy"
}

APEC_ECONOMIES = [
    "Australia", "Brunei", "Canada", "Chile", "China", "Hong Kong",
    "Indonesia", "Japan", "South Korea", "Malaysia", "Mexico", "New Zealand",
    "Papua New Guinea", "Peru", "Philippines", "Russia", "Singapore",
    "Chinese Taipei", "Thailand", "United States", "Vietnam"
]

CAPITAL_TO_ECONOMY = {
    "canberra": "Australia",
    "washington": "United States",
    "tokyo": "Japan",
    "beijing": "China",
    "ottawa": "Canada",
    "jakarta": "Indonesia",
    "singapore": "Singapore",
    "kuala lumpur": "Malaysia",
    "bangkok": "Thailand",
    "manila": "Philippines",
    "wellington": "New Zealand",
    "mexico city": "Mexico",
    "port moresby": "Papua New Guinea"
}

def classify_source(source_name):
    name = source_name.lower()
    if "embassy" in name or "consulate" in name:
        return "embassy"
    if "apec" in name or "state" in name:
        return "official"
    if "google news" in name:
        return "aggregator"
    return "media"

WORKSTREAM_KEYWORDS = {
    "Digital Trade": ["digital trade", "e-commerce", "data flow", "cross-border data", "digital economy"],
    "Cybersecurity": ["cybersecurity", "cyber attack", "data breach", "information security"],
    "Supply Chain Connectivity": ["supply chain", "logistics", "port", "shipping"],
    "Water Quality": ["water quality", "wastewater", "pollution", "sanitation"],
    "Technical Barriers to Trade": ["standards", "tbt", "technical regulation", "certification"],
    "Emerging Technology Standards": ["ai standards", "emerging technology", "artificial intelligence"],
    "Free Trade Area of the Asia-Pacific": ["ftaap", "free trade agreement", "regional integration"],
    "Services": ["services trade", "professional services", "mobility of professionals"],
    "Trade Policy": ["trade facilitation", "tariff", "wto", "rcep"]
}

# === Helper function to tag articles ===
def tag_workstreams(summary):
    matched = set()
    summary_lower = summary.lower()
    for theme, keywords in tag_rules.items():
        for keyword in keywords:
            if keyword in summary_lower:
                matched.add(theme)
    return list(matched) if matched else ["Uncategorized"]

# === Load previously processed articles ===
processed_path = "data/processed_articles.json"
if os.path.exists(processed_path):
    with open(processed_path, "r", encoding="utf-8") as f:
        existing_articles = json.load(f)
    existing_links = {a["link"] for a in existing_articles}
else:
    existing_articles = []
    existing_links = set()

# === Parse feeds ===
new_articles = []
for url in feeds:
    feed = feedparser.parse(url)
    for entry in feed.entries:
        if entry.link in existing_links:
            continue
        article = {
            "title": entry.title,
            "link": entry.link,
            "published": entry.get("published", datetime.utcnow().isoformat()),
            "summary": entry.get("summary", ""),
            "source": feed.feed.get("title", "Unknown"),
            "source_type": "media",
            "sentiment": "Neutral",  # Optional: add real sentiment analysis
            "workstreams": ", ".join(tag_workstreams(entry.get("summary", ""))),
            "aligned_with_us": "Unclear",  # Optional: NLP alignment analysis
            "economy": "Unknown",  # Optional: named entity recognition
            "timestamp": datetime.utcnow().isoformat()
        }
        new_articles.append(article)

# === Merge and save ===
all_articles = new_articles + existing_articles
with open(processed_path, "w", encoding="utf-8") as f:
    json.dump(all_articles, f, ensure_ascii=False, indent=2)

print(f"✅ Added {len(new_articles)} new articles. Total: {len(all_articles)} saved in {processed_path}")

