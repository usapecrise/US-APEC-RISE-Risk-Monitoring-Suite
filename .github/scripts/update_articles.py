#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Media Monitor → Risk Signals for Political and Institutional Continuity
------------------------------------------------------------------------
- Parses RSS feeds for relevant news
- Tags by economy & workstream
- Classifies signals into optimistic / pessimistic / baseline
- Exports:
  - media_log.csv (all articles)
  - risk_signals.csv (continuity-specific signals)
  - risk_assumption.csv (rolled-up summaries)
"""

import feedparser
import json
import os
from datetime import datetime
from urllib.parse import urlparse
import pandas as pd
from textblob import TextBlob

# === RSS Feeds (subset shown, you can extend) ===
FEEDS = [
    {"url": "https://feeds.bbci.co.uk/news/rss.xml", "source_type": "Media"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/AsiaPacific.xml", "source_type": "Media"},
    {"url": "https://www.straitstimes.com/news/asia/rss.xml", "source_type": "Media"},
    {"url": "https://www.channelnewsasia.com/rssfeeds/8395986", "source_type": "Media"},
    {"url": "https://thediplomat.com/feed/", "source_type": "Media"},
    {"url": "https://www.apec.org/feeds/rss", "source_type": "Multilateral"},
]

# === APEC Economies ===
APEC_ECONOMIES = [
    "Australia", "Brunei", "Canada", "Chile", "China", "Hong Kong",
    "Indonesia", "Japan", "Korea", "Malaysia", "Mexico", "New Zealand",
    "Papua New Guinea", "Peru", "Philippines", "Russia", "Singapore",
    "Chinese Taipei", "Thailand", "United States", "Vietnam"
]

# === Workstream Keywords ===
WORKSTREAM_KEYWORDS = {
    "Digital Trade": ["digital trade", "e-commerce", "data flow"],
    "Services": ["services trade", "liberalization"],
    "Supply Chain Connectivity": ["supply chain", "logistics", "port reform"],
    "Emerging Technology Standards": ["standards", "AI governance"],
    "Cloud Computing": ["cloud", "data center"],
    "Cybersecurity": ["cybersecurity", "data breach", "hacking"],
    "Water Quality": ["water quality", "pollution"],
    "Good Regulatory Practices": ["regulatory reform", "stakeholder consultation"],
    "Technical Barriers to Trade": ["TBT", "technical barriers"],
    "FTAAP": ["free trade area", "FTAAP"]
}

# === Continuity Keywords Only ===
CONTINUITY_KEYWORDS = {
    "optimistic": [
        "inauguration", "appointed", "continuity", "smooth transition",
        "cooperation", "collaboration", "consensus", "stability",
        "partnership", "alignment"
    ],
    "pessimistic": [
        "resign", "resigned", "step down", "dismissed", "ousted",
        "shakeup", "instability", "turmoil", "chaos", "unrest",
        "crisis", "collapse", "coup", "snap election",
        "vote of no confidence", "protest", "boycott"
    ]
}

# === Helpers ===
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

def classify_continuity(text):
    matched_pessimistic = [kw for kw in CONTINUITY_KEYWORDS["pessimistic"] if kw in text]
    matched_optimistic = [kw for kw in CONTINUITY_KEYWORDS["optimistic"] if kw in text]

    if matched_pessimistic:
        return "pessimistic", ", ".join(matched_pessimistic)
    elif matched_optimistic:
        return "optimistic", ", ".join(matched_optimistic)
    else:
        return "baseline", "No signal keywords detected."

# === Fetch New Articles ===
articles = []
for feed in FEEDS:
    parsed = feedparser.parse(feed["url"])
    for entry in parsed.entries[:15]:
        title = entry.get("title", "").strip()
        summary = entry.get("summary", "").strip() or entry.get("description", "").strip()
        link = entry.get("link", "").strip()
        pub = entry.get("published", datetime.utcnow().strftime("%Y-%m-%d"))

        if not link:
            continue

        combined_text = f"{title} {summary}"

        articles.append({
            "title": title,
            "link": link,
            "published": pub,
            "summary": summary,
            "source": urlparse(link).netloc,
            "source_type": feed["source_type"],
            "sentiment": classify_sentiment(combined_text),
            "economy": detect_economy(combined_text),
            "workstreams": tag_workstreams(combined_text),
            "timestamp": datetime.utcnow().isoformat()
        })

df = pd.DataFrame(articles)
df.to_csv("media_log.csv", index=False)

# === Generate Risk Signals (continuity only) ===
signals = []
for _, row in df.iterrows():
    text = f"{row.get('title', '')} {row.get('summary', '')}".lower()
    economy = row.get("economy", "Unknown")
    workstream = row.get("workstreams", "Uncategorized")

    scenario, justification = classify_continuity(text)

    signals.append({
        "Date": row.get("published", ""),
        "Economy": economy,
        "Workstream": workstream,
        "Assumption": "Political and institutional continuity",
        "Scenario": scenario,
        "Justification": justification,
        "Signal Strength": "High" if scenario in ["optimistic", "pessimistic"] else "Low"
    })

risk_signals = pd.DataFrame(signals)
risk_signals.to_csv("risk_signals.csv", index=False)
print(f"✅ Continuity signals saved: {len(risk_signals)}")

# === Roll-up Assumption Status ===
priority = {"pessimistic": 3, "optimistic": 2, "baseline": 1}
risk_signals["priority"] = risk_signals["Scenario"].map(priority)

summary = (
    risk_signals.sort_values("priority", ascending=False)
    .groupby("Economy")
    .first()
    .reset_index()[["Economy", "Scenario", "Date"]]
)

summary.insert(0, "Assumption", "Political and institutional continuity")
summary.to_csv("risk_assumption.csv", index=False)
print("✅ Continuity assumption roll-up saved → risk_assumption.csv")
