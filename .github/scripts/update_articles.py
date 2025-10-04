#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Media Monitor → Risk Signals for Political and Institutional Continuity
------------------------------------------------------------------------
- Parses RSS feeds for relevant news
- Tags by economy & workstream
- Classifies signals into optimistic / pessimistic / baseline
- Includes U.S. Embassy feeds across all APEC economies
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

# === RSS Feeds (Media + Multilateral) ===
FEEDS = [
    {"url": "https://feeds.bbci.co.uk/news/rss.xml", "source_type": "Media"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/AsiaPacific.xml", "source_type": "Media"},
    {"url": "https://www.straitstimes.com/news/asia/rss.xml", "source_type": "Media"},
    {"url": "https://www.channelnewsasia.com/rssfeeds/8395986", "source_type": "Media"},
    {"url": "https://thediplomat.com/feed/", "source_type": "Media"},
    {"url": "https://www.apec.org/feeds/rss", "source_type": "Multilateral"},
]

# === U.S. Embassy Feeds (APEC Economies) ===
EMBASSY_FEEDS = [
    {"url": "https://au.usembassy.gov/category/alert/feed/", "source_type": "Diplomatic", "economy": "Australia"},
    {"url": "https://bn.usembassy.gov/category/alert/feed/", "source_type": "Diplomatic", "economy": "Brunei"},
    {"url": "https://ca.usembassy.gov/category/alert/feed/", "source_type": "Diplomatic", "economy": "Canada"},
    {"url": "https://cl.usembassy.gov/category/alert/feed/", "source_type": "Diplomatic", "economy": "Chile"},
    {"url": "https://cn.usembassy.gov/category/alert/feed/", "source_type": "Diplomatic", "economy": "China"},
    {"url": "https://hk.usembassy.gov/category/alert/feed/", "source_type": "Diplomatic", "economy": "Hong Kong"},
    {"url": "https://id.usembassy.gov/category/alert/feed/", "source_type": "Diplomatic", "economy": "Indonesia"},
    {"url": "https://jp.usembassy.gov/category/alert/feed/", "source_type": "Diplomatic", "economy": "Japan"},
    {"url": "https://kr.usembassy.gov/category/alert/feed/", "source_type": "Diplomatic", "economy": "Korea"},
    {"url": "https://my.usembassy.gov/category/alert/feed/", "source_type": "Diplomatic", "economy": "Malaysia"},
    {"url": "https://mx.usembassy.gov/category/alert/feed/", "source_type": "Diplomatic", "economy": "Mexico"},
    {"url": "https://nz.usembassy.gov/category/alert/feed/", "source_type": "Diplomatic", "economy": "New Zealand"},
    {"url": "https://pg.usembassy.gov/category/alert/feed/", "source_type": "Diplomatic", "economy": "Papua New Guinea"},
    {"url": "https://pe.usembassy.gov/category/alert/feed/", "source_type": "Diplomatic", "economy": "Peru"},
    {"url": "https://ph.usembassy.gov/category/alert/feed/", "source_type": "Diplomatic", "economy": "Philippines"},
    {"url": "https://ru.usembassy.gov/category/alert/feed/", "source_type": "Diplomatic", "economy": "Russia"},
    {"url": "https://sg.usembassy.gov/category/alert/feed/", "source_type": "Diplomatic", "economy": "Singapore"},
    {"url": "https://tw.usembassy.gov/category/alert/feed/", "source_type": "Diplomatic", "economy": "Chinese Taipei"},
    {"url": "https://th.usembassy.gov/category/alert/feed/", "source_type": "Diplomatic", "economy": "Thailand"},
    {"url": "https://vn.usembassy.gov/category/alert/feed/", "source_type": "Diplomatic", "economy": "Vietnam"},
]

# === APEC Economies ===
APEC_ECONOMIES = [
    "Australia", "Brunei", "Canada", "Chile", "China", "Hong Kong",
    "Indonesia", "Japan", "Korea", "Malaysia", "Mexico", "New Zealand",
    "Papua New Guinea", "Peru", "Philippines", "Russia", "Singapore",
    "Chinese Taipei", "Taiwan:, "Thailand", "United States", "U.S.", "Vietnam"
]

WORKSTREAM_KEYWORDS = {
    "Digital Trade": [
        "digital trade", "e-commerce", "cross-border data flow", "data flow", 
        "digital economy", "digital transformation", "digitalization",
        "online marketplace", "e-payment", "fintech", "e-invoice",
        "digital policy", "digital infrastructure", "digital standards",
        "digital trade agreement", "digital services act", "digital governance"
    ],

    "Services": [
        "services trade", "liberalization", "professional mobility", 
        "mutual recognition", "business process outsourcing", "BPO", 
        "financial services", "education services", "healthcare services", 
        "tourism services", "services competitiveness", 
        "services reform", "services regulation"
    ],

    "Supply Chain Connectivity": [
        "supply chain", "logistics", "port reform", "customs facilitation", 
        "trade facilitation", "border clearance", "transit agreement", 
        "transport corridor", "maritime connectivity", "shipping", "freight", 
        "warehouse", "last-mile delivery", "cold chain", 
        "supply chain resilience", "supply chain security", "just-in-time", 
        "infrastructure investment", "trade bottleneck"
    ],

    "Emerging Technology Standards": [
        "standards", "AI governance", "artificial intelligence", 
        "emerging technology", "standardization", "ISO", "IEC", 
        "IEEE", "interoperability", "5G", "6G", "quantum computing",
        "semiconductors", "digital standards", "ethics framework", 
        "AI policy", "AI regulation", "machine learning", "tech governance"
    ],

    "Cloud Computing": [
        "cloud", "data center", "cloud infrastructure", "SaaS", "IaaS", "PaaS", 
        "hybrid cloud", "public cloud", "private cloud", "cloud migration", 
        "cloud sovereignty", "edge computing", "cloud regulation", 
        "data residency", "digital storage", "virtualization"
    ],

    "Cybersecurity": [
        "cybersecurity", "information security", "cyber threat", 
        "cyber attack", "data breach", "ransomware", "hacking", 
        "phishing", "malware", "network security", "incident response",
        "critical infrastructure protection", "cyber defense", 
        "cyber resilience", "cyber hygiene", "national CERT", 
        "cyber capacity building", "digital safety"
    ],

    "Water Quality": [
        "water quality", "pollution", "wastewater", "sanitation", 
        "clean water", "water treatment", "water management", "river basin", 
        "groundwater", "water monitoring", "water contamination", 
        "industrial discharge", "water governance", "water policy", 
        "environmental compliance", "clean water initiative", "water standards"
    ],

    "Good Regulatory Practices": [
        "regulatory reform", "stakeholder consultation", "impact assessment", 
        "RIA", "transparency", "public comment", "good regulatory practice", 
        "regulatory coherence", "regulatory alignment", "regulatory quality",
        "simplification", "regulatory sandbox", "better regulation", 
        "regulatory policy", "administrative reform", "one-stop shop",
        "regulatory improvement", "regulatory convergence"
    ],

    "Technical Barriers to Trade": [
        "technical barriers", "TBT", "conformity assessment", 
        "accreditation", "certification", "testing laboratory", 
        "metrology", "product standards", "compliance cost", 
        "quality infrastructure", "market access", "standard harmonization", 
        "SPS", "mutual recognition arrangement", "standard equivalence"
    ],

    "FTAAP": [
        "free trade area", "FTAAP", "regional economic integration", 
        "trade agreement", "RCEP", "CPTPP", "APEC regional integration", 
        "tariff reduction", "market access", "preferential trade", 
        "trade liberalization", "economic cooperation", 
        "trade negotiation", "framework agreement", "strategic partnership"
    ]
}

CONTINUITY_KEYWORDS = {
    "optimistic": [
        # Governance stability / leadership continuity
        "inauguration", "re-elected", "appointed", "sworn in", "continuity", 
        "smooth transition", "peaceful transition", "new cabinet formed", 
        "coalition formed", "unity government", "caretaker government", 
        "stable administration", "power handover", "no change in leadership",
        "leadership transition", "peaceful transfer of power", 
        "orderly succession", "renewed mandate", "governing majority",
        
        # Institutional cooperation / functioning
        "cooperation", "collaboration", "coordination", "dialogue resumed", 
        "consensus", "bipartisan", "cross-party", "reconciliation", 
        "confidence restored", "agreement reached", "trilateral", 
        "bilateral talks", "partnership", "joint statement", 
        "policy continuity", "alignment", "shared priorities",
        
        # Diplomatic / regional continuity
        "maintained relations", "strengthened ties", "renewed engagement", 
        "strategic partnership", "ministerial dialogue", 
        "reaffirmed commitment", "endorsed framework", 
        "signed memorandum", "joint declaration", "pledged cooperation"
    ],

    "pessimistic": [
        # Leadership / government instability
        "resign", "resigned", "stepped down", "step down", "fired", 
        "dismissed", "sacked", "ousted", "recalled", "vote of no confidence",
        "impeached", "cabinet reshuffle", "shakeup", "ministerial change",
        "snap election", "deadlock", "hung parliament", "coalition collapse",
        "political crisis", "power struggle", "interim government", 
        "leadership vacuum", "disputed result", "leadership turmoil",
        
        # Governance / unrest
        "instability", "turmoil", "chaos", "unrest", "riots", "mass protest", 
        "boycott", "walkout", "strike", "civil unrest", "crackdown", 
        "state of emergency", "martial law", "constitutional crisis",
        "coup", "coup attempt", "military takeover", "dissolved parliament", 
        "dissolution", "collapse", "crisis", "governance breakdown",
        
        # Diplomatic or institutional friction
        "suspended dialogue", "withdrawn", "expelled diplomat", 
        "cut ties", "diplomatic rift", "sanctions imposed", 
        "policy reversal", "rollback", "breach of agreement", 
        "disputed territory", "boycott talks"
    ]
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

def classify_continuity(text):
    matched_pess = [kw for kw in CONTINUITY_KEYWORDS["pessimistic"] if kw in text]
    matched_opt = [kw for kw in CONTINUITY_KEYWORDS["optimistic"] if kw in text]
    if matched_pess:
        return "pessimistic", ", ".join(matched_pess)
    elif matched_opt:
        return "optimistic", ", ".join(matched_opt)
    else:
        return "baseline", "No signal keywords detected."

# === Fetch Articles ===
def fetch_feeds(feed_list):
    articles = []
    for feed in feed_list:
        parsed = feedparser.parse(feed["url"])
        if not parsed.entries:
            continue  # skip invalid feeds silently
        for entry in parsed.entries[:15]:
            title = entry.get("title", "").strip()
            summary = (entry.get("summary") or entry.get("description") or "").strip()
            link = (entry.get("link") or "").strip()
            pub = entry.get("published") or entry.get("updated") or datetime.utcnow().strftime("%Y-%m-%d")
            combined_text = f"{title} {summary}"

            articles.append({
                "title": title,
                "link": link,
                "published": pub,
                "summary": summary,
                "source": urlparse(link).netloc,
                "source_type": feed["source_type"],
                "economy": feed.get("economy", detect_economy(combined_text)),
                "workstreams": tag_workstreams(combined_text),
                "sentiment": classify_sentiment(combined_text),
                "timestamp": datetime.utcnow().isoformat()
            })
    return articles

media_articles = fetch_feeds(FEEDS)
embassy_articles = fetch_feeds(EMBASSY_FEEDS)
all_articles = media_articles + embassy_articles
df = pd.DataFrame(all_articles)
df.to_csv("media_log.csv", index=False)

# === Generate Risk Signals ===
signals = []
for _, row in df.iterrows():
    text = f"{row.get('title', '')} {row.get('summary', '')}".lower()
    scenario, justification = classify_continuity(text)
    signal_strength = (
        "Very High" if row["source_type"] == "Diplomatic" and scenario != "baseline"
        else "High" if scenario != "baseline"
        else "Low"
    )

    signals.append({
        "Date": row.get("published", ""),
        "Economy": row.get("economy", "Unknown"),
        "Workstream": row.get("workstreams", "Uncategorized"),
        "Assumption": "Political and institutional continuity",
        "Scenario": scenario,
        "Justification": justification,
        "Signal Strength": signal_strength
    })

risk_signals = pd.DataFrame(signals)
risk_signals.to_csv("risk_signals.csv", index=False)
print(f"✅ Continuity signals saved: {len(risk_signals)}")

# === Roll-up Summary ===
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
