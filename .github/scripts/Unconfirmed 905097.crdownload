import feedparser
import json
import os
from datetime import datetime
import pandas as pd
from textblob import TextBlob

# === Ensure /data folder exists ===
os.makedirs("data", exist_ok=True)

rss_sources = [
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

# === Economy Keywords ===
economy_keywords = {
    "Australia": ["Australia", "Canberra"],
    "Brunei": ["Brunei"],
    "Canada": ["Canada", "Ottawa"],
    "China": ["China", "Beijing"],
    "Indonesia": ["Indonesia", "Jakarta"],
    "Japan": ["Japan", "Tokyo"],
    "Republic of Korea": ["South Korea", "Korea", "Seoul"],
    "Malaysia": ["Malaysia", "Kuala Lumpur"],
    "Mexico": ["Mexico", "Mexico City"],
    "New Zealand": ["New Zealand", "NZ", "Wellington"],
    "Philippines": ["Philippines", "Manila"],
    "Singapore": ["Singapore"],
    "Chinese Taipei": ["Taiwan", "Taipei"],
    "Thailand": ["Thailand", "Bangkok"],
    "United States": ["United States", "USA", "Washington"],
    "Vietnam": ["Vietnam", "Hanoi"],
}

def tag_economies(text: str):
    found = []
    lower = text.lower()
    for econ, patterns in economy_keywords.items():
        for pat in patterns:
            if pat.lower() in lower:
                found.append(econ)
                break
    return found or ["Uncategorized"]

def get_sentiment(text: str):
    polarity = TextBlob(text).sentiment.polarity
    if polarity > 0.1:
        return "Positive"
    elif polarity < -0.1:
        return "Negative"
    else:
        return "Neutral"

# === Parse Feeds ===
articles = []

for source in rss_sources:
    url = source["url"]
    source_type = source["source_type"]
    feed = feedparser.parse(url)

    for entry in feed.entries:
        title = entry.get("title", "No title")
        summary = entry.get("summary", "") or entry.get("description", "")
        link = entry.get("link", "")
        try:
            published_dt = datetime(*entry.published_parsed[:6])
            published_str = published_dt.strftime("%Y-%m-%d")
        except Exception:
            published_str = datetime.now().strftime("%Y-%m-%d")

        text = " ".join([title, summary])
        economies = tag_economies(text + " " + link)
        sentiment = get_sentiment(text)

        article = {
            "title": title,
            "summary": summary,
            "link": link,
            "published": published_str,
            "source_type": source_type,
            "economy": ", ".join(economies),
            "sentiment": sentiment,
        }
        articles.append(article)

# === Save Raw JSON ===
output_json = "data/processed_articles.json"
with open(output_json, "w", encoding="utf-8") as f:
    json.dump(articles, f, ensure_ascii=False, indent=2)

# === Save Media Log CSV ===
df = pd.DataFrame(articles)
output_csv = "data/media_log.csv"
df.to_csv(output_csv, index=False)

# === Create Risk Signals for Tableau ===
def sentiment_to_scenario(sent):
    if sent == "Positive":
        return "Optimistic"
    elif sent == "Negative":
        return "Pessimistic"
    else:
        return "Baseline"

def sentiment_to_strength(sent):
    if sent == "Negative":
        return "High"
    elif sent == "Positive":
        return "Medium"
    else:
        return "Low"

df["Scenario"] = df["sentiment"].apply(sentiment_to_scenario)
df["Signal Strength"] = df["sentiment"].apply(sentiment_to_strength)
df["Assumption"] = "Political and Institutional Continuity"  # PMP mapping
df["Workstream"] = "General"
df["Justification"] = df["title"]

risk_signals = df.rename(columns={"published": "Date", "economy": "Economy"})[
    ["Date", "Economy", "Workstream", "Assumption", "Scenario", "Signal Strength", "Justification"]
]

risk_csv = "data/risk_signals.csv"
risk_signals.to_csv(risk_csv, index=False)

print(f"✅ Saved {len(df)} articles")
print(f"   → {output_json}")
print(f"   → {output_csv}")
print(f"   → {risk_csv}")
