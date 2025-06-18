import feedparser
import json
from datetime import datetime
import os

# ✅ Ensure the /data folder exists
os.makedirs("data", exist_ok=True)

rss_sources = [
    # 🌐 Media
    {"url": "https://www.straitstimes.com/news/asia/rss.xml", "source_type": "Media"},
    {"url": "https://www.channelnewsasia.com/rssfeeds/8395986", "source_type": "Media"},
    {"url": "https://www.bangkokpost.com/rss/data/topstories.xml", "source_type": "Media"},
    {"url": "https://asia.nikkei.com/rss", "source_type": "Media"},
    {"url": "https://vietnamnews.vn/rss", "source_type": "Media"},
    {"url": "https://www.philstar.com/rss", "source_type": "Media"},
    {"url": "https://malaysiakini.com/rss", "source_type": "Media"},
    {"url": "https://nzherald.co.nz/rss/", "source_type": "Media"},
    {"url": "https://thediplomat.com/feed/", "source_type": "Media"},
    
    # 🏛 Government
    {"url": "https://www.apec.org/rss/newsroom.xml", "source_type": "Government"},
    {"url": "https://www.state.gov/rss-feed/eap/feed.xml", "source_type": "Government"},
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
    {"url": "https://www.csis.org/rss/feeds/publications", "source_type": "Think Tank"},
    {"url": "https://www.brookings.edu/topic/asia/feed/", "source_type": "Think Tank"},
    {"url": "https://carnegieendowment.org/rss/topic/3001", "source_type": "Think Tank"},
    {"url": "https://www.lowyinstitute.org/the-interpreter/feed", "source_type": "Think Tank"},
    {"url": "https://www.chathamhouse.org/rss/all", "source_type": "Think Tank"},
    {"url": "https://www.eastasiaforum.org/feed/", "source_type": "Think Tank"},
    {"url": "https://asiafoundation.org/feed/", "source_type": "Think Tank"},

    # 🌍 Multilateral
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

# ✅ APEC economy names for tagging
known_economies = [
    "Australia", "Brunei", "Canada", "Chile", "China", "Hong Kong, China", "Indonesia",
    "Japan", "Republic of Korea", "Malaysia", "Mexico", "New Zealand", "Papua New Guinea", "Peru",
    "The Philippines", "Russia", "Singapore", "Chinese Taipei", "Thailand", "United States", "Vietnam"
]

def tag_economies(text):
    matches = []
    for econ in known_economies:
        if econ.lower() in text.lower():
            matches.append(econ)
    return list(set(matches)) or ["Unknown"]

# ✅ Parse RSS feeds
articles = []

for source in rss_sources:
    url = source["url"]
    source_type = source["source_type"]
    feed = feedparser.parse(url)

    for entry in feed.entries:
        title = entry.get("title", "No title")
        link = entry.get("link", "")
        summary = entry.get("summary", "") or entry.get("description", "")
        published = entry.get("published", "") or entry.get("updated", "")
        try:
            published_dt = datetime(*entry.published_parsed[:6])
            published_str = published_dt.strftime("%Y-%m-%d")
        except Exception:
            published_str = datetime.now().strftime("%Y-%m-%d")

        article = {
            "title": title,
            "link": link,
            "published": published_str,
            "summary": summary,
            "people": "",
            "leadership_terms": "",
            "aligned_with_us": "Unknown",
            "matched_alignment_phrase": "",
            "reform_themes": "",
            "economy": ", ".join(tag_economies(title + summary + link)),
            "source_type": source_type
        }
        articles.append(article)


# ✅ Save results to JSON file
output_path = "data/processed_articles.json"
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(articles, f, ensure_ascii=False, indent=2)

print(f"✅ Saved {len(articles)} articles to {output_path}")

