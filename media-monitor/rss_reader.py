import feedparser
import json
from datetime import datetime
import os

# ✅ Ensure the /data folder exists
os.makedirs("data", exist_ok=True)

# ✅ All relevant RSS feed URLs
rss_urls = [
    # 🌏 APEC-relevant news and governance
    "https://news.un.org/feed/subscribe/en/news/region/asia-pacific/feed/rss.xml",
    "https://www.aseanbriefing.com/news/feed/",
    "https://asiafoundation.org/feed/",
    "https://www.devex.com/news/asia-pacific/rss",
    "https://www.straitstimes.com/news/asia/rss.xml",
    "https://www.channelnewsasia.com/rssfeeds/8395986",
    "https://regulationasia.com/feed/",
    "https://www.eastasiaforum.org/feed/",
    "https://www.lowyinstitute.org/the-interpreter/feed",
    "https://www.zdnet.com/news/rss.xml",
    "https://techwireasia.com/feed/",
    "https://feeds.feedburner.com/TheHackersNews",
    "https://www.itnews.com.au/rss",
    "https://www.reutersagency.com/feed/?best-topics=trade&post_type=best",
    "https://www.supplychaindive.com/rss/",
    "https://www.wto.org/english/news_e/news_e.rss",
    "https://www.state.gov/rss-feed/eap/feed.xml",
    "https://www.csis.org/rss/feeds/publications",
    "https://www.brookings.edu/topic/asia/feed/",
    "https://carnegieendowment.org/rss/topic/3001",
    "https://www.chathamhouse.org/rss/all",
    "https://rss.app/feeds/APEC_News_Custom_Feed.xml",

    # 🇺🇸 U.S. Embassy feeds in APEC economies
    "https://au.usembassy.gov/feed/",
    "https://bn.usembassy.gov/feed/",
    "https://ca.usembassy.gov/feed/",
    "https://cl.usembassy.gov/feed/",
    "https://china.usembassy-china.org.cn/feed/",
    "https://id.usembassy.gov/feed/",
    "https://jp.usembassy.gov/feed/",
    "https://kr.usembassy.gov/feed/",
    "https://my.usembassy.gov/feed/",
    "https://mx.usembassy.gov/feed/",
    "https://nz.usembassy.gov/feed/",
    "https://pg.usembassy.gov/feed/",
    "https://pe.usembassy.gov/feed/",
    "https://ph.usembassy.gov/feed/",
    "https://sg.usembassy.gov/feed/",
    "https://www.ait.org.tw/feed/",
    "https://th.usembassy.gov/feed/",
    "https://vn.usembassy.gov/feed/",
    "https://hk.usconsulate.gov/feed/"
]

# ✅ APEC economy names for tagging
known_economies = [
    "Australia", "Brunei Darussalam", "Canada", "Chile", "China", "Hong Kong, China", "Indonesia",
    "Japan", "Republic of Korea", "Malaysia", "Mexico", "New Zealand", "Papua New Guinea", "Peru",
    "The Philippines", "Russia", "Singapore", "Chinese Taipei", "Thailand", "United States", "Viet Nam"
]

def tag_economies(text):
    matches = []
    for econ in known_economies:
        if econ.lower() in text.lower():
            matches.append(econ)
    return list(set(matches)) or ["Unknown"]

# ✅ Parse RSS feeds
articles = []

for url in rss_urls:
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
            "economy": ", ".join(tag_economies(title + summary + link))
        }
        articles.append(article)

# ✅ Save results to JSON file
output_path = "data/processed_articles.json"
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(articles, f, ensure_ascii=False, indent=2)

print(f"✅ Saved {len(articles)} articles to {output_path}")

