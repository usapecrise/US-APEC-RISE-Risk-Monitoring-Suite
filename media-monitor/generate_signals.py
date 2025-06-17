import pandas as pd
import json
from collections import Counter
from datetime import datetime

# === Load processed articles ===
with open("data/processed_articles.json", "r", encoding="utf-8") as f:
    articles = json.load(f)

df = pd.DataFrame(articles)

# === Filter & clean ===
df = df[df["economy"] != "Unknown"]
df = df[df["workstreams"] != "Uncategorized"]
df["timestamp"] = pd.to_datetime(df["timestamp"])
df = df.sort_values("timestamp", ascending=False)

# === Leadership-related keywords (tuned for risk) ===
leadership_keywords = [
    "resign", "cabinet", "minister", "leadership change", "instability", "election",
    "step down", "shakeup", "transition", "political crisis", "appointment", "replaced",
    "acting minister", "new minister", "leadership vacuum"
]

# === Generate risk signals ===
signal_data = []
grouped = df.groupby(["economy", "workstreams"])

for (economy, workstream), group in grouped:
    recent = group.head(10)
    text_blobs = (group["title"] + " " + group["summary"]).str.lower()

    neg_count = sum(1 for x in recent["sentiment"] if x == "Negative")
    pos_count = sum(1 for x in recent["sentiment"] if x == "Positive")
    misaligned = sum(1 for x in recent["aligned_with_us"] if x.lower() == "no")
    aligned = sum(1 for x in recent["aligned_with_us"] if x.lower() == "yes")
    leadership_mentions = sum(any(k in blob for k in leadership_keywords) for blob in text_blobs)

    # === Scenario Logic ===
    if neg_count >= 3 and misaligned >= 2 and leadership_mentions >= 2:
        scenario = "Pessimistic"
        justification = f"{neg_count} negative articles, {misaligned} misaligned, {leadership_mentions} mention leadership changes"
        strength = "High"
    elif pos_count >= 3 and aligned >= 2 and leadership_mentions >= 2:
        scenario = "Optimistic"
        justification = f"{pos_count} positive articles, {aligned} aligned, {leadership_mentions} leadership cooperation signals"
        strength = "High"
    else:
        scenario = "Baseline"
        justification = "No strong leadership or political trend"
        strength = "Medium"

    signal_data.append({
        "Economy": economy,
        "Workstream": workstream,
        "Scenario": scenario,
        "Justification": justification,
        "Signal Strength": strength,
        "Assumption": "Political and Institutional Continuity"
    })

import os
os.makedirs("../data", exist_ok=True)
signal_df.to_csv("../data/risk_signals.csv", index=False)
