import pandas as pd
import json

# Load articles
with open("data/processed_articles.json", "r", encoding="utf-8") as f:
    articles = json.load(f)

df = pd.DataFrame(articles)

# Leadership keywords categorized by scenario tag
leadership_keywords = {
    "pessimistic": [
        "resign", "step down", "shakeup", "instability", "political crisis",
        "snap election", "contested election", "loss of majority", "vote of no confidence",
        "dismissed", "removed from office", "parliamentary reshuffle",
        "coup", "military takeover", "protest", "dissolution", "crackdown", "emergency powers"
    ],
    "optimistic": [
        "inauguration", "assumed office", "appointed", "incoming administration"
    ],
    "neutral": [
        "cabinet", "minister", "leadership change", "transition", "appointment", 
        "acting minister", "new minister", "succession", "power shift", "handover",
        "change in leadership", "transfer of power", "exit", "runoff", "campaign",
        "president", "prime minister", "governor", "secretary", "chairperson",
        "director-general", "named as interim", "installed"
    ]
}

# Detect scenario signals
signals = []
for _, row in df.iterrows():
    text = f"{row.get('title', '')} {row.get('summary', '')}".lower()
    economy = row.get("economy", "Unknown")
    workstream = row.get("workstreams", "Uncategorized")
    justification = []
    signal_strength = "Low"
    scenario = "Baseline"

    matched_keywords = {
        tag: [kw for kw in keywords if kw in text]
        for tag, keywords in leadership_keywords.items()
    }

    # Determine scenario
    if matched_keywords["pessimistic"]:
        scenario = "Pessimistic"
        justification.extend(matched_keywords["pessimistic"])
        signal_strength = "High"
    elif matched_keywords["optimistic"]:
        scenario = "Optimistic"
        justification.extend(matched_keywords["optimistic"])
        signal_strength = "Medium"
    elif matched_keywords["neutral"]:
        scenario = "Baseline"
        justification.extend(matched_keywords["neutral"])
        signal_strength = "Low"

    if justification:
        signals.append({
            "Economy": economy,
            "Workstream": workstream,
            "Scenario": scenario,
            "Justification": ", ".join(justification),
            "Signal Strength": signal_strength,
            "Assumption": "Political and institutional continuity"
        })

# Save output
signals_df = pd.DataFrame(signals)
signals_df.to_csv("data/risk_signals.csv", index=False)
print("✅ Signals saved to data/risk_signals.csv")

