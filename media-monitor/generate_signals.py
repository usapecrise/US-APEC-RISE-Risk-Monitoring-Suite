import pandas as pd
import json
from pathlib import Path

# === Load articles ===
with open("data/processed_articles.json", "r", encoding="utf-8") as f:
    articles = json.load(f)

df = pd.DataFrame(articles)

# === Define keyword sets for each assumption ===
assumption_keywords = {
    "Stakeholder alignment with U.S. focus areas": {
        "optimistic": [
            "endorsed", "cooperation", "joint statement", "technical assistance",
            "bilateral agreement", "mou signed", "aligned with", "expressed support"
        ],
        "pessimistic": [
            "rejected", "boycott", "refused", "pushback", "disagreement",
            "divergence", "mistrust", "withdrew support", "non-alignment", "opposed"
        ],
    },
    "Political and institutional continuity": {
        "optimistic": [
            "inauguration", "assumed office", "appointed", "incoming administration"
        ],
        "pessimistic": [
            "resign", "step down", "shakeup", "instability", "political crisis",
            "snap election", "contested election", "loss of majority", "vote of no confidence",
            "dismissed", "removed from office", "parliamentary reshuffle",
            "coup", "military takeover", "protest", "dissolution", "crackdown", "emergency powers"
        ]
    },
    "Supply chain and trade flow resilience": {
        "optimistic": [
            "logistics agreement", "new trade route", "port expansion", "customs reform",
            "reduced tariffs", "supply chain optimization"
        ],
        "pessimistic": [
            "bottleneck", "port closure", "tariff increase", "logistics delay",
            "disruption", "supply chain risk", "shortages", "blockade"
        ]
    },
    "Private sector and stakeholder engagement": {
        "optimistic": [
            "ppp", "public-private partnership", "investment", "industry roundtable",
            "business forum", "innovation hub", "private sector interest"
        ],
        "pessimistic": [
            "lack of buy-in", "investment withdrawal", "low turnout",
            "stakeholder concern", "resistance", "disengagement", "protest"
        ]
    }
}

# === Detect scenario signals ===
signals = []

for _, row in df.iterrows():
    text = f"{row.get('title', '')} {row.get('summary', '')}".lower()
    economy = row.get("economy", "Unknown")
    workstream = row.get("workstreams", "Uncategorized")

    for assumption, patterns in assumption_keywords.items():
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
            "Economy": economy,
            "Workstream": workstream,
            "Assumption": assumption,
            "Scenario": scenario,
            "Justification": justification,
            "Signal Strength": strength
        })

# === Save to CSV ===
output_path = Path("data/risk_signals.csv")
pd.DataFrame(signals).to_csv(output_path, index=False)
print(f"✅ Signals saved to {output_path}")

