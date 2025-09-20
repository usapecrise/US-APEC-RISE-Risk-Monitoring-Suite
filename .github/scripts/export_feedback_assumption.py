import os
import requests
import pandas as pd

# ── CONFIG ──────────────────────────────────────────────
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
BASE_ID = "app0Ljjhrp3lTTpTO"
TABLE_NAME = "Feedback Form Entries"
VIEW_NAME = "Grid view"

OUTPUT_FILE = "feedback_assumption.csv"

# ── FUNCTIONS ───────────────────────────────────────────
def fetch_airtable():
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}"
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    records, offset = [], None
    while True:
        params = {"view": VIEW_NAME}
        if offset:
            params["offset"] = offset
        resp = requests.get(url, headers=headers, params=params)
        data = resp.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    return pd.DataFrame([r.get("fields", {}) for r in records])


def classify_status(score: float) -> str:
    if score >= 60:
        return "optimistic"
    elif score >= 30:
        return "baseline"
    else:
        return "pessimistic"


def main():
    df = fetch_airtable()
    if df.empty:
        print("⚠️ No feedback data found in Airtable")
        return

    # Normalize date
    last_date = (
        pd.to_datetime(df["Date"], errors="coerce").max().strftime("%Y-%m-%d")
        if "Date" in df.columns else pd.Timestamp.today().strftime("%Y-%m-%d")
    )

    # Normalize responses
    df = df.applymap(lambda x: str(x).strip().lower() if pd.notnull(x) else x)

    # Mapping dictionaries
    relevance_map = {
        "not at all relevant": 0,
        "slightly relevant": 25,
        "somewhat relevant": 50,
        "considerably relevant": 75,
        "greatly relevant": 100
    }
    knowledge_map = {
        "no increase at all": 0,
        "slightly increased": 25,
        "somewhat increased": 50,
        "considerably increased": 75,
        "greatly increased": 100
    }
    apply_map = {
        "yes: i expect to incorporate them routinely in my day-to-day tasks": 100,
        "somewhat: i may apply them occasionally when circumstances warrant": 50,
        "no: i do not foresee any practical use in my current role": 0
    }
    share_map = {
        "yes: i intend to actively share with colleagues or my network": 100,
        "somewhat: i may share in appropriate settings if relevant": 50,
        "not at this time: i do not currently have plans to share": 0
    }

    records = []

    # === Helper function ===
    def process_scores(subset, econ_label, level):
        scores = []

        def add_record(col, label, mapping):
            if col in subset.columns:
                vals = subset[col].map(mapping).dropna()
                if not vals.empty:
                    avg = vals.mean()
                    n = vals.count()
                    scores.append(avg)
                    records.append({
                        "Assumption": "Stakeholder alignment with U.S. focus areas",
                        "Monitoring Tool": "Feedback",
                        "Economy": econ_label,
                        "Workstream": "All",
                        "Level": level,
                        "Date": last_date,
                        "Signal": f"{avg:.0f}% average {label}",
                        "Status": classify_status(avg),
                        "Confidence Index 1 (Percent)": round(avg, 1),
                        "Confidence Index 2 (Responses)": int(n),
                        "Notes": f"Scores mapped 0–100. Thresholds: Optimistic ≥60%, Baseline 30–59%, Pessimistic <30%. Based on {n} responses."
                    })

        # Individual signals
        add_record("Relevance to Work", "relevance", relevance_map)
        add_record("Knowledge Gain", "knowledge gain", knowledge_map)
        add_record("Application Intent", "application intent", apply_map)
        add_record("Sharing Intent", "sharing intent", share_map)

        # Composite
        if scores:
            comp = sum(scores) / len(scores)
            records.append({
                "Assumption": "Stakeholder alignment with U.S. focus areas",
                "Monitoring Tool": "Feedback",
                "Economy": econ_label,
                "Workstream": "All",
                "Level": level,
                "Date": last_date,
                "Signal": f"Composite feedback score = {comp:.0f}%",
                "Status": classify_status(comp),
                "Confidence Index 1 (Percent)": round(comp, 1),
                "Confidence Index 2 (Responses)": int(len(scores)),
                "Notes": "Composite of relevance, knowledge, application, and sharing scores."
            })

    # === 1. APEC aggregate ===
    process_scores(df, "APEC (aggregate)", "Aggregate")

    # === 2. Economy-level breakdown ===
    if "Economy" in df.columns:
        for econ, subset in df.groupby("Economy"):
            process_scores(subset, econ, "Economy")

    # Save output
    out_df = pd.DataFrame(records)
    out_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Feedback assumption saved → {OUTPUT_FILE} ({len(out_df)} rows)")


if __name__ == "__main__":
    main()

