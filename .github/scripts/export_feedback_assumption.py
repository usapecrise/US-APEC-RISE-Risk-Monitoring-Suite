import os
import requests
import pandas as pd

# Airtable credentials and config
AIRTABLE_TOKEN = os.environ['AIRTABLE_TOKEN']
BASE_ID = 'app0Ljjhrp3lTTpTO'
MAIN_TABLE = 'Feedback Form Entries'
VIEW_NAME = 'Grid view'

# Output CSV path
OUTPUT_FILE = "feedback_assumption.csv"

def classify_scenario(pct: float) -> str:
    """Apply scenario thresholds (aligned with attendance)."""
    if pct >= 60:
        return "optimistic"
    elif pct >= 30:
        return "baseline"
    else:
        return "pessimistic"

def fetch_airtable():
    """Fetch data from Airtable Feedback Form Entries."""
    url = f"https://api.airtable.com/v0/{BASE_ID}/{MAIN_TABLE}"
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    records, offset = [], None

    while True:
        params = {"view": VIEW_NAME}
        if offset:
            params["offset"] = offset
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()

        for r in data.get("records", []):
            f = r.get("fields", {})
            records.append(f)

        offset = data.get("offset")
        if not offset:
            break

    return pd.DataFrame(records)

def main():
    df = fetch_airtable()

    if df.empty:
        print("⚠️ Feedback table is empty")
        return

    # Normalize text responses
    df = df.applymap(lambda x: str(x).strip().lower() if pd.notnull(x) else x)

    # Normalize date
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        last_date = df["Date"].max().strftime("%Y-%m-%d")
    else:
        last_date = pd.Timestamp.today().strftime("%Y-%m-%d")

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
    scores = []

    for col, label, mapping in [
        ("Relevance to Work", "relevance to work", relevance_map),
        ("Knowledge Gain", "knowledge gain", knowledge_map),
        ("Application Intent", "application intent", apply_map),
        ("Sharing Intent", "sharing intent", share_map)
    ]:
        if col in df.columns:
            mapped = df[col].map(mapping).dropna()
            if not mapped.empty:
                avg_score = mapped.mean()
                scores.append(avg_score)
                scenario = classify_scenario(avg_score)
                records.append({
                    "Assumption": "Stakeholder alignment with U.S. focus areas",
                    "Monitoring Tool": "Feedback",
                    "Economy": "APEC (aggregate)",
                    "Date": last_date,
                    "Signal": f"{avg_score:.0f}% average {label}",
                    "Status": scenario,
                    "Notes": f"Derived from Feedback Form responses ({col})"
                })

    # Composite across all signals
    if scores:
        avg_rate = sum(scores) / len(scores)
        composite_scenario = classify_scenario(avg_rate)
        records.append({
            "Assumption": "Stakeholder alignment with U.S. focus areas",
            "Monitoring Tool": "Feedback",
            "Economy": "APEC (aggregate)",
            "Date": last_date,
            "Signal": f"Composite feedback score = {avg_rate:.0f}%",
            "Status": composite_scenario,
            "Notes": "Average of relevance, knowledge, application, and sharing intent"
        })

    if records:
        out_df = pd.DataFrame(records)
        out_df.to_csv(OUTPUT_FILE, index=False)
        print(f"✅ Feedback assumption status saved → {OUTPUT_FILE} ({len(records)} rows)")
    else:
        print("⚠️ No valid feedback signals found")

if __name__ == "__main__":
    main()
