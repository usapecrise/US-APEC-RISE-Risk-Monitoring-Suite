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
    """Apply scenario thresholds based on % positive responses."""
    if pct >= 75:
        return "optimistic"
    elif pct >= 40:
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

    # Normalize date
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        last_date = df["Date"].max().strftime("%Y-%m-%d")
    else:
        last_date = pd.Timestamp.today().strftime("%Y-%m-%d")

    records = []

    # --- 1. Individual signals ---
    for col, label in [
        ("Knowledge Gain", "knowledge gain"),
        ("Application Intent", "application intent"),
        ("Sharing Intent", "sharing intent"),
    ]:
        if col in df.columns:
            yes_rate = (df[col].astype(str).str.strip().str.lower() == "yes").mean() * 100
            scenario = classify_scenario(yes_rate)
            records.append({
                "assumption": "Stakeholder alignment with U.S. focus areas",
                "monitoring_tool": "feedback",
                "economy": "APEC (aggregate)",
                "date": last_date,
                "signal": f"{yes_rate:.0f}% reported {label}",
                "status": scenario,
                "notes": f"Derived from Feedback Form responses ({col})"
            })

    # --- 2. Composite signal ---
    if records:
        avg_rate = sum([float(r["signal"].split("%")[0]) for r in records]) / len(records)
        composite_scenario = classify_scenario(avg_rate)
        records.append({
            "assumption": "Stakeholder alignment with U.S. focus areas",
            "monitoring_tool": "feedback",
            "economy": "APEC (aggregate)",
            "date": last_date,
            "signal": f"Composite score across feedback signals = {avg_rate:.0f}%",
            "status": composite_scenario,
            "notes": "Average of knowledge, application, and sharing intent"
        })

    if records:
        out_df = pd.DataFrame(records)
        out_df.to_csv(OUTPUT_FILE, index=False)
        print(f"✅ Feedback assumption status saved → {OUTPUT_FILE} ({len(records)} rows)")
    else:
        print("⚠️ No valid feedback signals found")

if __name__ == "__main__":
    main()
