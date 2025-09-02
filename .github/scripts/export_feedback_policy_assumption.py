import os
import requests
import pandas as pd

# ── CONFIG ──────────────────────────────────────────────
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
BASE_ID = "app0Ljjhrp3lTTpTO"
TABLE_NAME = "Feedback Form Entries"   # replace with your Airtable table name
VIEW_NAME = "Grid view"

OUTPUT_FILE = "policy_reform_assumption.csv"

# ── FUNCTIONS ───────────────────────────────────────────
def fetch_airtable():
    """Fetch all records from Airtable and return as DataFrame."""
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}"
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    records, offset = [], None

    while True:
        params = {"view": VIEW_NAME}
        if offset:
            params["offset"] = offset
        resp = requests.get(url, headers=headers, params=params)
        data = resp.json()

        if "records" not in data:
            print(f"⚠️ Error fetching Airtable: {data}")
            break

        records.extend(data["records"])
        offset = data.get("offset")
        if not offset:
            break

    rows = [r.get("fields", {}) for r in records]
    return pd.DataFrame(rows)


def classify_status(pct_yes: float) -> str:
    """Apply scenario thresholds for application/sharing intent."""
    if pct_yes >= 75:
        return "optimistic"
    elif pct_yes >= 40:
        return "baseline"
    else:
        return "pessimistic"


def main():
    df = fetch_airtable()
    if df.empty:
        print("⚠️ No feedback data found in Airtable")
        return

    # Normalize date
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        last_date = df["Date"].max().strftime("%Y-%m-%d")
    else:
        last_date = pd.Timestamp.today().strftime("%Y-%m-%d")

    # Filter for policy dialogues & meetings
    if "Workshop Title" in df.columns:
        policy_df = df[df["Workshop Title"].str.contains("dialogue|meeting", case=False, na=False)]
    else:
        print("⚠️ Workshop Title column not found, using all data")
        policy_df = df.copy()

    records = []

    # ── Application Intent ───────────────────────────────
    if "Application Intent" in policy_df.columns:
        app_yes_rate = (policy_df["Application Intent"].astype(str)
                        .str.strip().str.lower() == "yes").mean() * 100
        records.append({
            "assumption": "Policy and regulatory openness",
            "monitoring_tool": "feedback",
            "economy": "APEC (aggregate)",   # refine if Economy field exists
            "date": last_date,
            "signal": f"{app_yes_rate:.0f}% intend to apply recommendations",
            "status": classify_status(app_yes_rate),
            "notes": "Feedback from policy dialogue/meeting participants (Application Intent)"
        })

    # ── Sharing Intent ──────────────────────────────────
    if "Sharing Intent" in policy_df.columns:
        share_yes_rate = (policy_df["Sharing Intent"].astype(str)
                          .str.strip().str.lower() == "yes").mean() * 100
        records.append({
            "assumption": "Policy and regulatory openness",
            "monitoring_tool": "feedback",
            "economy": "APEC (aggregate)",
            "date": last_date,
            "signal": f"{share_yes_rate:.0f}% intend to share recommendations",
            "status": classify_status(share_yes_rate),
            "notes": "Feedback from policy dialogue/meeting participants (Sharing Intent)"
        })

    # ── Save Output ─────────────────────────────────────
    if records:
        out_df = pd.DataFrame(records)
        out_df.to_csv(OUTPUT_FILE, index=False)
        print(f"✅ Feedback (policy openness) assumption saved → {OUTPUT_FILE} ({len(records)} rows)")
    else:
        print("⚠️ No valid policy dialogue/meeting feedback found")


# ── MAIN ───────────────────────────────────────────────
if __name__ == "__main__":
    main()
