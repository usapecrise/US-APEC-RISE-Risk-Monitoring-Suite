import os
import requests
import pandas as pd

# ── CONFIG ──────────────────────────────────────────────
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
BASE_ID = "app0Ljjhrp3lTTpTO"
TABLE_NAME = "Feedback Form Entries"   # replace with your Airtable table name
VIEW_NAME = "Grid view"

OUTPUT_FILE = "feedback_policy_assumption.csv"

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


def classify_status(score: float) -> str:
    """Apply scenario thresholds for feedback signals (aligned with attendance)."""
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

    if policy_df.empty:
        print("⚠️ No policy dialogue/meeting feedback found")
        return

    # Normalize responses to lowercase strings
    policy_df = policy_df.applymap(lambda x: str(x).strip().lower() if pd.notnull(x) else x)

    # Mapping dictionaries
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

    # ── Application Intent ───────────────────────────────
    if "Application Intent" in policy_df.columns:
        app_scores = policy_df["Application Intent"].map(apply_map).dropna()
        if not app_scores.empty:
            avg_app = app_scores.mean()
            scores.append(avg_app)
            records.append({
                "Assumption": "Policy and regulatory openness",
                "Monitoring Tool": "Feedback",
                "Economy": "APEC (aggregate)",
                "Date": last_date,
                "Signal": f"{avg_app:.0f}% average application intent",
                "Status": classify_status(avg_app),
                "Notes": "Feedback from policy dialogue/meeting participants (Application Intent)"
            })

    # ── Sharing Intent ──────────────────────────────────
    if "Sharing Intent" in policy_df.columns:
        share_scores = policy_df["Sharing Intent"].map(share_map).dropna()
        if not share_scores.empty:
            avg_share = share_scores.mean()
            scores.append(avg_share)
            records.append({
                "Assumption": "Policy and regulatory openness",
                "Monitoring Tool": "Feedback",
                "Economy": "APEC (aggregate)",
                "Date": last_date,
                "Signal": f"{avg_share:.0f}% average sharing intent",
                "Status": classify_status(avg_share),
                "Notes": "Feedback from policy dialogue/meeting participants (Sharing Intent)"
            })

    # ── Composite Row ───────────────────────────────────
    if scores:
        composite_score = sum(scores) / len(scores)
        records.append({
            "Assumption": "Policy and regulatory openness",
            "Monitoring Tool": "Feedback",
            "Economy": "APEC (aggregate)",
            "Date": last_date,
            "Signal": f"Composite feedback score = {composite_score:.0f}%",
            "Status": classify_status(composite_score),
            "Notes": "Average of application and sharing intent (policy dialogues/meetings)"
        })

    # ── Save Output ─────────────────────────────────────
    if records:
        out_df = pd.DataFrame(records)
        out_df.to_csv(OUTPUT_FILE, index=False)
        print(f"✅ Feedback (policy openness) assumption saved → {OUTPUT_FILE} ({len(records)} rows)")
    else:
        print("⚠️ No valid policy dialogue/meeting feedback signals found")


# ── MAIN ───────────────────────────────────────────────
if __name__ == "__main__":
    main()
