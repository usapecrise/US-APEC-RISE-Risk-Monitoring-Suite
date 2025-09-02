import pandas as pd

INPUT_FILE = "Feedback_Form_Data.csv"
OUTPUT_FILE = "policy_reform_assumption.csv"

def classify_status(pct_yes: float) -> str:
    """Apply scenario thresholds for application/sharing intent."""
    if pct_yes >= 75:
        return "optimistic"
    elif pct_yes >= 40:
        return "baseline"
    else:
        return "pessimistic"

def main():
    try:
        df = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        print(f"⚠️ File not found: {INPUT_FILE}")
        return
    if df.empty:
        print("⚠️ No feedback data found")
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

    # Application Intent
    if "Application Intent" in policy_df.columns:
        app_yes_rate = (policy_df["Application Intent"].astype(str).str.strip().str.lower() == "yes").mean() * 100
        records.append({
            "assumption": "Policy and regulatory openness",
            "monitoring_tool": "feedback",
            "economy": "APEC (aggregate)",   # could refine if economy field exists
            "date": last_date,
            "signal": f"{app_yes_rate:.0f}% intend to apply recommendations",
            "status": classify_status(app_yes_rate),
            "notes": "Feedback from policy dialogue/meeting participants (Application Intent)"
        })

    # Sharing Intent
    if "Sharing Intent" in policy_df.columns:
        share_yes_rate = (policy_df["Sharing Intent"].astype(str).str.strip().str.lower() == "yes").mean() * 100
        records.append({
            "assumption": "Policy and regulatory openness",
            "monitoring_tool": "feedback",
            "economy": "APEC (aggregate)",
            "date": last_date,
            "signal": f"{share_yes_rate:.0f}% intend to share recommendations",
            "status": classify_status(share_yes_rate),
            "notes": "Feedback from policy dialogue/meeting participants (Sharing Intent)"
        })

    if records:
        out_df = pd.DataFrame(records)
        out_df.to_csv(OUTPUT_FILE, index=False)
        print(f"✅ Feedback (policy openness) assumption saved → {OUTPUT_FILE} ({len(records)} rows)")
    else:
        print("⚠️ No valid policy dialogue/meeting feedback found")

if __name__ == "__main__":
    main()
