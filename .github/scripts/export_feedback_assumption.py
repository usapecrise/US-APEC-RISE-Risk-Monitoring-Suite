import os
import pandas as pd

# Input file (already exported by export_Feedback_Form_Data.py)
RAW_FEEDBACK_FILE = "Feedback_Form_Data.csv"
OUTPUT_FILE = "feedback_assumption.csv"

def main():
    try:
        df = pd.read_csv(RAW_FEEDBACK_FILE)
    except FileNotFoundError:
        print(f"⚠️ No feedback file found at {RAW_FEEDBACK_FILE}")
        return
    if df.empty:
        print("⚠️ Feedback file is empty")
        return

    # Normalize date
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        last_date = df["Date"].max().strftime("%Y-%m-%d")
    else:
        last_date = pd.Timestamp.today().strftime("%Y-%m-%d")

    # Example rule: classify based on Knowledge Gain % Yes
    # (adjust depending on your actual Feedback Form Data schema)
    if "Knowledge Gain" in df.columns:
        yes_rate = (df["Knowledge Gain"].str.strip().str.lower() == "yes").mean() * 100
    else:
        print("⚠️ 'Knowledge Gain' column not found in Feedback Form Data")
        return

    # Scenario thresholds (example)
    if yes_rate >= 75:
        scenario = "optimistic"
    elif yes_rate >= 40:
        scenario = "baseline"
    else:
        scenario = "pessimistic"

    # Build assumption record
    feedback_status = pd.DataFrame([{
        "assumption": "Stakeholder alignment with U.S. focus areas",
        "monitoring_tool": "feedback",
        "economy": "APEC (aggregate)",   # could be broken down if survey captures economy
        "date": last_date,
        "signal": f"{yes_rate:.0f}% of respondents reported knowledge gain",
        "status": scenario,
        "notes": "Derived from Feedback Form survey responses"
    }])

    feedback_status.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Feedback assumption status saved → {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
