import pandas as pd
import os

INPUT_FILE = "risk_signals.csv"
OUTPUT_FILE = "risk_assumption.csv"

def classify_scenario(text: str) -> str:
    """Simple keyword-based classification of media/risk signals."""
    text = str(text).lower()

    # Example rules — adjust for your tagging logic
    if any(word in text for word in ["resignation", "instability", "crisis", "disruption", "coup"]):
        return "pessimistic"
    elif any(word in text for word in ["cooperation", "stability", "strengthen", "continuity"]):
        return "optimistic"
    else:
        return "baseline"

def main():
    try:
        df = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        print(f"⚠️ No input file found at {INPUT_FILE}")
        return
    if df.empty:
        print("⚠️ risk_signals.csv is empty")
        return

    records = []
    for _, row in df.iterrows():
        economy = row.get("economy", "Unknown")
        date = row.get("date", "")
        signal_text = row.get("signal", row.to_dict())  # fallback to raw row

        status = classify_scenario(signal_text)

        records.append({
            "Assumption": "Political and institutional continuity",
            "Monitoring_tool": "media_monitor",
            "Economy": economy,
            "Date": date,
            "Signal": str(signal_text),
            "Status": status,
            "Notes": "Derived from media monitoring / risk signals"
        })

    if records:
        out_df = pd.DataFrame(records)
        out_df.to_csv(OUTPUT_FILE, index=False)
        print(f"✅ Risk assumption status saved → {OUTPUT_FILE} ({len(records)} rows)")
    else:
        print("⚠️ No valid risk signals found")

if __name__ == "__main__":
    main()
