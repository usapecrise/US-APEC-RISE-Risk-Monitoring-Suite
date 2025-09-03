import pandas as pd
import os

INPUT_FILE = "risk_signals.csv"
OUTPUT_FILE = "risk_assumption.csv"

def classify_scenario(text: str) -> str:
    """Keyword-based classification of media/risk signals."""
    text = str(text).lower()

    pessimistic_words = ["resignation", "resigned", "instability", "unstable",
                         "crisis", "disruption", "coup", "conflict", "protest"]
    optimistic_words = ["cooperation", "stability", "stable",
                        "strengthen", "continuity", "support"]

    if any(word in text for word in pessimistic_words):
        return "pessimistic"
    elif any(word in text for word in optimistic_words):
        return "optimistic"
    else:
        return "baseline"

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"⚠️ No input file found at {INPUT_FILE}")
        return

    df = pd.read_csv(INPUT_FILE)
    if df.empty:
        print("⚠️ risk_signals.csv is empty")
        return

    records = []
    for _, row in df.iterrows():
        economy = row.get("economy", "Unknown")
        date = pd.to_datetime(row.get("date", ""), errors="coerce")
        date_str = date.strftime("%Y-%m-%d") if not pd.isna(date) else ""

        signal_text = row.get("signal", "")

        status = classify_scenario(signal_text)

        records.append({
            "Assumption": "Political and institutional continuity",
            "Monitoring Tool": "Media Monitor",
            "Economy": economy,
            "Date": date_str,
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

