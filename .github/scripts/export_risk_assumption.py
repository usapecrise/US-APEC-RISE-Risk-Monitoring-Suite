import pandas as pd
import os
import re

INPUT_FILE = "risk_signals.csv"
OUTPUT_FILE = "risk_assumption.csv"

def classify_scenario(text: str):
    """Keyword/phrase-based classification of media/risk signals with confidence score."""
    text = str(text).lower()

    # Pessimistic keywords/phrases
    pessimistic_patterns = [
        r"\bresignation\b", r"\bresigned\b", r"\bstep(ped)? down\b",
        r"\boustered?\b", r"\bdismissed\b", r"\bsacked\b", r"\bremoved from office\b",
        r"\binstability\b", r"\bunstable\b", r"\bturbulence\b", r"\bturmoil\b",
        r"\bchaos\b", r"\bunrest\b", r"\bcrisis\b", r"\bcollapse\b", r"\bcoup\b",
        r"\bdisruption\b", r"\bconflict\b", r"\bviolence\b", r"\bprotest(s)?\b", r"\bboycott(s)?\b",
        r"\bdeadlock\b", r"\bgridlock\b", r"\bblocked reform\b", r"\bstalled reform\b"
    ]

    # Optimistic keywords/phrases
    optimistic_patterns = [
        r"\bstability\b", r"\bstable\b", r"\bcontinuity\b", r"\bsmooth transition\b",
        r"\bcooperation\b", r"\bcollaboration\b", r"\bpartnership\b", r"\balignment\b",
        r"\bagreement\b", r"\bconsensus\b", r"\bstrengthen(ed|ing)?\b", r"\breinforce(d|ment)?\b",
        r"\bsupport(ed|ing)?\b", r"\bendorse(d|ment)?\b", r"\binstitutionaliz(e|ed|ing)\b",
        r"\bimplementation\b", r"\badoption\b", r"\bratification\b", r"\bcommitment maintained\b"
    ]

    pessimistic_hits = sum(bool(re.search(pat, text)) for pat in pessimistic_patterns)
    optimistic_hits = sum(bool(re.search(pat, text)) for pat in optimistic_patterns)

    if pessimistic_hits > optimistic_hits:
        status = "pessimistic"
        score = -pessimistic_hits   # negative = pessimistic strength
    elif optimistic_hits > pessimistic_hits:
        status = "optimistic"
        score = optimistic_hits     # positive = optimistic strength
    elif pessimistic_hits == optimistic_hits == 0:
        status = "baseline"
        score = 0
    else:
        # tie case
        status = "baseline"
        score = optimistic_hits - pessimistic_hits

    return status, score

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

        status, score = classify_scenario(signal_text)

        records.append({
            "Assumption": "Political and institutional continuity",
            "Monitoring Tool": "Media Monitor",
            "Economy": economy,
            "Date": date_str,
            "Signal": str(signal_text),
            "Status": status,
            "Confidence Score": score,
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

