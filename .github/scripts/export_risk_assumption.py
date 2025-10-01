#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Political & Institutional Continuity Assumption (Media Monitor, DistilBERT + Keyword Boosts)
------------------------------------------------------------------------------------------
- Reads risk_signals.csv
- Classifies signals using:
  * Keyword rules (optimistic / pessimistic)
  * Hugging Face DistilBERT fallback
  * Keyword boosts for context
- Outputs standardized fields:
  * Confidence Index 1 = % optimistic or pessimistic (depending on level)
  * Confidence Index 2 = number of signals
  * Notes clarify interpretation
"""

import pandas as pd
import os
import re
from transformers import pipeline

INPUT_FILE = "risk_signals.csv"
OUTPUT_FILE = "risk_assumption.csv"

# Load Hugging Face DistilBERT sentiment model
hf_sentiment = pipeline(
    "sentiment-analysis",
    model="distilbert-base-uncased-finetuned-sst-2-english"
)

# ── Keyword patterns ─────────────────────────────────────
PESSIMISTIC_PATTERNS = [
    r"\bresignation\b", r"\bresigned\b", r"\bstep(ped)? down\b",
    r"\bdismissed\b", r"\bsacked\b", r"\bremoved from office\b",
    r"\binstability\b", r"\bunstable\b", r"\bturmoil\b",
    r"\bchaos\b", r"\bunrest\b", r"\bcrisis\b", r"\bcoup\b",
    r"\bdisruption\b", r"\bconflict\b", r"\bviolence\b", r"\bprotest(s)?\b",
    r"\bdeadlock\b", r"\bstalemate\b"
]

OPTIMISTIC_PATTERNS = [
    r"\bstability\b", r"\bstable\b", r"\bcontinuity\b", r"\bsmooth transition\b",
    r"\bcooperation\b", r"\bcollaboration\b", r"\bpartnership\b", r"\balignment\b",
    r"\bagreement\b", r"\bconsensus\b", r"\bstrengthen(ed|ing)?\b", r"\breinforce(d|ment)?\b",
    r"\bsupport(ed|ing)?\b", r"\bendorse(d|ment)?\b", r"\binstitutionaliz(e|ed|ing)\b",
    r"\bimplementation\b", r"\badoption\b", r"\bratification\b", r"\bcommitment maintained\b",
    r"\bpolicy continuity\b", r"\binstitutional resilience\b",
    # U.S.-alignment patterns
    r"\bu\.s\. support(ed|ing)?\b", r"\baligned with u\.s\. priorities\b",
    r"\bendorsed by the united states\b", r"\bu\.s\. partnership\b"
]

# Keyword boosts (from your feedback script)
POSITIVE_HINTS = {
    "strengthen","reinforce","support","stability","aligned","consensus",
    "endorse","commitment","continuity","resilience","cooperation","agreement"
}
NEGATIVE_HINTS = {
    "instability","crisis","unrest","turmoil","violence","stalemate","deadlock",
    "resignation","coup","dismissed","sacked","conflict","chaos","removed"
}

# ── Classification helpers ───────────────────────────────
def hf_sentiment_analysis(text: str):
    """DistilBERT sentiment mapped to optimistic/pessimistic/baseline."""
    if not text or text.strip() == "":
        return "baseline", 0.0

    result = hf_sentiment(text[:512])[0]
    label = result["label"].upper()
    score = result["score"]

    if score < 0.6:
        return "baseline", score

    if label == "POSITIVE":
        return "optimistic", score
    elif label == "NEGATIVE":
        return "pessimistic", score
    else:
        return "baseline", score


def classify_scenario(text: str):
    """Keyword + DistilBERT fallback + keyword boosts."""
    txt = str(text).lower()

    pess_hits = sum(bool(re.search(pat, txt)) for pat in PESSIMISTIC_PATTERNS)
    opt_hits  = sum(bool(re.search(pat, txt)) for pat in OPTIMISTIC_PATTERNS)

    # Strong keyword evidence wins immediately
    if pess_hits >= 2 and pess_hits > opt_hits:
        return "pessimistic", pess_hits
    elif opt_hits >= 2 and opt_hits > pess_hits:
        return "optimistic", opt_hits

    # Fallback → DistilBERT
    sentiment, strength = hf_sentiment_analysis(txt)

    # Keyword boosts for weak/neutral cases
    tokens = set(txt.split())
    if sentiment == "baseline":
        if tokens & POSITIVE_HINTS:
            return "optimistic", strength
        elif tokens & NEGATIVE_HINTS:
            return "pessimistic", strength

    return sentiment, strength


def classify_summary(opt_count, pess_count, total):
    """Classify summaries using balance of optimism/pessimism."""
    if total == 0:
        return "baseline", 0
    opt_pct = (opt_count / total) * 100
    pess_pct = (pess_count / total) * 100

    if opt_pct >= 60:
        return "optimistic", round(opt_pct, 1)
    elif pess_pct >= 30:
        return "pessimistic", round(pess_pct, 1)
    else:
        return "baseline", max(round(opt_pct, 1), round(pess_pct, 1))

# ── MAIN ───────────────────────────────────────────────
def main():
    if not os.path.exists(INPUT_FILE):
        print(f"⚠️ No input file found at {INPUT_FILE}")
        return

    df = pd.read_csv(INPUT_FILE)
    if df.empty:
        print("⚠️ risk_signals.csv is empty")
        return

    # Detect text column dynamically
    text_col = None
    for candidate in ["signal", "text", "content", "description"]:
        if candidate in df.columns:
            text_col = candidate
            break
    if not text_col:
        print(f"⚠️ No suitable text column found. Available: {list(df.columns)}")
        return

    records = []
    all_classes = []

    # === 1. Signal-level rows ===
    for _, row in df.iterrows():
        economy = row.get("economy", "Unknown")
        workstream = row.get("workstream", "Unspecified") if "workstream" in df.columns else "Unspecified"
        date = pd.to_datetime(row.get("date", ""), errors="coerce")
        date_str = date.strftime("%Y-%m-%d") if not pd.isna(date) else ""
        signal_text = row.get(text_col, "")

        status, strength = classify_scenario(signal_text)
        all_classes.append(status)

        records.append({
            "Assumption": "Political and institutional continuity",
            "Monitoring Tool": "Media Tracker",
            "Economy": economy,
            "Workstream": workstream,
            "Level": "Signal",
            "Date": date_str,
            "Signal": str(signal_text),
            "Status": status,
            "Confidence Index 1": strength,
            "Confidence Index 2": 1,
            "Notes": "Signal-level classification. CI1 = keyword/NLP/boost match; CI2 = 1 signal."
        })

    # === 2. Economy summaries ===
    if "economy" in df.columns:
        for econ, subset in df.groupby("economy"):
            total = len(subset)
            if total == 0:
                continue
            econ_classes = subset[text_col].apply(lambda x: classify_scenario(x)[0])
            opt_count = (econ_classes == "optimistic").sum()
            pess_count = (econ_classes == "pessimistic").sum()
            status, ci1 = classify_summary(opt_count, pess_count, total)

            records.append({
                "Assumption": "Political and institutional continuity",
                "Monitoring Tool": "Media Monitor",
                "Economy": econ,
                "Workstream": "All",
                "Level": "Economy",
                "Date": pd.to_datetime(subset["date"], errors="coerce").max().strftime("%Y-%m-%d")
                        if "date" in subset.columns else "",
                "Signal": f"{ci1:.0f}% {status} (out of {total} signals)",
                "Status": status,
                "Confidence Index 1": ci1,
                "Confidence Index 2": total,
                "Notes": "Economy summary. CI1 = % optimistic or pessimistic (whichever stronger); CI2 = total signals."
            })

    # === 3. Workstream summaries ===
    if "workstream" in df.columns:
        for ws, subset in df.groupby("workstream"):
            total = len(subset)
            if total == 0:
                continue
            ws_classes = subset[text_col].apply(lambda x: classify_scenario(x)[0])
            opt_count = (ws_classes == "optimistic").sum()
            pess_count = (ws_classes == "pessimistic").sum()
            status, ci1 = classify_summary(opt_count, pess_count, total)

            records.append({
                "Assumption": "Political and institutional continuity",
                "Monitoring Tool": "Media Monitor",
                "Economy": "APEC (aggregate)",
                "Workstream": ws if ws else "Unspecified",
                "Level": "Workstream",
                "Date": pd.to_datetime(subset["date"], errors="coerce").max().strftime("%Y-%m-%d")
                        if "date" in subset.columns else "",
                "Signal": f"{ci1:.0f}% {status} (out of {total} signals)",
                "Status": status,
                "Confidence Index 1": ci1,
                "Confidence Index 2": total,
                "Notes": "Workstream summary. CI1 = % optimistic or pessimistic (whichever stronger); CI2 = total signals."
            })

    # === 4. Aggregate snapshot ===
    total_signals = len(df)
    all_classes = pd.Series([classify_scenario(x)[0] for x in df[text_col]])
    opt_count = (all_classes == "optimistic").sum()
    pess_count = (all_classes == "pessimistic").sum()
    status, ci1 = classify_summary(opt_count, pess_count, total_signals)

    records.append({
        "Assumption": "Political and institutional continuity",
        "Monitoring Tool": "Media Monitor",
        "Economy": "APEC (aggregate)",
        "Workstream": "All",
        "Level": "Aggregate",
        "Date": pd.to_datetime(df["date"], errors="coerce").max().strftime("%Y-%m-%d")
                if "date" in df.columns else "",
        "Signal": f"{ci1:.0f}% {status} (out of {total_signals} signals)",
        "Status": status,
        "Confidence Index 1": ci1,
        "Confidence Index 2": total_signals,
        "Notes": "Aggregate snapshot. CI1 = % optimistic or pessimistic (whichever stronger); CI2 = total signals."
    })

    # === 5. Time-series summary (monthly) ===
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["month"] = df["date"].dt.to_period("M")

        for month, subset in df.groupby("month"):
            total = len(subset)
            if total == 0:
                continue
            month_classes = subset[text_col].apply(lambda x: classify_scenario(x)[0])
            opt_count = (month_classes == "optimistic").sum()
            pess_count = (month_classes == "pessimistic").sum()
            status, ci1 = classify_summary(opt_count, pess_count, total)

            records.append({
                "Assumption": "Political and institutional continuity",
                "Monitoring Tool": "Media Monitor",
                "Economy": "APEC (aggregate)",
                "Workstream": "All",
                "Level": "Time-Series (Monthly)",
                "Date": str(month),
                "Signal": f"{ci1:.0f}% {status} (out of {total} signals in {month})",
                "Status": status,
                "Confidence Index 1": ci1,
                "Confidence Index 2": total,
                "Notes": "Monthly trend summary. CI1 = % optimistic or pessimistic (whichever stronger); CI2 = signals in month."
            })

    # === Export ===
    out_df = pd.DataFrame(records)
    out_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Risk assumption saved → {OUTPUT_FILE} ({len(out_df)} rows))")

    # === Console summary ===
    print("\n--- Classification Summary ---")
    print(all_classes.value_counts())

if __name__ == "__main__":
    main()
