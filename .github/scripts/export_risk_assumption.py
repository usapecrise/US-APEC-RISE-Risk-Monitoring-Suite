#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Political & Institutional Continuity Assumption (Media Monitor)
---------------------------------------------------------------
- Reads risk_signals.csv
- Classifies signals by optimistic / pessimistic keyword patterns
- Outputs:
  * Signal-level classification
  * Economy summaries (% optimistic + total signals)
  * Workstream summaries (% optimistic + total signals)
  * Aggregate summary (% optimistic + total signals)
"""

import pandas as pd
import os
import re

INPUT_FILE = "risk_signals.csv"
OUTPUT_FILE = "risk_assumption.csv"

# ── CLASSIFICATION ─────────────────────────────────────
def classify_scenario(text: str):
    """Keyword/phrase-based classification of media/risk signals."""
    text = str(text).lower()

    pessimistic_patterns = [
        r"\bresignation\b", r"\bresigned\b", r"\bstep(ped)? down\b",
        r"\boustered?\b", r"\bdismissed\b", r"\bsacked\b", r"\bremoved from office\b",
        r"\binstability\b", r"\bunstable\b", r"\bturbulence\b", r"\bturmoil\b",
        r"\bchaos\b", r"\bunrest\b", r"\bcrisis\b", r"\bcollapse\b", r"\bcoup\b",
        r"\bdisruption\b", r"\bconflict\b", r"\bviolence\b", r"\bprotest(s)?\b", r"\bboycott(s)?\b",
        r"\bdeadlock\b", r"\bgridlock\b", r"\bblocked reform\b", r"\bstalled reform\b",
        r"\bpower struggle\b", r"\bpolitical vacuum\b", r"\bstalemate\b"
    ]

    optimistic_patterns = [
        r"\bstability\b", r"\bstable\b", r"\bcontinuity\b", r"\bsmooth transition\b",
        r"\bcooperation\b", r"\bcollaboration\b", r"\bpartnership\b", r"\balignment\b",
        r"\bagreement\b", r"\bconsensus\b", r"\bstrengthen(ed|ing)?\b", r"\breinforce(d|ment)?\b",
        r"\bsupport(ed|ing)?\b", r"\bendorse(d|ment)?\b", r"\binstitutionaliz(e|ed|ing)\b",
        r"\bimplementation\b", r"\badoption\b", r"\bratification\b", r"\bcommitment maintained\b",
        r"\bpolicy continuity\b", r"\binstitutional resilience\b"
    ]

    pessimistic_hits = sum(bool(re.search(pat, text)) for pat in pessimistic_patterns)
    optimistic_hits = sum(bool(re.search(pat, text)) for pat in optimistic_patterns)

    if pessimistic_hits > optimistic_hits:
        return "pessimistic", pessimistic_hits
    elif optimistic_hits > pessimistic_hits:
        return "optimistic", optimistic_hits
    elif pessimistic_hits == optimistic_hits == 0:
        return "baseline", 0
    else:
        return "baseline", max(optimistic_hits, pessimistic_hits)


def classify_percentage(pct: float) -> str:
    """Classify percentage thresholds for summaries."""
    if pct >= 60:
        return "optimistic"
    elif pct >= 30:
        return "baseline"
    else:
        return "pessimistic"


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

    # === 1. Signal-level rows ===
    for _, row in df.iterrows():
        economy = row.get("economy", "Unknown")
        workstream = row.get("workstream", "Unspecified") if "workstream" in df.columns else "Unspecified"
        date = pd.to_datetime(row.get("date", ""), errors="coerce")
        date_str = date.strftime("%Y-%m-%d") if not pd.isna(date) else ""
        signal_text = row.get(text_col, "")

        status, hits = classify_scenario(signal_text)

        records.append({
            "Assumption": "Political and institutional continuity",
            "Monitoring Tool": "Media Monitor",
            "Economy": economy,
            "Workstream": workstream,
            "Level": "Signal",
            "Date": date_str,
            "Signal": str(signal_text),
            "Status": status,
            "Confidence Index 1 (Keyword Hits)": hits,  # absolute keyword count
            "Confidence Index 2 (Signals)": 1,
            "Notes": "Signal-level classification. CI1 = keyword matches; CI2 = 1 signal."
        })

    # === 2. Economy summaries ===
    if "economy" in df.columns:
        for econ, subset in df.groupby("economy"):
            total = len(subset)
            if total == 0:
                continue
            optimistic_count = subset[text_col].apply(lambda x: classify_scenario(x)[0] == "optimistic").sum()
            pct_opt = (optimistic_count / total) * 100
            econ_status = classify_percentage(pct_opt)

            records.append({
                "Assumption": "Political and institutional continuity",
                "Monitoring Tool": "Media Monitor",
                "Economy": econ,
                "Workstream": "All",
                "Level": "Economy",
                "Date": pd.to_datetime(subset["date"], errors="coerce").max().strftime("%Y-%m-%d")
                        if "date" in subset.columns else "",
                "Signal": f"{pct_opt:.0f}% optimistic (out of {total} signals)",
                "Status": econ_status,
                "Confidence Index 1 (Percent Optimistic)": round(pct_opt, 1),
                "Confidence Index 2 (Signals Reviewed)": total,
                "Notes": "Economy summary. CI1 = % optimistic signals; CI2 = total signals reviewed."
            })

    # === 3. Workstream summaries ===
    if "workstream" in df.columns:
        for ws, subset in df.groupby("workstream"):
            total = len(subset)
            if total == 0:
                continue
            optimistic_count = subset[text_col].apply(lambda x: classify_scenario(x)[0] == "optimistic").sum()
            pct_opt = (optimistic_count / total) * 100
            ws_status = classify_percentage(pct_opt)

            records.append({
                "Assumption": "Political and institutional continuity",
                "Monitoring Tool": "Media Monitor",
                "Economy": "APEC (aggregate)",
                "Workstream": ws if ws else "Unspecified",
                "Level": "Workstream",
                "Date": pd.to_datetime(subset["date"], errors="coerce").max().strftime("%Y-%m-%d")
                        if "date" in subset.columns else "",
                "Signal": f"{pct_opt:.0f}% optimistic (out of {total} signals)",
                "Status": ws_status,
                "Confidence Index 1 (Percent Optimistic)": round(pct_opt, 1),
                "Confidence Index 2 (Signals Reviewed)": total,
                "Notes": "Workstream summary. CI1 = % optimistic signals; CI2 = total signals reviewed."
            })

    # === 4. Aggregate summary ===
    total_signals = len(df)
    optimistic_count = df[text_col].apply(lambda x: classify_scenario(x)[0] == "optimistic").sum()
    pct_opt = (optimistic_count / total_signals) * 100 if total_signals > 0 else 0
    agg_status = classify_percentage(pct_opt)

    records.append({
        "Assumption": "Political and institutional continuity",
        "Monitoring Tool": "Media Monitor",
        "Economy": "APEC (aggregate)",
        "Workstream": "All",
        "Level": "Aggregate",
        "Date": pd.to_datetime(df["date"], errors="coerce").max().strftime("%Y-%m-%d")
                if "date" in df.columns else "",
        "Signal": f"{pct_opt:.0f}% optimistic (out of {total_signals} signals)",
        "Status": agg_status,
        "Confidence Index 1 (Percent Optimistic)": round(pct_opt, 1),
        "Confidence Index 2 (Signals Reviewed)": total_signals,
        "Notes": "Aggregate summary. CI1 = % optimistic signals; CI2 = total signals reviewed."
    })

    # === Export ===
    out_df = pd.DataFrame(records)
    out_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Risk assumption saved → {OUTPUT_FILE} ({len(out_df)} rows)")


if __name__ == "__main__":
    main()
