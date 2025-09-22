#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Merge assumption files into a unified dataset
---------------------------------------------
Outputs:
- assumptions_status.csv        → detailed rows (all economies, all dates)
- assumptions_summary.csv       → grouped summary by assumption (with friendly tool names + Signal_Count)
- assumptions_status_cards.csv  → latest aggregate row per assumption (for Tableau KPI cards: Most_Recent_Status + counts + %s)
- assumptions_breakdown.csv     → assumption × source tool breakdown (counts + % contribution)
"""

import pandas as pd
import os

OUTPUT_FILE    = "assumptions_status.csv"
SUMMARY_FILE   = "assumptions_summary.csv"
CARDS_FILE     = "assumptions_status_cards.csv"
BREAKDOWN_FILE = "assumptions_breakdown.csv"

# List of assumption input files to merge
INPUT_FILES = [
    "attendance_assumption.csv",
    "attendance_continuity_assumption.csv",
    "feedback_assumption.csv",
    "feedback_policy_assumption.csv",
    "risk_assumption.csv",
    "policy_reform_assumption.csv",
    "cost_share_assumption.csv",
]

# ✅ Friendly monitoring tool names
SOURCE_LABELS = {
    "attendance_assumption.csv": "Attendance Tracker (Alignment)",
    "attendance_continuity_assumption.csv": "Attendance Tracker (Continuity)",
    "feedback_assumption.csv": "Feedback Tracker (Alignment)",
    "feedback_policy_assumption.csv": "Feedback Tracker (Openness)",
    "risk_assumption.csv": "Media Monitor (Continuity)",
    "policy_reform_assumption.csv": "Policy Reform Tracker (Openness)",
    "cost_share_assumption.csv": "Cost-Share Tracker (Ownership)"
}

def format_pct(value):
    """Convert decimal to percentage string with no decimals"""
    try:
        return f"{round(value)}%"
    except:
        return ""

def main():
    run_date = pd.Timestamp.today().strftime("%Y-%m-%d")
    print(f"🚀 Starting assumptions merge run on {run_date}")

    dfs = []
    row_counts = {}

    for file in INPUT_FILES:
        try:
            df = pd.read_csv(file)
            df.columns = [c.strip().lower() for c in df.columns]  # normalize col names

            if not df.empty:
                df["source_file"] = SOURCE_LABELS.get(file, file)  # map to friendly tool name
                dfs.append(df)
                row_counts[file] = len(df)
                print(f"✅ Loaded {file} ({len(df)} rows)")
            else:
                row_counts[file] = 0
                print(f"⚠️ {file} is empty, skipping")
        except FileNotFoundError:
            row_counts[file] = None
            print(f"⚠️ {file} not found, skipping")

    if not dfs:
        print("⚠️ No assumption files found, nothing to merge")
        return

    merged = pd.concat(dfs, ignore_index=True)

    # ✅ enforce required columns
    required_cols = ["assumption","monitoring_tool","economy","date","signal","status","notes"]
    for col in required_cols:
        if col not in merged.columns:
            merged[col] = ""

    merged = merged[required_cols + ["source_file"]]

    # ✅ normalize assumption labels
    assumption_map = {
        "Stakeholder alignment with U.S. priorities": "Stakeholder alignment with U.S. focus areas"
    }
    merged["assumption"] = merged["assumption"].replace(assumption_map)

    # ✅ remap risk assumptions → Continuity
    is_risk = merged["source_file"] == "Media Monitor (Continuity)"
    merged.loc[is_risk, "assumption"] = "Political and institutional continuity"
    merged.loc[is_risk & merged["status"].isna(), "status"] = "Baseline"

    # ✅ normalize status case
    merged["status"] = merged["status"].astype(str).str.lower().str.strip()
    merged["status"] = merged["status"].replace({
        "optimistic":"Optimistic",
        "baseline":"Baseline",
        "pessimistic":"Pessimistic"
    })

    # === Export detailed file ===
    merged.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Unified assumptions file saved → {OUTPUT_FILE} ({len(merged)} rows)")

    # === Breakdown by assumption + source tool ===
    breakdown = (
        merged.groupby(["assumption", "source_file"])
        .size()
        .reset_index(name="count")
        .sort_values(["assumption", "source_file"])
    )
    totals = breakdown.groupby("assumption")["count"].transform("sum")
    breakdown["percent"] = (breakdown["count"] / totals * 100).round(1)
    breakdown.to_csv(BREAKDOWN_FILE, index=False)
    print(f"✅ Assumptions breakdown file saved → {BREAKDOWN_FILE} ({len(breakdown)} rows)")

    # === Assumption-level summary ===
    summary = (
        merged.groupby(["assumption", "status"])
        .size()
        .unstack(fill_value=0)
    )
    summary["Total"] = summary.sum(axis=1)
    for col in ["Optimistic", "Baseline", "Pessimistic"]:
        if col in summary.columns:
            summary[f"{col}_pct"] = (summary[col] / summary["Total"]) * 100

    most_recent = (
        merged.sort_values("date", ascending=False)
        .groupby("assumption")
        .first()["status"]
    )
    summary["Most_Recent_Status"] = most_recent

    # ✅ Monitoring tools (friendly names)
    tools = (
        merged.groupby("assumption")["source_file"]
        .unique()
        .apply(lambda x: ", ".join(sorted({str(i) for i in x if pd.notna(i)})))
    )
    summary["Monitoring_Tools"] = tools

    summary = summary.reset_index()
    summary["Signal_Count"] = summary["Total"]
    summary["Last_Updated"] = run_date
    summary.to_csv(SUMMARY_FILE, index=False)
    print(f"✅ Assumptions summary file saved → {SUMMARY_FILE} ({len(summary)} rows)")

    # === Aggregate for Tableau cards ===
    # --- 1. Get most recent status from APEC aggregate rows ---
    most_recent = (
        merged[merged["economy"] == "APEC (aggregate)"]
        .sort_values("date", ascending=False)
        .groupby("assumption")
        .first()[["status","date","notes"]]
        .rename(columns={"status":"Most_Recent_Status","date":"Latest_Date","notes":"Latest_Notes"})
    )

    # --- 2. Totals from ALL inputs ---
    totals = (
        merged.groupby(["assumption","status"])
        .size()
        .unstack(fill_value=0)
        .rename(columns={
            "Optimistic":"Optimistic",
            "Baseline":"Baseline",
            "Pessimistic":"Pessimistic"
        })
    )
    totals["Signal_Count"] = totals.sum(axis=1)
    for col in ["Optimistic","Baseline","Pessimistic"]:
        if col in totals.columns:
            totals[f"{col}_pct"] = (totals[col] / totals["Signal_Count"]) * 100

    # --- 3. Breakdown string per assumption ---
    breakdown_dict = (
        merged.groupby(["assumption", "source_file"])
        .size()
        .reset_index(name="count")
        .groupby("assumption")
        .apply(lambda g: "; ".join([f"{row['source_file']}={row['count']}" for _, row in g.iterrows()]))
    )
    breakdown_dict = breakdown_dict.reset_index().rename(columns={0:"Inputs_By_Tool"})

    # --- 4. Merge all parts into cards ---
    cards = totals.merge(most_recent, on="assumption", how="left")
    cards = cards.merge(breakdown_dict, on="assumption", how="left")
    cards = cards.reset_index()

    # ✅ Format percentages
    for col in ["Optimistic_pct","Baseline_pct","Pessimistic_pct"]:
        if col in cards.columns:
            cards[col] = cards[col].apply(format_pct)

    cards["Last_Updated"] = run_date
    cards.to_csv(CARDS_FILE, index=False)
    print(f"✅ Aggregate-level status file saved → {CARDS_FILE} ({len(cards)} rows)")

    print(f"🎉 Run completed successfully on {run_date}")

if __name__ == "__main__":
    main()
