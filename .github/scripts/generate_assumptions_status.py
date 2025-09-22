#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Merge assumption files into a unified dataset
---------------------------------------------
Outputs:
- assumptions_status.csv   → detailed rows (all economies, all dates)
- assumptions_summary.csv  → grouped summary by assumption
- assumptions_status_cards.csv → latest aggregate row per assumption (for Tableau KPI cards)
"""

import pandas as pd
import os

OUTPUT_FILE = "assumptions_status.csv"
SUMMARY_FILE = "assumptions_summary.csv"
CARDS_FILE = "assumptions_status_cards.csv"

# List of assumption input files to merge
INPUT_FILES = [
    "attendance_assumption.csv",
    "attendance_continuity_assumption.csv",
    "feedback_assumption.csv",
    "feedback_policy_assumption.csv",
    "risk_assumption.csv",
    "policy_reform_assumption.csv",
    "cost_share_assumption.csv",
    # Add more assumption files here later if needed
]

def main():
    run_date = pd.Timestamp.today().strftime("%Y-%m-%d")
    print(f"🚀 Starting assumptions merge run on {run_date}")

    dfs = []
    row_counts = {}

    for file in INPUT_FILES:
        try:
            df = pd.read_csv(file)

            # ✅ normalize column names to lowercase and strip spaces
            df.columns = [c.strip().lower() for c in df.columns]

            if not df.empty:
                df["source_file"] = file  # keep traceability
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

    # ✅ normalize assumption labels (prevent duplicates)
    assumption_map = {
        "Stakeholder alignment with U.S. priorities": "Stakeholder alignment with U.S. focus areas"
    }
    merged["assumption"] = merged["assumption"].replace(assumption_map)

    # ✅ Force all risk_assumption.csv rows into Continuity
    is_risk = merged["source_file"] == "risk_assumption.csv"
    merged.loc[is_risk, "assumption"] = "Political and institutional continuity"

    # ✅ If status is missing/null in these rows, default to Baseline
    merged.loc[is_risk & merged["status"].isna(), "status"] = "Baseline"

    # ✅ normalize status to Title Case
    merged["status"] = merged["status"].str.capitalize()

    # ✅ validation check
    missing = [f for f in INPUT_FILES if f not in row_counts or row_counts[f] in (0, None)]
    if missing:
        raise ValueError(f"❌ Missing or empty assumption files in merged output: {missing}")
    else:
        print("✅ All assumption files present in merged output")

    # === Export detailed file ===
    merged.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Unified assumptions file saved → {OUTPUT_FILE} ({len(merged)} rows)")

    # === Row-count summary by file ===
    print("\n📊 Row count by source file:")
    for f, count in row_counts.items():
        if count is None:
            print(f"  {f}: not found")
        else:
            print(f"  {f}: {count} rows")

    # === Row-count summary by assumption + status ===
    if "assumption" in merged.columns and "status" in merged.columns:
        print("\n📊 Row count by assumption and status:")
        summary_counts = (
            merged.groupby(["assumption", "status"])
            .size()
            .reset_index(name="count")
        )
        summary_counts["status"] = summary_counts["status"].str.capitalize()  # ✅ normalize for logs
        for _, row in summary_counts.iterrows():
            print(f"  {row['assumption']} | {row['status']}: {row['count']} rows")

    # === Assumption-level summary ===
    if "assumption" in merged.columns and "status" in merged.columns:
        summary = (
            merged.groupby(["assumption", "status"])
            .size()
            .unstack(fill_value=0)
        )

        # ✅ normalize column names (make status columns Title Case)
        summary = summary.rename(
            columns={
                "optimistic": "Optimistic",
                "baseline": "Baseline",
                "pessimistic": "Pessimistic"
            }
        )

        # Add totals and percentages
        summary["Total"] = summary.sum(axis=1)
        for col in ["Optimistic", "Baseline", "Pessimistic"]:
            if col in summary.columns:
                summary[f"{col}_pct"] = (summary[col] / summary["Total"]) * 100

        # Most recent status by assumption (latest date row, normalized)
        most_recent = (
            merged.sort_values("date", ascending=False)
            .groupby("assumption")
            .first()["status"]
            .str.capitalize()
        )
        summary["Most_Recent_Status"] = most_recent

        # Monitoring tools used per assumption
        tools = (
            merged.groupby("assumption")["monitoring_tool"]
            .unique()
            .apply(lambda x: ", ".join(sorted({str(i) for i in x if pd.notna(i) and str(i).lower() != "nan"})))
        )
        summary["Monitoring_Tools"] = tools

        # Reset for export
        summary = summary.reset_index()

        # ✅ Add Last_Updated column
        summary["Last_Updated"] = run_date

        # Save summary
        summary.to_csv(SUMMARY_FILE, index=False)
        print(f"✅ Assumptions summary file saved → {SUMMARY_FILE} ({len(summary)} rows)")

    # === Aggregate-only latest status for Tableau cards ===
    if "economy" in merged.columns and "date" in merged.columns:
        latest_agg = (
            merged[merged["economy"] == "APEC (aggregate)"]
            .sort_values("date", ascending=False)
            .groupby("assumption")
            .first()
            .reset_index()
        )
        latest_agg["status"] = latest_agg["status"].str.capitalize()  # normalize case
        latest_agg["Last_Updated"] = run_date  # add run date
        latest_agg.to_csv(CARDS_FILE, index=False)
        print(f"✅ Aggregate-level status file saved → {CARDS_FILE} ({len(latest_agg)} rows)")

    print(f"🎉 Run completed successfully on {run_date}")


if __name__ == "__main__":
    main()
