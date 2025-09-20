import pandas as pd
import os

OUTPUT_FILE = "assumptions_status.csv"
SUMMARY_FILE = "assumptions_summary.csv"

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
        summary = merged.groupby(["assumption", "status"]).size().reset_index(name="count")
        for _, row in summary.iterrows():
            print(f"  {row['assumption']} | {row['status']}: {row['count']} rows")

    # === NEW: Assumption-level summary ===
    if "assumption" in merged.columns and "status" in merged.columns:
        summary = (
            merged.groupby(["assumption", "status"])
            .size()
            .unstack(fill_value=0)
        )

        # Add totals and percentages
        summary["Total"] = summary.sum(axis=1)
        for col in ["optimistic", "baseline", "pessimistic"]:
            if col in summary.columns:
                summary[f"{col}_pct"] = (summary[col] / summary["Total"]) * 100

        # Most recent status by assumption (latest date row)
        most_recent = (
            merged.sort_values("date", ascending=False)
            .groupby("assumption")
            .first()["status"]
        )
        summary["Most_Recent_Status"] = most_recent

        # Monitoring tools used per assumption (safe version)
        tools = (
            merged.groupby("assumption")["monitoring_tool"]
            .unique()
            .apply(lambda x: ", ".join(sorted({str(i) for i in x if pd.notna(i) and str(i).lower() != "nan"})))
        )
        summary["Monitoring_Tools"] = tools

        # Reset for export
        summary = summary.reset_index()

        # Save summary
        summary.to_csv(SUMMARY_FILE, index=False)
        print(f"✅ Assumptions summary file saved → {SUMMARY_FILE} ({len(summary)} rows)")


if __name__ == "__main__":
    main()
