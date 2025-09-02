import pandas as pd
import glob

OUTPUT_FILE = "assumptions_status.csv"

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
    for file in INPUT_FILES:
        try:
            df = pd.read_csv(file)
            if not df.empty:
                df["source_file"] = file  # keep traceability
                dfs.append(df)
                print(f"✅ Loaded {file} ({len(df)} rows)")
            else:
                print(f"⚠️ {file} is empty, skipping")
        except FileNotFoundError:
            print(f"⚠️ {file} not found, skipping")

    if not dfs:
        print("⚠️ No assumption files found, nothing to merge")
        return

    merged = pd.concat(dfs, ignore_index=True)

    # Standardize columns (fill missing if some file is off-schema)
    required_cols = ["assumption","monitoring_tool","economy","date","signal","status","notes"]
    for col in required_cols:
        if col not in merged.columns:
            merged[col] = ""

    merged = merged[required_cols + ["source_file"]]

    merged.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Unified assumptions file saved → {OUTPUT_FILE} ({len(merged)} rows)")

if __name__ == "__main__":
    main()
