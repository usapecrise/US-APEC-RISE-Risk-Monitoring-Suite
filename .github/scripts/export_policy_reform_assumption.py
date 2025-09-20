#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Policy Reform (OC1) → Policy and Regulatory Openness
----------------------------------------------------
Generates assumption data for:
- Reform-level signals
- Economy-level summaries
- Workstream-level summaries
- APEC aggregate

Logic:
- Tracks adoption of policy reforms (fully adopted, in progress, not started, unsure)
- CI1 = numeric score (100 = fully adopted, 75 = in progress, 50 = unsure, 0 = not yet initiated)
- CI2 = count of reforms
"""

import os
import requests
import pandas as pd

# ── CONFIG ──────────────────────────────────────────────
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
BASE_ID = "app0Ljjhrp3lTTpTO"
TABLE_NAME = "OC1 Policy Reforms"
VIEW_NAME = "Grid view"

OUTPUT_FILE = "policy_reform_assumption.csv"

# ── FUNCTIONS ───────────────────────────────────────────
def fetch_table(table_name):
    url = f"https://api.airtable.com/v0/{BASE_ID}/{table_name}"
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    records, offset = [], None
    while True:
        params = {"view": VIEW_NAME}
        if offset:
            params["offset"] = offset
        resp = requests.get(url, headers=headers, params=params)
        data = resp.json()
        for r in data.get("records", []):
            f = r.get("fields", {})
            records.append({
                "Date": f.get("Date", ""),
                "Economy": f.get("Economy", "Unspecified"),
                "Workstream": f.get("Workstream", "Unspecified"),
                "Reform Status": f.get("Reform Adopted/Advanced", "Unspecified"),
                "Reform Type": f.get("Reform Type", ""),
                "Other Reform": f.get("Other Reform", ""),
                "Notes": f.get("Notes", "")
            })
        offset = data.get("offset")
        if not offset:
            break
    return pd.DataFrame(records)


def classify_status(reform_status: str):
    """Classify reform status into category + CI1 score."""
    status = str(reform_status).strip().lower()
    if status == "yes- fully adopted or implemented":
        return "optimistic", 100
    elif status == "yes- in progress or under development":
        return "optimistic", 75
    elif status in ["unsure", "don’t know", "unsure / don’t know"]:
        return "baseline", 50
    elif status == "no- not yet initiated":
        return "pessimistic", 0
    else:
        return "baseline", 50  # default fallback


def classify_percentage(pct: float) -> str:
    """Classify average CI1 percentage into scenario status."""
    if pct >= 60:
        return "optimistic"
    elif pct >= 30:
        return "baseline"
    else:
        return "pessimistic"


# ── MAIN ───────────────────────────────────────────────
def main():
    df = fetch_table(TABLE_NAME)
    if df.empty:
        print("⚠️ No policy reform data found, skipping assumption export")
        return

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    latest_date = (
        df["Date"].max().strftime("%Y-%m-%d")
        if df["Date"].notna().any()
        else pd.Timestamp.today().strftime("%Y-%m-%d")
    )

    records = []

    # === 1. Reform-level rows ===
    for _, row in df.iterrows():
        reform_area = row.get("Reform Type") or row.get("Other Reform") or "Unspecified Reform"
        reform_status = row.get("Reform Status", "Unspecified")
        status, ci1 = classify_status(reform_status)

        records.append({
            "Assumption": "Policy and regulatory openness",
            "Monitoring Tool": "Policy Reform",
            "Economy": row.get("Economy", "Unknown"),
            "Workstream": row.get("Workstream", "Unspecified"),
            "Level": "Signal",
            "Date": row["Date"].strftime("%Y-%m-%d") if pd.notnull(row["Date"]) else latest_date,
            "Signal": f"{reform_area} reform = {reform_status}",
            "Status": status,
            "Confidence Index 1 (Percent)": ci1,
            "Confidence Index 2 (Count)": 1,
            "Notes": "Individual reform status. CI1 graded: Adopted=100, In progress=75, Unsure=50, Not initiated=0."
        })

    # === 2. Economy-level summary ===
    if "Economy" in df.columns:
        for econ, subset in df.groupby("Economy"):
            total = len(subset)
            if total == 0:
                continue
            avg_score = subset["Reform Status"].apply(lambda x: classify_status(x)[1]).mean()
            econ_status = classify_percentage(avg_score)

            records.append({
                "Assumption": "Policy and regulatory openness",
                "Monitoring Tool": "Policy Reform",
                "Economy": econ,
                "Workstream": "All",
                "Level": "Economy",
                "Date": latest_date,
                "Signal": f"Average reform progress = {avg_score:.0f}%",
                "Status": econ_status,
                "Confidence Index 1 (Percent)": round(avg_score, 1),
                "Confidence Index 2 (Count)": total,
                "Notes": f"Economy-level summary. Thresholds: Optimistic ≥60%, Baseline 30–59%, "
                         f"Pessimistic <30%. Based on {total} reforms."
            })

    # === 3. Workstream-level summary ===
    if "Workstream" in df.columns:
        for ws, subset in df.groupby("Workstream"):
            total = len(subset)
            if total == 0:
                continue
            avg_score = subset["Reform Status"].apply(lambda x: classify_status(x)[1]).mean()
            ws_status = classify_percentage(avg_score)

            records.append({
                "Assumption": "Policy and regulatory openness",
                "Monitoring Tool": "Policy Reform",
                "Economy": "APEC (aggregate)",
                "Workstream": ws if ws else "Unspecified",
                "Level": "Workstream",
                "Date": latest_date,
                "Signal": f"Average reform progress = {avg_score:.0f}%",
                "Status": ws_status,
                "Confidence Index 1 (Percent)": round(avg_score, 1),
                "Confidence Index 2 (Count)": total,
                "Notes": f"Workstream-level summary. Thresholds: Optimistic ≥60%, Baseline 30–59%, "
                         f"Pessimistic <30%. Based on {total} reforms."
            })

    # === 4. APEC aggregate summary ===
    total_reforms = len(df)
    avg_score = df["Reform Status"].apply(lambda x: classify_status(x)[1]).mean()
    agg_status = classify_percentage(avg_score)

    records.append({
        "Assumption": "Policy and regulatory openness",
        "Monitoring Tool": "Policy Reform",
        "Economy": "APEC (aggregate)",
        "Workstream": "All",
        "Level": "Aggregate",
        "Date": latest_date,
        "Signal": f"Average reform progress = {avg_score:.0f}%",
        "Status": agg_status,
        "Confidence Index 1 (Percent)": round(avg_score, 1),
        "Confidence Index 2 (Count)": total_reforms,
        "Notes": f"Aggregate summary. Thresholds: Optimistic ≥60%, Baseline 30–59%, "
                 f"Pessimistic <30%. Based on {total_reforms} reforms."
    })

    # === Export ===
    out_df = pd.DataFrame(records)
    out_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Policy reform assumption saved → {OUTPUT_FILE} ({len(out_df)} rows))")


if __name__ == "__main__":
    main()
