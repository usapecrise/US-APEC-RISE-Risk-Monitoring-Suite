#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Attendance (Meetings/Dialogues) → Political and Institutional Continuity
------------------------------------------------------------------------
Generates assumption data for:
- Economy-level continuity (attendance across last 5 dialogues)
- Aggregate APEC continuity

Logic:
- Looks at the last 5 dialogue/meeting events
- Classifies optimism based on % attendance

Standardization:
- CI1 = Percent (% of last 5 dialogues attended)
- CI2 = Count (# of dialogues attended, 0–5)
- Thresholds: Optimistic ≥60%, Baseline 30–59%, Pessimistic <30%
"""

import os
import requests
import pandas as pd

# Airtable Config
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
BASE_ID = "app0Ljjhrp3lTTpTO"

TABLES = {
    "Other Sign-Ins (Meetings/Dialogues)": "tbl6qMYkcIzkl8q7D"
}
VIEW_ID = None
APEC_TOTAL = 21   # denominator for % calculations

def fetch_table(table_label, table_id):
    url = f"https://api.airtable.com/v0/{BASE_ID}/{table_id}"
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    records, offset = [], None

    while True:
        params = {"view": VIEW_ID} if VIEW_ID else {}
        if offset:
            params["offset"] = offset

        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            raise RuntimeError(f"Airtable API error {resp.status_code} for {table_label}: {resp.text}")

        data = resp.json()
        for r in data.get("records", []):
            f = r.get("fields", {})
            records.append({
                "Workshop": f.get("Workshop", ""),
                "Workshop Date": f.get("Workshop Date", ""),
                "Economy": f.get("Economy", f.get("Economy or Guest", [])),
                "Organization": f.get("Organization", ""),
                "Source Table": table_label
            })

        offset = data.get("offset")
        if not offset:
            break

    return pd.DataFrame(records)

def safe_format_date(series):
    """Return YYYY-MM-DD if series has valid dates, else empty string."""
    max_date = pd.to_datetime(series, errors="coerce").max()
    return max_date.strftime("%Y-%m-%d") if pd.notna(max_date) else ""

def main():
    # === 1. Fetch data ===
    dfs = [fetch_table(label, tid) for label, tid in TABLES.items()]
    df = pd.concat(dfs, ignore_index=True)

    if df.empty:
        print("⚠️ No attendance data found, writing empty continuity file")
        pd.DataFrame(columns=[
            "Assumption","Monitoring Tool","Economy","Workstream","Level","Date",
            "Signal","Status","Confidence Index 1 (Percent)","Confidence Index 2 (Count)","Notes"
        ]).to_csv("attendance_continuity_assumption.csv", index=False)
        return

    # Normalize
    df["Workshop Date"] = pd.to_datetime(df["Workshop Date"], errors="coerce")
    df["Economy"] = df["Economy"].apply(lambda x: x if isinstance(x, list) else [x] if x else [])
    df = df.explode("Economy")
    df["Economy"] = df["Economy"].str.strip()
    df["Workshop Key"] = df["Workshop"].astype(str) + " | " + df["Workshop Date"].astype(str)

    # === 2. Identify last 5 events ===
    event_order = (
        df.groupby("Workshop Key")["Workshop Date"].min()
        .sort_values(ascending=False)
        .index
    )
    last5_events = list(event_order[:5])
    last5_df = df[df["Workshop Key"].isin(last5_events)]

    if last5_df.empty:
        print("⚠️ No recent events found, skipping continuity analysis")
        return

    # === 3. Economy-level responsiveness ===
    economy_stats = (
        last5_df.groupby("Economy")["Workshop Key"].nunique()
        .reset_index(name="Events_Attended")
    )

    records = []
    for _, row in economy_stats.iterrows():
        economy = row["Economy"]
        attended = row["Events_Attended"]
        pct_attended = (attended / 5) * 100

        if pct_attended >= 60:
            status = "optimistic"
        elif pct_attended >= 30:
            status = "baseline"
        else:
            status = "pessimistic"

        records.append({
            "Assumption": "Political and institutional continuity",
            "Monitoring Tool": "Attendance",
            "Economy": economy,
            "Workstream": "All",
            "Level": "Economy",
            "Date": safe_format_date(last5_df["Workshop Date"]),
            "Signal": f"{economy} attended {attended}/5 most recent dialogues",
            "Status": status,
            "Confidence Index 1 (Percent)": round(pct_attended, 1),
            "Confidence Index 2 (Count)": attended,
            "Notes": "CI1 = % of last 5 dialogues attended. CI2 = number of dialogues attended (0–5). "
                     "Thresholds: Optimistic ≥60, Baseline 30–59, Pessimistic <30."
        })

    # === 4. APEC aggregate continuity ===
    avg_attended = economy_stats["Events_Attended"].mean() if not economy_stats.empty else 0
    pct_agg = (avg_attended / 5) * 100

    if pct_agg >= 60:
        agg_status = "optimistic"
    elif pct_agg >= 30:
        agg_status = "baseline"
    else:
        agg_status = "pessimistic"

    records.append({
        "Assumption": "Political and institutional continuity",
        "Monitoring Tool": "Attendance",
        "Economy": "APEC (aggregate)",
        "Workstream": "All",
        "Level": "Aggregate",
        "Date": safe_format_date(last5_df["Workshop Date"]),
        "Signal": f"On average, economies attended {avg_attended:.1f}/5 recent dialogues",
        "Status": agg_status,
        "Confidence Index 1 (Percent)": round(pct_agg, 1),
        "Confidence Index 2 (Count)": int(round(avg_attended, 0)),
        "Notes": "CI1 = % of last 5 dialogues attended (aggregate). CI2 = average number attended. "
                 "Thresholds: Optimistic ≥60, Baseline 30–59, Pessimistic <30."
    })

    # === 5. Export ===
    out_df = pd.DataFrame(records)
    out_df.to_csv("attendance_continuity_assumption.csv", index=False)
    print(f"✅ Continuity assumption saved → attendance_continuity_assumption.csv ({len(out_df)} rows))")


if __name__ == "__main__":
    main()
