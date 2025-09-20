#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Attendance → Stakeholder Alignment Assumption
---------------------------------------------
Generates assumption data for:
- Aggregate APEC participation
- Workstream-level participation
- Economy-level participation

Logic:
- Looks at the last 5 dialogues/workshops
- Measures how many economies participated
- Classifies optimism based on thresholds
"""

import os
import requests
import pandas as pd

# Airtable Config
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
BASE_ID = "app0Ljjhrp3lTTpTO"

TABLES = {
    "OT1 Sign-Ins (Workshops)": "tblIpPKx5wzr42YZX",
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
            raise RuntimeError(f"Airtable API error {resp.status_code}: {resp.text}")
        data = resp.json()
        for r in data.get("records", []):
            f = r.get("fields", {})
            records.append({
                "Workshop": f.get("Workshop", ""),
                "Workshop Date": f.get("Workshop Date", ""),
                "Workstream": f.get("Workstream", []),
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

# === Main ===
dfs = [fetch_table(label, tid) for label, tid in TABLES.items()]
df = pd.concat(dfs, ignore_index=True)

# Ensure lists are exploded into separate rows
df["Economy"] = df["Economy"].apply(lambda x: x if isinstance(x, list) else [x] if x else [])
df["Workstream"] = df["Workstream"].apply(lambda x: x if isinstance(x, list) else [x] if x else [])
df = df.explode("Economy").explode("Workstream")
df["Economy"] = df["Economy"].str.strip()
df["Workstream"] = df["Workstream"].str.strip()

# Add workshop key
df["Workshop Key"] = df["Workshop"].astype(str) + " | " + df["Workshop Date"].astype(str)
df["Workshop Date"] = pd.to_datetime(df["Workshop Date"], errors="coerce")

rows = []

# --- Aggregate level ---
workshop_stats = (
    df.groupby("Workshop Key")
    .agg({"Economy": "nunique", "Workshop Date": "first"})
    .reset_index()
    .sort_values("Workshop Date", ascending=False)
)
last5 = workshop_stats.head(5)
economies_present = last5["Economy"].mean()
pct = (economies_present / APEC_TOTAL) * 100
status = "optimistic" if pct >= 60 else "baseline" if pct >= 30 else "pessimistic"

rows.append({
    "Assumption": "Stakeholder alignment with U.S. focus areas",
    "Monitoring Tool": "Attendance",
    "Economy": "APEC (aggregate)",
    "Workstream": "All",
    "Level": "Aggregate",
    "Date": safe_format_date(last5["Workshop Date"]),
    "Signal": f"Average {economies_present:.1f} economies represented (last 5 dialogues)",
    "Status": status,
    "Confidence Index 1 (Percent)": round(pct, 1),
    "Confidence Index 2 (Breadth)": int(round(economies_present, 0)),
    "Notes": "Thresholds: Optimistic ≥60%, Baseline 30–59%, Pessimistic <30% of APEC economies."
})

# --- Workstream level ---
for ws, g in df.groupby("Workstream"):
    ws_stats = (
        g.groupby("Workshop Key")
        .agg({"Economy": "nunique", "Workshop Date": "first"})
        .reset_index()
        .sort_values("Workshop Date", ascending=False)
    )
    last5_ws = ws_stats.head(5)
    if last5_ws.empty:
        continue
    economies_present_ws = last5_ws["Economy"].mean()
    pct_ws = (economies_present_ws / APEC_TOTAL) * 100
    status_ws = "optimistic" if pct_ws >= 60 else "baseline" if pct_ws >= 30 else "pessimistic"

    rows.append({
        "Assumption": "Stakeholder alignment with U.S. focus areas",
        "Monitoring Tool": "Attendance",
        "Economy": "APEC (aggregate)",
        "Workstream": ws if ws else "Unspecified",
        "Level": "Workstream",
        "Date": safe_format_date(last5_ws["Workshop Date"]),
        "Signal": f"Average {economies_present_ws:.1f} economies represented (last 5 {ws} dialogues)",
        "Status": status_ws,
        "Confidence Index 1 (Percent)": round(pct_ws, 1),
        "Confidence Index 2 (Breadth)": int(round(economies_present_ws, 0)),
        "Notes": "Thresholds: Optimistic ≥60%, Baseline 30–59%, Pessimistic <30%."
    })

    # --- Economy level ---
    for econ, ge in g.groupby("Economy"):
        econ_ws_stats = (
            ge.groupby("Workshop Key")
            .agg({"Economy": "count", "Workshop Date": "first"})
            .reset_index()
            .sort_values("Workshop Date", ascending=False)
        )
        last5_econ_ws = econ_ws_stats.head(5)
        if last5_econ_ws.empty:
            continue
        attended_count = (last5_econ_ws["Economy"] > 0).sum()
        pct_attended = (attended_count / 5) * 100

        # thresholds: 3–5 = optimistic, 2 = baseline, 0–1 = pessimistic
        if attended_count >= 3:
            status_econ = "optimistic"
        elif attended_count == 2:
            status_econ = "baseline"
        else:
            status_econ = "pessimistic"

        rows.append({
            "Assumption": "Stakeholder alignment with U.S. focus areas",
            "Monitoring Tool": "Attendance",
            "Economy": econ,
            "Workstream": ws if ws else "Unspecified",
            "Level": "Economy",
            "Date": safe_format_date(last5_econ_ws["Workshop Date"]),
            "Signal": f"{econ} attended {attended_count}/5 {ws} dialogues",
            "Status": status_econ,
            "Confidence Index 1 (Percent)": round(pct_attended, 1),
            "Confidence Index 2 (Breadth)": attended_count,
            "Notes": "Thresholds: Optimistic ≥3/5, Baseline 2/5, Pessimistic ≤1/5."
        })

attendance_status = pd.DataFrame(rows)
attendance_status.to_csv("attendance_assumption.csv", index=False)
print(f"✅ Attendance assumption saved → attendance_assumption.csv ({len(rows)} rows))")
