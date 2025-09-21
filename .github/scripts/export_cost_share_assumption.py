#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Cost-share (OT5) → Responsible Local Ownership
----------------------------------------------
Generates assumption data for:
- Aggregate APEC cost-share
- Economy-level cost-share
- Workstream-level cost-share

Logic:
- Uses Host Country-based contributions
- Tracks latest Fiscal Year
- CI1 = Amount ($ contributed)
- CI2 = Count (# of contributing firms)
- Thresholds:
    * If ≥2 years of history → dynamic: Optimistic = ≥ historical avg, Baseline = ≥ 25th percentile
    * Else → fallback: static $ thresholds ($5k / $1k), scaled by fiscal year progress
"""

import os
import requests
import pandas as pd
import datetime

# Airtable Config
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
BASE_ID = "app0Ljjhrp3lTTpTO"
TABLE_ID = "tblpb71OXvUUyJTVF"   # Cost-share (OT5) table
VIEW_ID = "Grid view"

def fetch_ot5():
    """Fetch OT5 Private Sector Resources data from Airtable."""
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
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

            def flatten(val):
                if isinstance(val, list):
                    return [v.strip() for v in val]
                return [val] if val else []

            records.append({
                "Economy": flatten(f.get("Economy", [])),
                "Firm": flatten(f.get("Firm", [])),
                "Workstream": flatten(f.get("Workstream", [])),
                "Fiscal Year": f.get("Fiscal Year", ""),
                "ResourceOrigin": f.get("Resource Origin", ""),
                "Amount": f.get("Amount", 0),
                "Engagement": flatten(f.get("Engagement", []))
            })

        offset = data.get("offset")
        if not offset:
            break

    df = pd.DataFrame(records)

    # Clean amounts
    def parse_amount(x):
        if pd.isna(x): return 0.0
        try:
            return float(str(x).replace("$", "").replace(",", ""))
        except:
            return 0.0
    df["Amount_clean"] = df["Amount"].apply(parse_amount)

    return df

# === Helpers ===
def explode_lists(df, col):
    """Explode list-type columns into multiple rows."""
    return df.explode(col).reset_index(drop=True)

def split_ids(series):
    """Return unique IDs/names across rows."""
    values = set()
    for val in series.dropna():
        if isinstance(val, str):
            for v in val.split(","):
                values.add(v.strip())
        elif isinstance(val, list):
            for v in val:
                values.add(str(v).strip())
    return values

def classify_with_history(total, econ_count, firm_count, latest_fy, df_all):
    """Classify scenario status based on $ amount, economies, firms, and historical OT5 data."""
    today = datetime.date.today()

    # --- Historical benchmarks ---
    hist_df = df_all[df_all["Fiscal Year"].notna() & (df_all["ResourceOrigin"] == "Host Country-based")]
    yearly_totals = (
        hist_df.groupby("Fiscal Year")["Amount_clean"].sum()
        .reset_index(name="Total")
    )

    if len(yearly_totals) >= 2:
        hist_avg = yearly_totals["Total"].mean()
        hist_p25 = yearly_totals["Total"].quantile(0.25)
        threshold_opt = hist_avg
        threshold_base = hist_p25
        threshold_type = "historical"
    else:
        # Fallback: static thresholds, scaled by fiscal year progress
        try:
            fy_year = int(latest_fy)
        except:
            fy_year = today.year
        fy_start = datetime.date(fy_year, 1, 1)
        months_into_fy = (today.year - fy_start.year) * 12 + (today.month - fy_start.month)
        progress = max(1, months_into_fy) / 12
        threshold_opt = 5000 * progress
        threshold_base = 1000 * progress
        threshold_type = "static"

    # --- Classification ---
    if total >= threshold_opt and econ_count >= 2 and firm_count >= 2:
        return "optimistic", threshold_type
    elif total >= threshold_base and econ_count >= 1 and firm_count >= 1:
        return "baseline", threshold_type
    else:
        return "pessimistic", threshold_type

# === Load data ===
df = fetch_ot5()
if df.empty:
    print("⚠️ No cost-share data found, writing empty file")
    pd.DataFrame(columns=[
        "Assumption","Monitoring Tool","Economy","Workstream","Level","Date",
        "Signal","Status","Confidence Index 1 (Amount $)","Confidence Index 2 (Count)","Notes"
    ]).to_csv("cost_share_assumption.csv", index=False)
    exit()

# Filter only Host Country-based
df = df[df["ResourceOrigin"].isin(["Host Country-based"])]

# Pick latest FY
if "Fiscal Year" in df.columns and not df["Fiscal Year"].dropna().empty:
    latest_fy = df["Fiscal Year"].dropna().max()
    df_latest = df[df["Fiscal Year"] == latest_fy]
else:
    latest_fy = "Unknown"
    df_latest = df

# === Build outputs ===
rows = []

# Aggregate level
total_amount = df_latest["Amount_clean"].sum()
economies_count = len(split_ids(df_latest["Economy"]))
firms_count = len(split_ids(df_latest["Firm"]))
agg_status, thresh_type = classify_with_history(total_amount, economies_count, firms_count, latest_fy, df)

rows.append({
    "Assumption": "Responsible local ownership",
    "Monitoring Tool": "Cost-share",
    "Economy": "APEC (aggregate)",
    "Workstream": "All",
    "Level": "Aggregate",
    "Date": f"{latest_fy}-12-31" if latest_fy != "Unknown" else pd.Timestamp.today().strftime("%Y-%m-%d"),
    "Signal": f"${total_amount:,.0f} from {firms_count} firms across {economies_count} economies (FY {latest_fy}, Host Country-based only)",
    "Status": agg_status,
    "Confidence Index 1 (Amount $)": round(total_amount, 2),
    "Confidence Index 2 (Count)": firms_count,
    "Notes": f"Host Country-based cost-share only. CI1 = $ amount; CI2 = # firms contributing. Thresholds based on {thresh_type} values."
})

# Economy level
df_econ = explode_lists(df_latest, "Economy")
for econ, g in df_econ.groupby("Economy"):
    econ_total = g["Amount_clean"].sum()
    econ_firms = len(split_ids(g["Firm"]))
    econ_status, thresh_type = classify_with_history(econ_total, 1 if econ else 0, econ_firms, latest_fy, df)

    rows.append({
        "Assumption": "Responsible local ownership",
        "Monitoring Tool": "Cost-share",
        "Economy": econ,
        "Workstream": "All",
        "Level": "Economy",
        "Date": f"{latest_fy}-12-31" if latest_fy != "Unknown" else pd.Timestamp.today().strftime("%Y-%m-%d"),
        "Signal": f"${econ_total:,.0f} from {econ_firms} firms (FY {latest_fy}, Host Country-based only)",
        "Status": econ_status,
        "Confidence Index 1 (Amount $)": round(econ_total, 2),
        "Confidence Index 2 (Count)": econ_firms,
        "Notes": f"Host Country-based cost-share only. CI1 = $ amount; CI2 = # firms contributing. Thresholds based on {thresh_type} values."
    })

# Workstream level
df_ws = explode_lists(df_latest, "Workstream")
for ws, g in df_ws.groupby("Workstream"):
    ws_total = g["Amount_clean"].sum()
    ws_firms = len(split_ids(g["Firm"]))
    ws_econs = len(split_ids(g["Economy"]))
    ws_status, thresh_type = classify_with_history(ws_total, ws_econs, ws_firms, latest_fy, df)

    rows.append({
        "Assumption": "Responsible local ownership",
        "Monitoring Tool": "Cost-share",
        "Economy": "APEC (aggregate)",
        "Workstream": ws,
        "Level": "Workstream",
        "Date": f"{latest_fy}-12-31" if latest_fy != "Unknown" else pd.Timestamp.today().strftime("%Y-%m-%d"),
        "Signal": f"${ws_total:,.0f} from {ws_firms} firms across {ws_econs} economies (FY {latest_fy}, Host Country-based only)",
        "Status": ws_status,
        "Confidence Index 1 (Amount $)": round(ws_total, 2),
        "Confidence Index 2 (Count)": ws_firms,
        "Notes": f"Host Country-based cost-share only. CI1 = $ amount; CI2 = # firms contributing. Thresholds based on {thresh_type} values."
    })

# Export
assumption_df = pd.DataFrame(rows)
assumption_df.to_csv("cost_share_assumption.csv", index=False)
print(f"✅ Cost-share assumption saved → cost_share_assumption.csv ({len(rows)} rows))")
