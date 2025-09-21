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
- CI1 = total $ contributed
- CI2 = # of contributing firms
- Thresholds:
    * Year 1 (low bar):
        Optimistic ≥ $2k or ≥ 2 firms
        Baseline > $0
        Pessimistic = $0
    * Year 2+ (original):
        Optimistic ≥ historical avg (or $5k fallback)
        Baseline ≥ 25th percentile (or $1k fallback)
        Pessimistic < baseline
"""

import os
import requests
import pandas as pd
import datetime

# Airtable Config
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
BASE_ID = "app0Ljjhrp3lTTpTO"
TABLE_ID = "tblpb71OXvUUyJTVF"   # Cost-share (OT5)
VIEW_ID = "Grid view"

def fetch_ot5():
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    records, offset = [], None
    while True:
        params = {"view": VIEW_ID}
        if offset:
            params["offset"] = offset
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            raise RuntimeError(f"Airtable API error {resp.status_code}: {resp.text}")
        data = resp.json()
        for r in data.get("records", []):
            f = r.get("fields", {})
            def flatten(val):
                if isinstance(val, list): return [v.strip() for v in val]
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
        if not offset: break
    df = pd.DataFrame(records)

    # clean $
    def parse_amount(x):
        if pd.isna(x): return 0.0
        try: return float(str(x).replace("$", "").replace(",", ""))
        except: return 0.0
    df["Amount_clean"] = df["Amount"].apply(parse_amount)
    return df

# helpers
def explode_lists(df, col): return df.explode(col).reset_index(drop=True)
def split_ids(series):
    values=set()
    for val in series.dropna():
        if isinstance(val,str):
            for v in val.split(","): values.add(v.strip())
        elif isinstance(val,list):
            for v in val: values.add(str(v).strip())
    return values

def classify_dynamic(total, econ_count, firm_count, latest_fy, df_all):
    today = datetime.date.today()
    try:
        fy_int = int(latest_fy)
    except:
        fy_int = today.year

    project_year = fy_int - today.year + 1  # crude: assumes FY start = project start

    # --- Year 1 thresholds (baseline default if any contribution >0) ---
    if project_year <= 1:
        if total >= 2000 or firm_count >= 2:
            return "optimistic", "year1-lowbar"
        elif total > 0:
            return "baseline", "year1-lowbar"
        else:
            return "pessimistic", "year1-lowbar"

    # --- Year 2+ thresholds ---
    hist_df = df_all[df_all["Fiscal Year"].notna() & (df_all["ResourceOrigin"]=="Host Country-based")]
    yearly_totals = hist_df.groupby("Fiscal Year")["Amount_clean"].sum().reset_index(name="Total")
    if len(yearly_totals) >= 2:
        hist_avg = yearly_totals["Total"].mean()
        hist_p25 = yearly_totals["Total"].quantile(0.25)
        if total >= hist_avg and econ_count>=2 and firm_count>=2:
            return "optimistic","historical"
        elif total >= hist_p25 and econ_count>=1 and firm_count>=1:
            return "baseline","historical"
        else:
            return "pessimistic","historical"
    else:
        if total >= 5000 and econ_count>=2 and firm_count>=2:
            return "optimistic","static"
        elif total >= 1000 and econ_count>=1 and firm_count>=1:
            return "baseline","static"
        else:
            return "pessimistic","static"

# === main ===
df = fetch_ot5()
if df.empty:
    print("⚠️ No cost-share data found")
    pd.DataFrame(columns=[
        "Assumption","Monitoring Tool","Economy","Workstream","Level","Date",
        "Signal","Status","Confidence Index 1 (Amount)","Confidence Index 2 (Breadth)","Notes"
    ]).to_csv("cost_share_assumption.csv", index=False)
    exit()

df = df[df["ResourceOrigin"].isin(["Host Country-based"])]
latest_fy = df["Fiscal Year"].dropna().max() if "Fiscal Year" in df.columns else "Unknown"
df_latest = df[df["Fiscal Year"]==latest_fy] if latest_fy!="Unknown" else df

rows=[]
total_amount=df_latest["Amount_clean"].sum()
economies_count=len(split_ids(df_latest["Economy"]))
firms_count=len(split_ids(df_latest["Firm"]))
status,thresh_type=classify_dynamic(total_amount,economies_count,firms_count,latest_fy,df)

rows.append({
    "Assumption":"Responsible local ownership",
    "Monitoring Tool":"Cost-share",
    "Economy":"APEC (aggregate)",
    "Workstream":"All",
    "Level":"Aggregate",
    "Date":f"{latest_fy}-12-31" if latest_fy!="Unknown" else pd.Timestamp.today().strftime("%Y-%m-%d"),
    "Signal":f"${total_amount:,.0f} from {firms_count} firms across {economies_count} economies (FY {latest_fy})",
    "Status":status,
    "Confidence Index 1 (Amount)":round(total_amount,2),
    "Confidence Index 2 (Breadth)":firms_count,
    "Notes":f"Thresholds applied: {thresh_type}. CI1=$ amount, CI2=# firms."
})

# economy
df_econ=explode_lists(df_latest,"Economy")
for econ,g in df_econ.groupby("Economy"):
    econ_total=g["Amount_clean"].sum()
    econ_firms=len(split_ids(g["Firm"]))
    status,thresh_type=classify_dynamic(econ_total,1 if econ else 0,econ_firms,latest_fy,df)
    rows.append({
        "Assumption":"Responsible local ownership",
        "Monitoring Tool":"Cost-share",
        "Economy":econ,
        "Workstream":"All",
        "Level":"Economy",
        "Date":f"{latest_fy}-12-31",
        "Signal":f"${econ_total:,.0f} from {econ_firms} firms (FY {latest_fy})",
        "Status":status,
        "Confidence Index 1 (Amount)":round(econ_total,2),
        "Confidence Index 2 (Breadth)":econ_firms,
        "Notes":f"Thresholds applied: {thresh_type}. CI1=$ amount, CI2=# firms."
    })

# workstream
df_ws=explode_lists(df_latest,"Workstream")
for ws,g in df_ws.groupby("Workstream"):
    ws_total=g["Amount_clean"].sum()
    ws_firms=len(split_ids(g["Firm"]))
    ws_econs=len(split_ids(g["Economy"]))
    status,thresh_type=classify_dynamic(ws_total,ws_econs,ws_firms,latest_fy,df)
    rows.append({
        "Assumption":"Responsible local ownership",
        "Monitoring Tool":"Cost-share",
        "Economy":"APEC (aggregate)",
        "Workstream":ws,
        "Level":"Workstream",
        "Date":f"{latest_fy}-12-31",
        "Signal":f"${ws_total:,.0f} from {ws_firms} firms across {ws_econs} economies (FY {latest_fy})",
        "Status":status,
        "Confidence Index 1 (Amount)":round(ws_total,2),
        "Confidence Index 2 (Breadth)":ws_firms,
        "Notes":f"Thresholds applied: {thresh_type}. CI1=$ amount, CI2=# firms."
    })

pd.DataFrame(rows).to_csv("cost_share_assumption.csv",index=False)
print(f"✅ Cost-share assumption saved → cost_share_assumption.csv ({len(rows)} rows))")
