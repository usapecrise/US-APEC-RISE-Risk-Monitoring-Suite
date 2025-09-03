import os
import requests
import pandas as pd

# Airtable Config
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
BASE_ID = "app0Ljjhrp3lTTpTO"
TABLE_ID = "OT5 Private Sector Resources"
VIEW_ID = "Grid view"

def fetch_ot5():
    """Fetch OT5 Private Sector Resources data from Airtable."""
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    records, offset = [], None

    while True:
        params = {}
        if VIEW_ID:
            params["view"] = VIEW_ID
        if offset:
            params["offset"] = offset

        resp = requests.get(url, headers=headers, params=params)
        print("DEBUG response:", resp.status_code, resp.text[:200])

        if resp.status_code != 200:
            raise RuntimeError(f"Airtable API error {resp.status_code}: {resp.text}")

        data = resp.json()
        for r in data.get("records", []):
            f = r.get("fields", {})
            records.append({
                "Economy": f.get("Economy", ""),
                "Firm": f.get("Firm", ""),
                "Workstream": f.get("Workstream", ""),
                "Fiscal Year": f.get("Fiscal Year", ""),
                "Type": f.get("Type", ""),
                "Amount": f.get("Amount", 0)
            })

        offset = data.get("offset")
        if not offset:
            break

    return pd.DataFrame(records)


# === Load data from Airtable ===
df = fetch_ot5()

# === Clean Amount field ===
def parse_amount(x):
    if pd.isna(x):
        return 0.0
    try:
        return float(str(x).replace("$", "").replace(",", ""))
    except:
        return 0.0

df["Amount_clean"] = df["Amount"].apply(parse_amount)

# === Filter Home Economy only ===
if "Type" in df.columns:
    df = df[df["Type"].str.contains("Home", case=False, na=False)]

# === Pick latest fiscal year ===
if "Fiscal Year" in df.columns and not df["Fiscal Year"].dropna().empty:
    latest_fy = df["Fiscal Year"].dropna().max()
    df_latest = df[df["Fiscal Year"] == latest_fy]
else:
    latest_fy = "Unknown"
    df_latest = df

# === Classification rules (absolute thresholds for host-only) ===
def classify(total, econ_count, firm_count):
    """Classify cost-share signal based on host contributions only."""
    if total >= 5000 and econ_count >= 2 and firm_count >= 2:
        return "optimistic"
    elif total >= 1000 and econ_count >= 1 and firm_count >= 1:
        return "baseline"
    else:
        return "pessimistic"

rows = []

# === Aggregate APEC-wide signal ===
total_amount = df_latest["Amount_clean"].sum()
economies_count = df_latest["Economy"].nunique()
firms_count = df_latest["Firm"].nunique()

rows.append({
    "Assumption": "Responsible local ownership",
    "Monitoring Tool": "Cost-share",
    "Economy": "APEC (aggregate)",
    "Workstream": "All",
    "Level": "Aggregate",
    "Date": f"{latest_fy}-12-31" if latest_fy != "Unknown" else pd.Timestamp.today().strftime("%Y-%m-%d"),
    "Signal": f"${total_amount:,.0f} from {firms_count} firms across {economies_count} economies (FY {latest_fy}, Home Economy only)",
    "Status": classify(total_amount, economies_count, firms_count),
    "Notes": "Home Economy cost-share only (excludes USG and third-party). Thresholds: Optimistic ≥$5,000 from ≥2 firms across ≥2 economies; Baseline ≥$1,000 from ≥1 firm; Pessimistic < baseline"
})

# === Economy-level signals ===
for econ, g in df_latest.groupby("Economy"):
    econ_total = g["Amount_clean"].sum()
    econ_firms = g["Firm"].nunique()
    scenario_econ = classify(econ_total, 1 if econ else 0, econ_firms)

    rows.append({
        "Assumption": "Responsible local ownership",
        "Monitoring Tool": "Cost-share",
        "Economy": econ,
        "Workstream": "All",
        "Level": "Economy",
        "Date": f"{latest_fy}-12-31" if latest_fy != "Unknown" else pd.Timestamp.today().strftime("%Y-%m-%d"),
        "Signal": f"${econ_total:,.0f} from {econ_firms} firms (FY {latest_fy}, Home Economy only)",
        "Status": scenario_econ,
        "Notes": "Home Economy cost-share only. Thresholds: Optimistic ≥$5,000 from ≥2 firms; Baseline ≥$1,000 from ≥1 firm; Pessimistic < baseline"
    })

# === Workstream-level signals ===
if "Workstream" in df_latest.columns:
    for ws, g in df_latest.groupby("Workstream"):
        ws_total = g["Amount_clean"].sum()
        ws_econs = g["Economy"].nunique()
        ws_firms = g["Firm"].nunique()
        scenario_ws = classify(ws_total, ws_econs, ws_firms)

        rows.append({
            "Assumption": "Responsible local ownership",
            "Monitoring Tool": "Cost-share",
            "Economy": "APEC (aggregate)",
            "Workstream": ws,
            "Level": "Workstream",
            "Date": f"{latest_fy}-12-31" if latest_fy != "Unknown" else pd.Timestamp.today().strftime("%Y-%m-%d"),
            "Signal": f"${ws_total:,.0f} from {ws_firms} firms across {ws_econs} economies (FY {latest_fy}, Home Economy only)",
            "Status": scenario_ws,
            "Notes": "Home Economy cost-share only. Thresholds: Optimistic ≥$5,000 from ≥2 firms across ≥2 economies; Baseline ≥$1,000 from ≥1 firm; Pessimistic < baseline"
        })

# === Export ===
assumption_df = pd.DataFrame(rows)
assumption_df.to_csv("cost_share_assumption.csv", index=False)
print(f"✅ Cost-share assumption saved → cost_share_assumption.csv ({len(rows)} rows)")

