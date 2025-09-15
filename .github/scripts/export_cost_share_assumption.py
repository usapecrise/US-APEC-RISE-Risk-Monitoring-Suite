import os
import requests
import pandas as pd
import datetime

# Airtable Config
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
BASE_ID = "app0Ljjhrp3lTTpTO"
TABLE_ID = "tblpb71OXvUUyJTVF"   # Cost-share (OT5) table ID
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

            # flatten linked record lists
            def flatten(val):
                if isinstance(val, list):
                    return ", ".join(val)
                return val

            records.append({
                "Economy": flatten(f.get("economy", [])),
                "Firm": flatten(f.get("firm", [])),
                "Workstream": flatten(f.get("workstream", [])),
                "Fiscal Year": f.get("fiscal year", ""),
                "ResourceOrigin": f.get("resource origin", ""),
                "Amount": f.get("amount", 0),
                "Engagement": flatten(f.get("engagement", []))
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


# === Load data ===
df = fetch_ot5()

# === Debug logging ===
print("🔍 Raw Airtable preview (first 5 rows):")
print(df.head(5).to_dict(orient="records"))
print("🔍 Columns returned:", df.columns.tolist())
print("🔍 Row count before Host-Country filter:", len(df))

# ✅ Filter only Host-Country based
df = df[df["ResourceOrigin"].str.contains("Host-Country", case=False, na=False)]
print("🔍 Row count after Host-Country filter:", len(df))

# Pick latest FY
if "Fiscal Year" in df.columns and not df["Fiscal Year"].dropna().empty:
    latest_fy = df["Fiscal Year"].dropna().max()
    df_latest = df[df["Fiscal Year"] == latest_fy]
else:
    latest_fy = "Unknown"
    df_latest = df


# === Classification rules ===
def classify_with_time(total, econ_count, firm_count, latest_fy):
    """Classify cost-share signal with early-year buffer + scaled thresholds."""
    today = datetime.date.today()
    try:
        fy_year = int(latest_fy)
    except:
        fy_year = today.year

    # Adjust FY start (assuming Jan 1; switch to Oct 1 if fiscal year = US FY)
    fy_start = datetime.date(fy_year, 1, 1)
    months_into_fy = (today.year - fy_start.year) * 12 + (today.month - fy_start.month)
    progress = max(1, months_into_fy) / 12  # scale 1–12 months into fraction

    # Early-year buffer: first 3 months never pessimistic
    if months_into_fy < 3 and total == 0:
        return "baseline"

    # Scaled thresholds
    threshold_opt = 5000 * progress
    threshold_base = 1000 * progress

    if total >= threshold_opt and econ_count >= 2 and firm_count >= 2:
        return "optimistic"
    elif total >= threshold_base and econ_count >= 1 and firm_count >= 1:
        return "baseline"
    else:
        return "pessimistic"


# === Build outputs ===
rows = []

# Aggregate level
total_amount = df_latest["Amount_clean"].sum()
economies_count = df_latest["Economy"].nunique()
firms_count = df_latest["Firm"].nunique()
agg_status = classify_with_time(total_amount, economies_count, firms_count, latest_fy)

rows.append({
    "Assumption": "Responsible local ownership",
    "Monitoring Tool": "Cost-share",
    "Economy": "APEC (aggregate)",
    "Workstream": "All",
    "Level": "Aggregate",
    "Date": f"{latest_fy}-12-31" if latest_fy != "Unknown" else pd.Timestamp.today().strftime("%Y-%m-%d"),
    "Signal": f"${total_amount:,.0f} from {firms_count} firms across {economies_count} economies (FY {latest_fy}, Host-Country based only)",
    "Status": agg_status,
    "Confidence Index 1 (Amount)": total_amount,
    "Confidence Index 2 (Breadth)": firms_count + economies_count,
    "Notes": "Host-country cost-share only. CI1 = $ amount; CI2 = firms + economies contributing. Thresholds scale with FY progress; early months buffered."
})

# Economy level
for econ, g in df_latest.groupby("Economy"):
    econ_total = g["Amount_clean"].sum()
    econ_firms = g["Firm"].nunique()
    scenario_econ = classify_with_time(econ_total, 1 if econ else 0, econ_firms, latest_fy)

    rows.append({
        "Assumption": "Responsible local ownership",
        "Monitoring Tool": "Cost-share",
        "Economy": econ,
        "Workstream": "All",
        "Level": "Economy",
        "Date": f"{latest_fy}-12-31" if latest_fy != "Unknown" else pd.Timestamp.today().strftime("%Y-%m-%d"),
        "Signal": f"${econ_total:,.0f} from {econ_firms} firms (FY {latest_fy}, Host-Country based only)",
        "Status": scenario_econ,
        "Confidence Index 1 (Amount)": econ_total,
        "Confidence Index 2 (Breadth)": econ_firms,
        "Notes": "Host-country cost-share only. CI1 = $ amount; CI2 = # firms contributing."
    })

# Workstream level
if "Workstream" in df_latest.columns:
    for ws, g in df_latest.groupby("Workstream"):
        ws_total = g["Amount_clean"].sum()
        ws_econs = g["Economy"].nunique()
        ws_firms = g["Firm"].nunique()
        scenario_ws = classify_with_time(ws_total, ws_econs, ws_firms, latest_fy)

        rows.append({
            "Assumption": "Responsible local ownership",
            "Monitoring Tool": "Cost-share",
            "Economy": "APEC (aggregate)",
            "Workstream": ws,
            "Level": "Workstream",
            "Date": f"{latest_fy}-12-31" if latest_fy != "Unknown" else pd.Timestamp.today().strftime("%Y-%m-%d"),
            "Signal": f"${ws_total:,.0f} from {ws_firms} firms across {ws_econs} economies (FY {latest_fy}, Host-Country based only)",
            "Status": scenario_ws,
            "Confidence Index 1 (Amount)": ws_total,
            "Confidence Index 2 (Breadth)": ws_firms + ws_econs,
            "Notes": "Host-country cost-share only. CI1 = $ amount; CI2 = firms + economies contributing. Thresholds scale with FY progress; early months buffered."
        })

# Export
assumption_df = pd.DataFrame(rows)
assumption_df.to_csv("cost_share_assumption.csv", index=False)
print(f"✅ Cost-share assumption saved → cost_share_assumption.csv ({len(rows)} rows))")
