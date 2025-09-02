# .github/scripts/export_cost_share_assumption.py
import os
import pandas as pd

# Load data
file_path = "OT5 Private Sector Resources-Grid view.csv"  # adjust path if needed
df = pd.read_csv(file_path)

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

# === Aggregate indicators ===
total_amount = df_latest["Amount_clean"].sum()
economies_count = df_latest["Economy"].nunique()
firms_count = df_latest["Firm"].nunique()

# === Classification rules (lower thresholds) ===
def classify(total, econ_count, firm_count):
    if total >= 5000 and econ_count >= 2 and firm_count >= 2:
        return "optimistic"
    elif total >= 1000 and econ_count >= 1 and firm_count >= 1:
        return "baseline"
    else:
        return "pessimistic"

# === Aggregate signal ===
rows = [{
    "assumption": "Private sector cost-share commitments sustained",
    "monitoring_tool": "cost_share",
    "economy": "APEC (aggregate)",
    "workstream": "All",
    "level": "aggregate",
    "date": pd.Timestamp.today().strftime("%Y-%m-%d"),
    "signal": f"${total_amount:,.0f} from {firms_count} firms across {economies_count} economies (FY {latest_fy}, Home Economy only)",
    "status": classify(total_amount, economies_count, firms_count),
    "notes": "Aggregate private sector resources recorded in OT5"
}]

# === Economy-level signals ===
for econ, g in df_latest.groupby("Economy"):
    econ_total = g["Amount_clean"].sum()
    econ_firms = g["Firm"].nunique()
    scenario_econ = classify(econ_total, 1 if econ else 0, econ_firms)

    rows.append({
        "assumption": "Private sector cost-share commitments sustained",
        "monitoring_tool": "cost_share",
        "economy": econ,
        "workstream": "All",
        "level": "economy",
        "date": pd.Timestamp.today().strftime("%Y-%m-%d"),
        "signal": f"${econ_total:,.0f} from {econ_firms} firms (FY {latest_fy}, Home Economy)",
        "status": scenario_econ,
        "notes": "Economy-specific private sector resources recorded in OT5"
    })

# === Export ===
assumption_df = pd.DataFrame(rows)
assumption_df.to_csv("cost_share_assumption.csv", index=False)
print(f"✅ Cost-share assumption saved → cost_share_assumption.csv ({len(rows)} rows)")
