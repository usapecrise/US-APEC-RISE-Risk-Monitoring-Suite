import os
import requests
import pandas as pd

# Airtable Config
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
BASE_ID = "app0Ljjhrp3lTTpTO"
TABLE_NAME = "OC1 Policy Reforms"
VIEW_NAME = "Grid view"

def fetch_table(table_name):
    """Fetch all records from an Airtable table."""
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
                "Economy": f.get("Economy", ""),
                "Workstream": f.get("Workstream", ""),
                "Reform Status": f.get("Reform Adopted/Advanced", ""),
                "Reform Type": f.get("Reform Type", ""),
                "Other Reform": f.get("Other Reform", ""),
                "Notes": f.get("Notes", "")
            })

        offset = data.get("offset")
        if not offset:
            break

    return pd.DataFrame(records)

def classify_status(reform_status: str) -> str:
    """Map 3-category status into optimistic / baseline / pessimistic."""
    status = str(reform_status).strip().lower()

    if status in ["yes", "in progress", "under development"]:
        return "optimistic"
    elif status == "not yet initiated":
        return "pessimistic"
    elif status in ["unsure", "unsure/don't know", "don't know"]:
        return "baseline"
    else:
        return "baseline"

def main():
    df = fetch_table(TABLE_NAME)
    if df.empty:
        print("⚠️ No policy reform data found, skipping assumption export")
        return

    # Normalize date
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    records = []
    for _, row in df.iterrows():
        reform_area = row.get("Reform Type") or row.get("Other Reform") or "Unspecified Reform"
        reform_status = row.get("Reform Status", "")
        status = classify_status(reform_status)

        records.append({
            "assumption": "Policy and regulatory openness",
            "monitoring_tool": "policy_reform",
            "economy": row.get("Economy", "Unknown"),
            "date": row["Date"].strftime("%Y-%m-%d") if pd.notnull(row["Date"]) else "",
            "signal": f"{reform_area} reform = {reform_status}",
            "status": status,
            "notes": row.get("Notes", "")
        })

    out_df = pd.DataFrame(records)
    out_df.to_csv("policy_reform_assumption.csv", index=False)
    print(f"✅ Policy reform assumption saved → policy_reform_assumption.csv ({len(out_df)} rows)")

if __name__ == "__main__":
    main()

