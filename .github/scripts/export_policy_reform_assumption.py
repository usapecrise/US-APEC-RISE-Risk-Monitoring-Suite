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
    """Fetch all records from Airtable table."""
    url = f"https://api.airtable.com/v0/{BASE_ID}/{table_name}"
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    records, offset = [], None

    while True:
        params = {"view": VIEW_NAME}
        if offset:
            params["offset"] = offset
        resp = requests.get(url, headers=headers, params=params)
        data = resp.json()

        if "records" not in data:
            print(f"⚠️ Error fetching Airtable: {data}")
            break

        for r in data["records"]:
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
    """Map reform status into optimistic / baseline / pessimistic."""
    status = str(reform_status).strip().lower()

    optimistic_keywords = ["yes", "adopted", "in progress", "under development", "ongoing", "advanced"]
    unsure_keywords = ["unsure", "don't know", "unsure/don't know"]

    if status in optimistic_keywords:
        return "optimistic"
    elif status == "not yet initiated":
        return "pessimistic"
    elif status in unsure_keywords:
        return "baseline"
    else:
        return "baseline"


def classify_percentage(pct: float) -> str:
    """Classify percentage thresholds for summary rows."""
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

    # Normalize date
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    latest_date = df["Date"].max().strftime("%Y-%m-%d") if df["Date"].notna().any() else pd.Timestamp.today().strftime("%Y-%m-%d")

    records = []

    # === 1. Reform-level rows ===
    for _, row in df.iterrows():
        reform_area = row.get("Reform Type") or row.get("Other Reform") or "Unspecified Reform"
        reform_status = row.get("Reform Status", "")
        status = classify_status(reform_status)

        records.append({
            "Assumption": "Policy and regulatory openness",
            "Monitoring Tool": "Policy Reform",
            "Economy": row.get("Economy", "Unknown"),
            "Workstream": row.get("Workstream", "Unspecified"),
            "Date": row["Date"].strftime("%Y-%m-%d") if pd.notnull(row["Date"]) else latest_date,
            "Signal": f"{reform_area} reform = {reform_status}",
            "Status": status,
            "Notes": (row.get("Notes", "") + " Thresholds: Optimistic = Reform adopted/in progress; Baseline = Unsure; Pessimistic = Not initiated").strip()
        })

    # === 2. Economy-level summary rows ===
    if "Economy" in df.columns:
        for econ, subset in df.groupby("Economy"):
            total = len(subset)
            if total == 0:
                continue
            optimistic_count = subset["Reform Status"].apply(lambda x: classify_status(x) == "optimistic").sum()
            pct_optimistic = (optimistic_count / total) * 100
            econ_status = classify_percentage(pct_optimistic)

            records.append({
                "Assumption": "Policy and regulatory openness",
                "Monitoring Tool": "Policy Reform",
                "Economy": econ,
                "Workstream": "All",
                "Date": latest_date,
                "Signal": f"{pct_optimistic:.0f}% of reforms adopted or in progress",
                "Status": econ_status,
                "Notes": "Thresholds: Optimistic ≥60% of reforms underway; Baseline 30–59%; Pessimistic <30%"
            })

    # === 3. Workstream-level summary rows ===
    if "Workstream" in df.columns:
        for ws, subset in df.groupby("Workstream"):
            total = len(subset)
            if total == 0:
                continue
            optimistic_count = subset["Reform Status"].apply(lambda x: classify_status(x) == "optimistic").sum()
            pct_optimistic = (optimistic_count / total) * 100
            ws_status = classify_percentage(pct_optimistic)

            records.append({
                "Assumption": "Policy and regulatory openness",
                "Monitoring Tool": "Policy Reform",
                "Economy": "APEC (aggregate)",
                "Workstream": ws if ws else "Unspecified",
                "Date": latest_date,
                "Signal": f"{pct_optimistic:.0f}% of reforms adopted or in progress",
                "Status": ws_status,
                "Notes": "Thresholds: Optimistic ≥60% of reforms underway; Baseline 30–59%; Pessimistic <30%"
            })

    # === 4. APEC aggregate summary ===
    total_reforms = len(df)
    optimistic_count = df["Reform Status"].apply(lambda x: classify_status(x) == "optimistic").sum()
    pct_optimistic = (optimistic_count / total_reforms) * 100 if total_reforms > 0 else 0
    agg_status = classify_percentage(pct_optimistic)

    records.append({
        "Assumption": "Policy and regulatory openness",
        "Monitoring Tool": "Policy Reform",
        "Economy": "APEC (aggregate)",
        "Workstream": "All",
        "Date": latest_date,
        "Signal": f"{pct_optimistic:.0f}% of reforms adopted or in progress",
        "Status": agg_status,
        "Notes": "Thresholds: Optimistic ≥60% of reforms underway; Baseline 30–59%; Pessimistic <30%"
    })

    # === Export ===
    out_df = pd.DataFrame(records)
    out_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Policy reform assumption saved → {OUTPUT_FILE} ({len(out_df)} rows)")


if __name__ == "__main__":
    main()
