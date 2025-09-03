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
    latest_date = df["Date"].max().strftime("%Y-%m-%d") if df["Date"].notna().any() else pd.Timestamp.today().strftime("%Y-%m-%d")

    records = []

    # === 1. Reform-level rows ===
    for _, row in df.iterrows():
        reform_area = row.get("Reform Type") or row.get("Other Reform") or "Unspecified Reform"
        reform_status = row.get("Reform Status", "")
        status = classify_status(reform_status)

        # Map to CI1 percent
        if status == "optimistic":
            ci1 = 100
        elif status == "baseline":
            ci1 = 50
        else:
            ci1 = 0

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
            "Notes": "Individual reform status. Thresholds: Optimistic = Adopted/In progress; Baseline = Unsure; Pessimistic = Not initiated."
        })

    # === 2. Economy-level summary ===
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
                "Level": "Economy",
                "Date": latest_date,
                "Signal": f"{pct_optimistic:.0f}% of reforms adopted or in progress",
                "Status": econ_status,
                "Confidence Index 1 (Percent)": round(pct_optimistic, 1),
                "Confidence Index 2 (Count)": total,
                "Notes": f"Economy-level summary. Thresholds: Optimistic ≥60%, Baseline 30–59%, Pessimistic <30%. Based on {total} reforms."
            })

    # === 3. Workstream-level summary ===
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
                "Level": "Workstream",
                "Date": latest_date,
                "Signal": f"{pct_optimistic:.0f}% of reforms adopted or in progress",
                "Status": ws_status,
                "Confidence Index 1 (Percent)": round(pct_optimistic, 1),
                "Confidence Index 2 (Count)": total,
                "Notes": f"Workstream-level summary. Thresholds: Optimistic ≥60%, Baseline 30–59%, Pessimistic <30%. Based on {total} reforms."
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
        "Level": "Aggregate",
        "Date": latest_date,
        "Signal": f"{pct_optimistic:.0f}% of reforms adopted or in progress",
        "Status": agg_status,
        "Confidence Index 1 (Percent)": round(pct_optimistic, 1),
        "Confidence Index 2 (Count)": total_reforms,
        "Notes": f"Aggregate summary. Thresholds: Optimistic ≥60%, Baseline 30–59%, Pessimistic <30%. Based on {total_reforms} reforms."
    })

    # === Export ===
    out_df = pd.DataFrame(records)
    out_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Policy reform assumption saved → {OUTPUT_FILE} ({len(out_df)} rows)")


if __name__ == "__main__":
    main()
