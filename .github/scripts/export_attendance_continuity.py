import os
import requests
import pandas as pd

# Airtable Config
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
BASE_ID = "app0Ljjhrp3lTTpTO"
TABLES = {
    "Other Sign-Ins (Meetings/Dialogues)": "tbl6qMYkcIzkl8q7D"
}
VIEW_ID = None  # optional

def fetch_table(table_label, table_id):
    """Fetch all records from an Airtable table by ID."""
    url = f"https://api.airtable.com/v0/{BASE_ID}/{table_id}"
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    records, offset = [], None

    while True:
        params, offset = ({"view": VIEW_ID} if VIEW_ID else {}), offset
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
                "Economy": f.get("Economy", ""),
                "Organization": f.get("Organization", ""),
                "Source Table": table_label
            })

        offset = data.get("offset")
        if not offset:
            break

    return pd.DataFrame(records)


def classify_economy(attended: int):
    """Classify economy continuity status based on attendance out of 3."""
    if attended == 3:
        return "optimistic", 100
    elif attended == 2:
        return "baseline", 67
    elif attended == 1:
        return "baseline", 33
    else:
        return "pessimistic", 0


def classify_percentage(avg_attended: float):
    """Classify aggregate % thresholds."""
    pct = (avg_attended / 3) * 100
    if pct >= 60:
        return "optimistic", pct
    elif pct >= 30:
        return "baseline", pct
    else:
        return "pessimistic", pct


def main():
    # === 1. Fetch data ===
    dfs = [fetch_table(label, tid) for label, tid in TABLES.items()]
    df = pd.concat(dfs, ignore_index=True)

    if df.empty:
        print("⚠️ No attendance data found, writing empty continuity file")
        pd.DataFrame(columns=[
            "Assumption","Monitoring Tool","Economy","Workstream","Level","Date","Signal","Status","Confidence Index","Notes"
        ]).to_csv("attendance_continuity_assumption.csv", index=False)
        return

    # Normalize
    df["Workshop Date"] = pd.to_datetime(df["Workshop Date"], errors="coerce")
    df["Economy"] = df["Economy"].apply(lambda x: "; ".join(x) if isinstance(x, list) else str(x))
    df["Workshop Key"] = df["Workshop"].astype(str) + " | " + df["Workshop Date"].astype(str)

    # === 2. Identify last 3 events ===
    event_order = (
        df.groupby("Workshop Key")["Workshop Date"].min()
        .sort_values(ascending=False)
        .index
    )
    last3_events = list(event_order[:3])
    last3_df = df[df["Workshop Key"].isin(last3_events)]

    # === 3. Economy-level responsiveness ===
    economy_stats = (
        last3_df.groupby("Economy")["Workshop Key"].nunique()
        .reset_index(name="Events_Attended")
    )

    records = []
    for _, row in economy_stats.iterrows():
        economy = row["Economy"]
        attended = row["Events_Attended"]
        status, confidence = classify_economy(attended)

        records.append({
            "Assumption": "Political and institutional continuity",
            "Monitoring Tool": "Attendance",
            "Economy": economy,
            "Workstream": "All",
            "Level": "Economy",
            "Date": last3_df["Workshop Date"].max().strftime("%Y-%m-%d"),
            "Signal": f"{economy} attended {attended}/3 most recent events",
            "Status": status,
            "Confidence Index": confidence,
            "Notes": "Thresholds: Optimistic ≥67% (2–3/3 attended), Baseline 33% (1/3), Pessimistic 0%."
        })

    # === 4. APEC aggregate continuity ===
    avg_attended = economy_stats["Events_Attended"].mean() if not economy_stats.empty else 0
    agg_status, agg_confidence = classify_percentage(avg_attended)

    records.append({
        "Assumption": "Political and institutional continuity",
        "Monitoring Tool": "Attendance",
        "Economy": "APEC (aggregate)",
        "Workstream": "All",
        "Level": "Aggregate",
        "Date": last3_df["Workshop Date"].max().strftime("%Y-%m-%d"),
        "Signal": f"On average, economies attended {avg_attended:.1f}/3 recent events",
        "Status": agg_status,
        "Confidence Index": round(agg_confidence, 1),
        "Notes": "Thresholds: Optimistic ≥60%, Baseline 30–59%, Pessimistic <30%."
    })

    # === 5. Export ===
    out_df = pd.DataFrame(records)
    out_df.to_csv("attendance_continuity_assumption.csv", index=False)
    print(f"✅ Continuity assumption saved → attendance_continuity_assumption.csv ({len(out_df)} rows)")


if __name__ == "__main__":
    main()

