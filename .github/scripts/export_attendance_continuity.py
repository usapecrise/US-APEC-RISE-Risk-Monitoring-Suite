import os 
import requests
import pandas as pd

# Airtable Config
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
BASE_ID = "app0Ljjhrp3lTTpTO"
TABLES = {
    "Other Sign-Ins (Meetings/Dialogues)": "tbl6qMYkcIzkl8q7D"
}
VIEW_ID = None  # or replace with your "viw..." if you want to filter by view


def fetch_table(table_label, table_id):
    """Fetch all records from an Airtable table by ID."""
    url = f"https://api.airtable.com/v0/{BASE_ID}/{table_id}"
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    records, offset = [], None

    while True:
        params = {}
        if VIEW_ID:
            params["view"] = VIEW_ID
        if offset:
            params["offset"] = offset

        resp = requests.get(url, headers=headers, params=params)
        print("DEBUG response for", table_label, ":", resp.status_code, resp.text[:200])

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


def classify_economy(attended):
    """Classify economy continuity status based on attendance out of 3."""
    if attended == 3:
        return "optimistic", "Consistently attended last 3 events"
    elif attended == 2:
        return "baseline", "Attended 2 of last 3 events"
    else:
        return "pessimistic", "Missed 2+ recent events"


def main():
    # === 1. Fetch data ===
    dfs = [fetch_table(label, tid) for label, tid in TABLES.items()]
    df = pd.concat(dfs, ignore_index=True)

    if df.empty:
        print("⚠️ No attendance data found, writing empty continuity file")
        pd.DataFrame(columns=[
            "Assumption","Monitoring Tool","Economy","Date","Signal","Status","Notes"
        ]).to_csv("attendance_continuity_assumption.csv", index=False)
        return

    # Normalize dates and economy field
    df["Workshop Date"] = pd.to_datetime(df["Workshop Date"], errors="coerce")
    df["Economy"] = df["Economy"].apply(
        lambda x: "; ".join(x) if isinstance(x, list) else str(x)
    )

    # Create Workshop Key (Workshop + Date)
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
        status, signal = classify_economy(attended)

        records.append({
            "Assumption": "Political and institutional continuity",
            "Monitoring Tool": "Attendance",
            "Economy": economy,
            "Date": last3_df["Workshop Date"].max().strftime("%Y-%m-%d"),
            "Signal": signal,
            "Status": status,
            "Notes": f"{economy} attended {attended}/3 most recent APEC events"
        })

    # === 4. APEC aggregate continuity ===
    avg_attended = economy_stats["Events_Attended"].mean() if not economy_stats.empty else 0
    if avg_attended >= 1.8:
        agg_status = "optimistic"
    elif avg_attended >= 0.9:
        agg_status = "baseline"
    else:
        agg_status = "pessimistic"

    records.append({
        "Assumption": "Political and institutional continuity",
        "Monitoring Tool": "Attendance",
        "Economy": "APEC (aggregate)",
        "Date": last3_df["Workshop Date"].max().strftime("%Y-%m-%d"),
        "Signal": f"On average, economies attended {avg_attended:.1f}/3 recent events",
        "Status": agg_status,
        "Notes": "Thresholds: ≥1.8 optimistic (≥60%), 0.9–1.7 baseline (30–59%), <0.9 pessimistic (<30%)"
    })

    # === 5. Export ===
    out_df = pd.DataFrame(records)
    out_df.to_csv("attendance_continuity_assumption.csv", index=False)
    print(f"✅ Continuity assumption saved → attendance_continuity_assumption.csv ({len(out_df)} rows)")


if __name__ == "__main__":
    main()

