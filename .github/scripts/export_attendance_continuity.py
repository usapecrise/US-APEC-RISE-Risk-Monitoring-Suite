import os
import requests
import pandas as pd

# Airtable Config
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
BASE_ID = "app0Ljjhrp3lTTpTO"
TABLES = "OT1 Sign-Ins (Workshops)": "tblIpPKx5wzr42YZX", "Other Sign-Ins (Meetings/Dialogues)": "tbl6qMYkcIzkl8q7D"s
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
                "Workshop Title": f.get("Workshop", ""),
                "Date": f.get("Workshop Date", ""),
                "Economy": f.get("Economy", ""),
                "Participant Name": f.get("Participant Name", ""),
                "Organization": f.get("Organization", ""),
                "Source Table": table_name
            })

        offset = data.get("offset")
        if not offset:
            break

    return pd.DataFrame(records)

def main():
    # === 1. Fetch attendance data ===
    dfs = [fetch_table(tbl) for tbl in TABLES]
    df = pd.concat(dfs, ignore_index=True)

    if df.empty:
        print("⚠️ No attendance data found, skipping continuity assumption")
        return

    # Normalize dates
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    # === 2. Check responsiveness by economy ===
    # Count how many distinct events each economy joined in the last 3
    event_order = df.groupby("Workshop Title")["Date"].min().sort_values(ascending=False).index
    last3_events = list(event_order[:3])
    last3_df = df[df["Workshop Title"].isin(last3_events)]

    economy_stats = (
        last3_df.groupby("Economy")["Workshop Title"].nunique()
        .reset_index(name="Events_Attended")
    )

    records = []
    for _, row in economy_stats.iterrows():
        economy = row["Economy"]
        attended = row["Events_Attended"]

        if attended == 3:
            status = "optimistic"
            signal = "Consistently attended last 3 events"
        elif attended == 2:
            status = "baseline"
            signal = "Attended 2 of last 3 events"
        else:
            status = "pessimistic"
            signal = "Missed 2+ recent events"

        records.append({
            "assumption": "Political and institutional continuity",
            "monitoring_tool": "attendance",
            "economy": economy,
            "date": last3_df["Date"].max().strftime("%Y-%m-%d"),
            "signal": signal,
            "status": status,
            "notes": f"{economy} attended {attended}/3 most recent APEC events"
        })

    out_df = pd.DataFrame(records)
    out_df.to_csv("attendance_continuity_assumption.csv", index=False)
    print(f"✅ Continuity assumption saved → attendance_continuity_assumption.csv ({len(out_df)} rows)")

if __name__ == "__main__":
    main()
