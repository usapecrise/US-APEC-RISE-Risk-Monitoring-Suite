import os
import requests
import pandas as pd

# Airtable Config
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
BASE_ID = "YOUR_BASE_ID"
TABLES = ["OT1 Sign-Ins (Workshops)", "Other Sign-Ins (Meetings/Dialogues)"]  # replace with your table names
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
                "Workshop Title": f.get("Workshop Title", ""),
                "Date": f.get("Date", ""),
                "Workstream": f.get("Workstream", ""),  # ✅ NEW
                "Economy": f.get("Economy", ""),
                "Participant Name": f.get("Participant Name", ""),
                "Organization": f.get("Organization", ""),
                "Source Table": table_name
            })

        offset = data.get("offset")
        if not offset:
            break

    return pd.DataFrame(records)

# === 1. Export Raw Attendance ===
dfs = [fetch_table(tbl) for tbl in TABLES]
df = pd.concat(dfs, ignore_index=True)

# Create Workshop Key (title + date only)
df["Workshop Key"] = df["Workshop Title"].astype(str) + " | " + df["Date"].astype(str)

# Save raw attendance with Workstream included
df.to_csv("attendance_records.csv", index=False)
print(f"✅ Exported {len(df)} rows from {len(TABLES)} tables → attendance_records.csv")

# === 2. Generate Assumption Evidence (Stakeholder Alignment) ===
if not df.empty:
    # Normalize dates
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    # Group by workshop → count distinct economies
    workshop_stats = (
        df.groupby("Workshop Key")
        .agg({"Economy": "nunique", "Date": "first"})
        .reset_index()
        .sort_values("Date", ascending=False)
    )

    # Look at last 3 workshops
    last3 = workshop_stats.head(3)
    economies_present = last3["Economy"].mean()

    # Assume 21 APEC economies
    pct = (economies_present / 21) * 100

    if pct >= 75:
        scenario = "optimistic"
    elif pct >= 40:
        scenario = "baseline"
    else:
        scenario = "pessimistic"

    # Structured assumption schema
    attendance_status = pd.DataFrame([{
        "assumption": "Stakeholder alignment with U.S. focus areas",
        "monitoring_tool": "attendance",
        "economy": "APEC (aggregate)",
        "date": last3["Date"].max().strftime("%Y-%m-%d"),
        "signal": f"Average {economies_present:.1f} economies represented (last 3 dialogues)",
        "status": scenario,
        "notes": f"≈{pct:.0f}% of APEC economies participated"
    }])

    attendance_status.to_csv("attendance_assumption.csv", index=False)
    print(f"✅ Assumption status saved → attendance_assumption.csv")
else:
    print("⚠️ No attendance data found, skipping assumption status")
