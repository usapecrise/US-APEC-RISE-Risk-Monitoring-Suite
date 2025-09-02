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
                "Workshop": f.get("Workshop", ""),
                "Workshop Date": f.get("Workshop Date", ""),
                "Workstream": f.get("Workstream", ""),
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

# Debug print
print("DEBUG columns:", df.columns.tolist())

if "Workshop" not in df.columns or "Workshop Date" not in df.columns:
    raise KeyError(f"Expected Workshop/Workshop Date fields not found. Got: {df.columns.tolist()}")

# Create Workshop Key
df["Workshop Key"] = df["Workshop"].astype(str) + " | " + df["Workshop Date"].astype(str)

# Save raw attendance with Workstream included
df.to_csv("attendance_records.csv", index=False)
print(f"✅ Exported {len(df)} rows from {len(TABLES)} tables → attendance_records.csv")

# === 2. Generate Assumption Evidence (Stakeholder Alignment) ===
if not df.empty:
    df["Workshop Date"] = pd.to_datetime(df["Workshop Date"], errors="coerce")
    rows = []

    # --- APEC-wide aggregate ---
    workshop_stats = (
        df.groupby("Workshop Key")
        .agg({"Economy": "nunique", "Workshop Date": "first"})
        .reset_index()
        .sort_values("Workshop Date", ascending=False)
    )

    last3 = workshop_stats.head(3)
    economies_present = last3["Economy"].mean()
    pct = (economies_present / 21) * 100
    scenario = (
        "optimistic" if pct >= 75 else
        "baseline" if pct >= 40 else
        "pessimistic"
    )

    rows.append({
        "assumption": "Stakeholder alignment with U.S. focus areas",
        "monitoring_tool": "attendance",
        "economy": "APEC",
        "workstream": "All",
        "level": "aggregate",
        "date": last3["Workshop Date"].max().strftime("%Y-%m-%d"),
        "signal": f"Average {economies_present:.1f} economies represented (last 3 dialogues)",
        "status": scenario,
        "notes": f"≈{pct:.0f}% of APEC economies participated"
    })

    # --- Workstream breakdown ---
    for ws, g in df.groupby("Workstream"):
        if g.empty:
            continue

        ws_stats = (
            g.groupby("Workshop Key")
            .agg({"Economy": "nunique", "Workshop Date": "first"})
            .reset_index()
            .sort_values("Workshop Date", ascending=False)
        )

        last3_ws = ws_stats.head(3)
        if last3_ws.empty:
            continue

        economies_present_ws = last3_ws["Economy"].mean()
        pct_ws = (economies_present_ws / 21) * 100
        scenario_ws = (
            "optimistic" if pct_ws >= 75 else
            "baseline" if pct_ws >= 40 else
            "pessimistic"
        )

        rows.append({
            "assumption": "Stakeholder alignment with U.S. focus areas",
            "monitoring_tool": "attendance",
            "economy": "APEC",
            "workstream": ws,
            "level": "workstream",
            "date": last3_ws["Workshop Date"].max().strftime("%Y-%m-%d"),
            "signal": f"Average {economies_present_ws:.1f} economies represented (last 3 {ws} dialogues)",
            "status": scenario_ws,
            "notes": f"≈{pct_ws:.0f}% of APEC economies participated"
        })

        # --- Economy-level breakdown within each workstream ---
        for econ, ge in g.groupby("Economy"):
            econ_ws_stats = (
                ge.groupby("Workshop Key")
                .agg({"Economy": "count", "Workshop Date": "first"})
                .reset_index()
                .sort_values("Workshop Date", ascending=False)
            )

            last3_econ_ws = econ_ws_stats.head(3)
            if last3_econ_ws.empty:
                continue

            attended_count = (last3_econ_ws["Economy"] > 0).sum()
            pct_attended = (attended_count / 3) * 100
            scenario_econ = (
                "optimistic" if pct_attended == 100 else
                "baseline" if pct_attended >= 50 else
                "pessimistic"
            )

            rows.append({
                "assumption": "Stakeholder alignment with U.S. focus areas",
                "monitoring_tool": "attendance",
                "economy": econ,
                "workstream": ws,
                "level": "economy",
                "date": last3_econ_ws["Workshop Date"].max().strftime("%Y-%m-%d"),
                "signal": f"{econ} attended {attended_count} of last 3 {ws} dialogues",
                "status": scenario_econ,
                "notes": f"{pct_attended:.0f}% attendance rate"
            })

    # Export all levels
    attendance_status = pd.DataFrame(rows)
    attendance_status.to_csv("attendance_assumption.csv", index=False)
    print(f"✅ Assumption status saved → attendance_assumption.csv ({len(rows)} rows)")
else:
    print("⚠️ No attendance data found, skipping assumption status")
