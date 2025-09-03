import os
import requests
import pandas as pd
import urllib.parse

# Airtable Config
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
BASE_ID = "app0Ljjhrp3lTTpTO"

# ✅ Use table IDs
TABLES = {
    "OT1 Sign-Ins (Workshops)": "tblIpPKx5wzr42YZX",
    "Other Sign-Ins (Meetings/Dialogues)": "tbl6qMYkcIzkl8q7D"
}
VIEW_ID = None   # optional – use view filter if needed

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
                "Workstream": f.get("Workstream", ""),
                "Economy": f.get("Economy", f.get("Economy or Guest", "")),
                "Organization": f.get("Organization", ""),
                "Source Table": table_label
            })

        offset = data.get("offset")
        if not offset:
            break

    return pd.DataFrame(records)

# === 1. Export Raw Attendance ===
dfs = [fetch_table(label, tid) for label, tid in TABLES.items()]
df = pd.concat(dfs, ignore_index=True)

print("DEBUG columns:", df.columns.tolist())

# ✅ Normalize fields: flatten lists → strings
df["Economy"] = df["Economy"].apply(
    lambda x: "; ".join(x) if isinstance(x, list) else str(x)
)
df["Workstream"] = df["Workstream"].apply(
    lambda x: "; ".join(x) if isinstance(x, list) else str(x)
)

if "Workshop" not in df.columns or "Workshop Date" not in df.columns:
    raise KeyError(f"Expected Workshop/Workshop Date fields not found. Got: {df.columns.tolist()}")

# Create Workshop Key
df["Workshop Key"] = df["Workshop"].astype(str) + " | " + df["Workshop Date"].astype(str)

# Save raw attendance
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
        "optimistic" if pct >= 60 else
        "baseline" if pct >= 30 else
        "pessimistic"
    )

    rows.append({
        "Assumption": "Stakeholder alignment with U.S. focus areas",
        "Monitoring_tool": "attendance",
        "Economy": "APEC (aggregate)",
        "Workstream": "All",
        "Level": "aggregate",
        "Date": last3["Workshop Date"].max().strftime("%Y-%m-%d"),
        "Signal": f"Average {economies_present:.1f} economies represented (last 3 dialogues)",
        "Status": scenario,
        "Notes": f"≈{pct:.0f}% of APEC economies participated"
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
            "optimistic" if pct_ws >= 67 else
            "baseline" if pct_ws >= 33 else
            "pessimistic"
        )

        rows.append({
            "Assumption": "Stakeholder alignment with U.S. focus areas",
            "Monitoring_tool": "attendance",
            "Economy": "APEC (aggregate)",
            "Workstream": ws,
            "Level": "workstream",
            "Date": last3_ws["Workshop Date"].max().strftime("%Y-%m-%d"),
            "Signal": f"Average {economies_present_ws:.1f} economies represented (last 3 {ws} dialogues)",
            "Status": scenario_ws,
            "Notes": f"≈{pct_ws:.0f}% of APEC economies participated"
        })

        # --- Economy-level breakdown ---
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
                "Assumption": "Stakeholder alignment with U.S. focus areas",
                "Monitoring_tool": "attendance",
                "Economy": econ,
                "Workstream": ws,
                "Level": "economy",
                "Date": last3_econ_ws["Workshop Date"].max().strftime("%Y-%m-%d"),
                "Signal": f"{econ} attended {attended_count} of last 3 {ws} dialogues",
                "Status": scenario_econ,
                "Notes": f"{pct_attended:.0f}% attendance rate"
            })

    attendance_status = pd.DataFrame(rows)
    attendance_status.to_csv("attendance_assumption.csv", index=False)
    print(f"✅ Assumption status saved → attendance_assumption.csv ({len(rows)} rows)")
else:
    print("⚠️ No attendance data found, skipping assumption status")
