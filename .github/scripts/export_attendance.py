import os
import requests
import pandas as pd

# Airtable Config
AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
BASE_ID = "app0Ljjhrp3lTTpTO"

TABLES = {
    "OT1 Sign-Ins (Workshops)": "tblIpPKx5wzr42YZX",
    "Other Sign-Ins (Meetings/Dialogues)": "tbl6qMYkcIzkl8q7D"
}
VIEW_ID = None

def fetch_table(table_label, table_id):
    url = f"https://api.airtable.com/v0/{BASE_ID}/{table_id}"
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    records, offset = [], None

    while True:
        params = {"view": VIEW_ID} if VIEW_ID else {}
        if offset:
            params["offset"] = offset
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            raise RuntimeError(f"Airtable API error {resp.status_code}: {resp.text}")
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

# === Main ===
dfs = [fetch_table(label, tid) for label, tid in TABLES.items()]
df = pd.concat(dfs, ignore_index=True)

df["Economy"] = df["Economy"].apply(lambda x: "; ".join(x) if isinstance(x, list) else str(x))
df["Workstream"] = df["Workstream"].apply(lambda x: "; ".join(x) if isinstance(x, list) else str(x))
df["Workshop Key"] = df["Workshop"].astype(str) + " | " + df["Workshop Date"].astype(str)
df["Workshop Date"] = pd.to_datetime(df["Workshop Date"], errors="coerce")

rows = []

# --- Aggregate level ---
workshop_stats = (
    df.groupby("Workshop Key")
    .agg({"Economy": "nunique", "Workshop Date": "first"})
    .reset_index()
    .sort_values("Workshop Date", ascending=False)
)
last3 = workshop_stats.head(3)
economies_present = last3["Economy"].mean()
pct = (economies_present / 21) * 100
status = "optimistic" if pct >= 60 else "baseline" if pct >= 30 else "pessimistic"

rows.append({
    "Assumption": "Stakeholder alignment with U.S. focus areas",
    "Monitoring Tool": "Attendance",
    "Economy": "APEC (aggregate)",
    "Workstream": "All",
    "Level": "Aggregate",
    "Date": last3["Workshop Date"].max().strftime("%Y-%m-%d"),
    "Signal": f"Average {economies_present:.1f} economies represented (last 3 dialogues)",
    "Status": status,
    "Confidence Index 1 (Percent)": round(pct, 1),
    "Confidence Index 2 (Breadth)": int(round(economies_present, 0)),
    "Notes": "Thresholds: Optimistic ≥60%, Baseline 30–59%, Pessimistic <30% of APEC economies."
})

# --- Workstream level ---
for ws, g in df.groupby("Workstream"):
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
    status_ws = "optimistic" if pct_ws >= 60 else "baseline" if pct_ws >= 30 else "pessimistic"

    rows.append({
        "Assumption": "Stakeholder alignment with U.S. focus areas",
        "Monitoring Tool": "Attendance",
        "Economy": "APEC (aggregate)",
        "Workstream": ws,
        "Level": "Workstream",
        "Date": last3_ws["Workshop Date"].max().strftime("%Y-%m-%d"),
        "Signal": f"Average {economies_present_ws:.1f} economies represented (last 3 {ws} dialogues)",
        "Status": status_ws,
        "Confidence Index 1 (Percent)": round(pct_ws, 1),
        "Confidence Index 2 (Breadth)": int(round(economies_present_ws, 0)),
        "Notes": "Thresholds: Optimistic ≥60%, Baseline 30–59%, Pessimistic <30% of APEC economies."
    })

    # --- Economy level ---
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
        status_econ = "optimistic" if pct_attended >= 67 else "baseline" if pct_attended == 33 else "pessimistic"

        rows.append({
            "Assumption": "Stakeholder alignment with U.S. focus areas",
            "Monitoring Tool": "Attendance",
            "Economy": econ,
            "Workstream": ws,
            "Level": "Economy",
            "Date": last3_econ_ws["Workshop Date"].max().strftime("%Y-%m-%d"),
            "Signal": f"{econ} attended {attended_count}/3 {ws} dialogues",
            "Status": status_econ,
            "Confidence Index 1 (Percent)": round(pct_attended, 1),
            "Confidence Index 2 (Count)": attended_count,
            "Notes": "Thresholds: Optimistic ≥67% (2–3/3), Baseline 33% (1/3), Pessimistic 0%."
        })

attendance_status = pd.DataFrame(rows)
attendance_status.to_csv("attendance_assumption.csv", index=False)
print(f"✅ Attendance assumption saved → attendance_assumption.csv ({len(rows)} rows))")

