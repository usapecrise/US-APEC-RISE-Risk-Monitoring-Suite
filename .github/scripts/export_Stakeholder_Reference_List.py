import os, sys, csv, json, time, requests
from urllib.parse import quote
from datetime import datetime
from itertools import product

# ==============================
# Config
# ==============================
AIRTABLE_TOKEN = os.environ.get("AIRTABLE_TOKEN")
if not AIRTABLE_TOKEN:
    print("❌ AIRTABLE_TOKEN is not set.")
    sys.exit(1)

BASE_ID     = "app0Ljjhrp3lTTpTO"
MAIN_TABLE  = "Stakeholder Reference List"
VIEW_NAME   = "Grid view"

LINKED_CONFIG = {
    "Economy Reference List": {"table": "Economy Reference List", "display": "Economy"},
    "Workstream":             {"table": "Workstream Reference List", "display": "Workstream"},
    "Engagements":            {"table": "Workshop Reference List",              "display": "Workshop Title"},
}

WIDE_OUT = "Stakeholder_Reference_List.csv"
LONG_OUT = "Stakeholder_Reference_List_long.csv"

HEADERS = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}

# ==============================
# Helpers
# ==============================
def fetch_all_records(table, view=None):
    url = f"https://api.airtable.com/v0/{BASE_ID}/{quote(table)}"
    params, out = ({"view": view} if view else {}), []
    while True:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
        data = resp.json()
        out.extend(data.get("records", []))
        if "offset" not in data: break
        params["offset"] = data["offset"]
        time.sleep(0.1)
    print(f"✅ Fetched {len(out)} from '{table}'")
    return out

def ensure_list(v):
    if not v: return []
    return v if isinstance(v, list) else [v]

def join_pipe(vals):
    return "|".join(str(x).strip() for x in vals if str(x).strip())

# ==============================
# Build lookup maps for linked tables
# ==============================
linked_id_maps = {}
for field, cfg in LINKED_CONFIG.items():
    recs = fetch_all_records(cfg["table"])
    linked_id_maps[field] = {
        r["id"]: r.get("fields", {}).get(cfg["display"], "Unknown") for r in recs
    }

# ==============================
# Fetch main table
# ==============================
main_records = fetch_all_records(MAIN_TABLE, view=VIEW_NAME)
timestamp = datetime.utcnow().isoformat()
wide_rows, long_rows = [], []

# ==============================
# Transform
# ==============================
for rec in main_records:
    fields = dict(rec.get("fields", {}))
    fields["Last Updated"] = timestamp

    # Resolve linked lists
    for main_field, idmap in linked_id_maps.items():
        ids = ensure_list(fields.get(main_field))
        names = [idmap.get(i, "Unknown") for i in ids]
        fields[f"{main_field}_List"] = join_pipe(names)

    wide_rows.append(fields)

    ws_list = ensure_list(fields.get("Workstream_List", "").split("|")) if fields.get("Workstream_List") else [""]
    ec_list = ensure_list(fields.get("Economy Reference List_List", "").split("|")) if fields.get("Economy Reference List_List") else [""]
    en_list = ensure_list(fields.get("Engagements_List", "").split("|")) if fields.get("Engagements_List") else [""]

    for ws, ec, en in product(ws_list, ec_list, en_list):
        row = dict(fields)
        row["Workstream_Single"] = ws
        row["Economy_Single"]    = ec
        row["Engagement_Single"] = en
        long_rows.append(row)

# ==============================
# Write outputs
# ==============================
with open(WIDE_OUT, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=wide_rows[0].keys())
    writer.writeheader()
    writer.writerows(wide_rows)

with open(LONG_OUT, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=long_rows[0].keys())
    writer.writeheader()
    writer.writerows(long_rows)

print(f"✅ Export complete: {WIDE_OUT}")
print(f"✅ Export complete: {LONG_OUT}")
