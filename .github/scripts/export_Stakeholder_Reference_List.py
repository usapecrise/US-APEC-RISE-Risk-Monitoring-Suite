#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Export Stakeholder Reference List (wide + long) for Tableau.

- Resolves linked fields in the main table:
    * "Economy Reference List" -> names from "Economy Reference List" (display: "Economy")
    * "Workstream"             -> names from "Workstream Reference List" (display: "Workstream")
    * "Engagement"             -> names from "Workshop Reference List" (display: "Workshop")
    * "Engagement ID"          -> linked to "Workshop Reference List" (display: "Engagement ID")

- Outputs:
    Stakeholder_Reference_List.csv          (wide, human-friendly)
    Stakeholder_Reference_List_long.csv     (normalized: Workstream × Economy × Workshop × Engagement ID)
"""

import os, sys, csv, time, requests
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

# Linked fields: Stakeholder field name -> {linked table + display field}
LINKED_CONFIG = {
    "Economy Reference List": {"table": "Economy Reference List", "display": "Economy"},
    "Workstream":             {"table": "Workstream Reference List", "display": "Workstream"},
    "Engagement":             {"table": "Workshop Reference List",  "display": "Workshop"},       # 👈 fixed
    "Engagement ID":          {"table": "Workshop Reference List",  "display": "Engagement ID"},
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
        if "offset" not in data:
            break
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
    linked_id_maps[field] = {}
    for r in recs:
        fields = r.get("fields", {})
        # Handle multi-select (list of strings) properly
        val = fields.get(cfg["display"], "Unknown")
        if isinstance(val, list):
            val = "|".join(val)
        linked_id_maps[field][r["id"]] = val

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

    # Resolve linked tables into pipe-joined name lists
    for main_field, cfg in LINKED_CONFIG.items():
        ids = ensure_list(fields.get(main_field))
        names = [linked_id_maps[main_field].get(i, "Unknown") for i in ids]
        fields[f"{cfg['display']}_List"] = join_pipe(names)

    wide_rows.append(fields)

    # Split lists for normalization (long format)
    ws_list  = [s.strip() for s in fields.get("Workstream_List", "").split("|") if s.strip()] or [""]
    ec_list  = [s.strip() for s in fields.get("Economy_List", "").split("|") if s.strip()] or [""]
    wk_list  = [s.strip() for s in fields.get("Workshop_List", "").split("|") if s.strip()] or [""]   # 👈 fixed to Workshop_List
    eid_list = [s.strip() for s in fields.get("Engagement ID_List", "").split("|") if s.strip()] or [""]

    for ws, ec, wk, eid in product(ws_list, ec_list, wk_list, eid_list):
        row = dict(fields)
        row["Workstream_Single"]    = ws
        row["Economy_Single"]       = ec
        row["Workshop_Single"]      = wk
        row["Engagement_ID_Single"] = eid
        long_rows.append(row)

# ==============================
# Write outputs
# ==============================
if wide_rows:
    with open(WIDE_OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=wide_rows[0].keys())
        writer.writeheader()
        writer.writerows(wide_rows)
    print(f"✅ Export complete: {WIDE_OUT}")

if long_rows:
    with open(LONG_OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=long_rows[0].keys())
        writer.writeheader()
        writer.writerows(long_rows)
    print(f"✅ Export complete: {LONG_OUT}")
