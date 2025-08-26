#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Export Stakeholder Reference List (wide + long) for Tableau.

- Resolves linked fields in the main table:
    * "Economy Reference List"  -> names from "Economy Reference List" (display: "Economy")
    * "Workstream"              -> names from "Workstream Reference List" (display: "Workstream")
    * "Engagements"             -> ALSO resolved from "Workstream Reference List"
      using a readable column (default "Engagement Title", fallback to "Title"/"Name"/"Workstream")

- Outputs:
    Stakeholder_Reference_List.csv          (wide, human-friendly)
    Stakeholder_Reference_List_long.csv     (normalized Workstream × Economy × Engagement)

Notes:
- No economy-name standardization (per your request).
- Engagements is treated as a linked-record multi-select (recIDs) OR multi-select text.
"""

import os
import sys
import csv
import json
import time
import requests
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

# Main-table linked fields (keys MUST match main-table field names)
LINKED_CONFIG = {
    "Economy Reference List": {"table": "Economy Reference List", "display": "Economy"},
    "Workstream":             {"table": "Workstream Reference List", "display": "Workstream"},
}

# Engagements field in the main table (exact label)
ENGAGEMENT_FIELD_NAME = "Engagements"

# Engagement titles live in the Workstream Reference List table:
ENGAGEMENT_RESOLVE_TABLE   = "Workstream Reference List"
ENGAGEMENT_DISPLAY_CANDIDATES = ["Engagement Title", "Title", "Name", "Workstream"]  # first present wins

# Output files (repo root; change to "data/..." if you prefer)
WIDE_OUT = "Stakeholder_Reference_List.csv"
LONG_OUT = "Stakeholder_Reference_List_long.csv"

HEADERS = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}


# ==============================
# Helpers
# ==============================
def fetch_all_records(table, view=None, strict=True):
    url = f"https://api.airtable.com/v0/{BASE_ID}/{quote(table)}"
    params = {}
    if view:
        params["view"] = view
    out = []
    while True:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
        try:
            data = resp.json()
        except json.JSONDecodeError:
            msg = f"❌ Non-JSON response from '{table}': {resp.text[:200]}"
            print(msg)
            if strict:
                sys.exit(1)
            return []
        if "records" not in data:
            msg = f"❌ Error fetching '{table}': {data}"
            print(msg)
            if strict:
                sys.exit(1)
            return []
        out.extend(data["records"])
        off = data.get("offset")
        if not off:
            break
        params["offset"] = off
        time.sleep(0.1)
    print(f"✅ Fetched {len(out)} from '{table}'")
    return out


def ensure_list(v):
    if v is None:
        return []
    return v if isinstance(v, list) else [v]


def pipe_join(vals):
    vals = [str(x).strip() for x in vals if str(x).strip()]
    return "|".join(vals) if vals else ""


def looks_like_ids(vals):
    return bool(vals) and all(isinstance(x, str) and x.startswith("rec") for x in vals)


def pick(fields_dict, candidates):
    for c in candidates:
        if c in fields_dict and str(fields_dict[c]).strip():
            return str(fields_dict[c]).strip()
    return None


# ==============================
# Fetch main table
# ==============================
main_records = fetch_all_records(MAIN_TABLE, view=VIEW_NAME, strict=True)
print(f"🔍 Retrieved {len(main_records)} from '{MAIN_TABLE}'")

# ==============================
# Build maps for linked tables
# ==============================
linked_id_maps = {}
workstream_records_by_id = {}

for main_field, cfg in LINKED_CONFIG.items():
    table = cfg["table"]
    display = cfg["display"]
    recs = fetch_all_records(table, strict=True)
    if table == "Workstream Reference List":
        workstream_records_by_id = {r["id"]: r for r in recs}
    linked_id_maps[main_field] = {
        r["id"]: r.get("fields", {}).get(display, "Unknown") for r in recs
    }

# Engagement ID -> Title mapping from the Workstream Reference List
engagement_id_to_title = {}
for rid, r in workstream_records_by_id.items():
    title = pick(r.get("fields", {}), ENGAGEMENT_DISPLAY_CANDIDATES)
    if title:
        engagement_id_to_title[rid] = title

timestamp = datetime.utcnow().isoformat()
wide_rows, long_rows = [], []


# ==============================
# Transform
# ==============================
for rec in main_records:
    fields = dict(rec.get("fields", {}))  # shallow copy

    # Resolve Economy & Workstream into names and pipe-lists
    for main_field, cfg in LINKED_CONFIG.items():
        ids = ensure_list(fields.get(main_field))
        names = [linked_id_maps[main_field].get(_id, "Unknown") for _id in ids]
        disp = cfg["display"]
        fields[f"{disp} (Name)"] = ", ".join(names) if names else ""
        fields[f"{disp}_List"]  = pipe_join(names)

    # Resolve Engagements
    engagements_resolved = []
    raw_eng = ensure_list(fields.get(ENGAGEMENT_FIELD_NAME))
    if looks_like_ids(raw_eng):
        engagements_resolved = [engagement_id_to_title.get(x, x) for x in raw_eng]
        if raw_eng:
            fields["Engagement_IDs"] = pipe_join(raw_eng)  # keep raw IDs for debug
    else:
        # Multi-select strings (dropdown) or plain text
        engagements_resolved = [str(x).strip() for x in raw_eng if str(x).strip()]

    fields["Engagement_List"]  = pipe_join(engagements_resolved)
    fields["Engagement_Count"] = len(engagements_resolved)

    # Timestamp to force diffs
    fields["Last Updated"] = timestamp

    wide_rows.append(fields)

    # Normalize: Workstream × Economy × Engagement
    ws_list = [s for s in fields.get("Workstream_List", "").split("|") if s] or [""]
    ec_list = [s for s in fields.get("Economy_List", "").split("|") if s] or [""]
    en_list = [s for s in fields.get("Engagement_List", "").split("|") if s] or [""]

    for ws, ec, en in product(ws_list, ec_list, en_list):
        row = dict(fields)
        row["Workstream_Single"] = ws
        row["Economy_Single"]    = ec
        row["Engagement_Single"] = en
        # Overwrite single-name fields for clarity in long file
        row["Workstream (Name)"] = ws
        row["Economy (Name)"]    = ec
        long_rows.append(row)


# ==============================
# Write WIDE CSV
# ==============================
if wide_rows:
    preferred = [
        "Title", "Organization Type", "Fiscal Year",
        "Workstream (Name)", "Economy (Name)",
        "Workstream_List", "Economy_List",
        "Engagement_List", "Engagement_Count", "Engagement_IDs",
        "Last Updated",
    ]
    other = sorted({k for r in wide_rows for k in r.keys()} - set(preferred))
    fieldnames = preferred + other
    with open(WIDE_OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for r in wide_rows:
            w.writerow(r)
print(f"✅ Export complete: {WIDE_OUT}")

# ==============================
# Write LONG CSV
# ==============================
if long_rows:
    preferred_long = [
        "Title", "Organization Type", "Fiscal Year",
        "Workstream_Single", "Economy_Single", "Engagement_Single",
        "Workstream (Name)", "Economy (Name)",
        "Workstream_List", "Economy_List", "Engagement_List",
        "Engagement_Count", "Engagement_IDs",
        "Last Updated",
    ]
    other_long = sorted({k for r in long_rows for k in r.keys()} - set(preferred_long))
    fieldnames_long = preferred_long + other_long
    with open(LONG_OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames_long, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for r in long_rows:
            w.writerow(r)
print(f"✅ Export complete: {LONG_OUT}")
