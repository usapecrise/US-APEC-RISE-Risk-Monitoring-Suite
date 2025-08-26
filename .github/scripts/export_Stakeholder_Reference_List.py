#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Export Stakeholder Reference List (wide + long) for Tableau.

- Resolves linked fields:
    * "Economy Reference List"  -> Economy names (display col "Economy")
    * "Workstream"              -> Workstream names (display col "Workstream")

- Handles Engagement as a multi-select dropdown (strings). If you later
  convert to a linked table, it will try to resolve IDs as well (optional).

Outputs:
    Stakeholder_Reference_List.csv
    Stakeholder_Reference_List_long.csv
"""

import os
import csv
import sys
import json
import time
import requests
from urllib.parse import quote
from datetime import datetime
from itertools import product

# ------------------------------
# Config
# ------------------------------
AIRTABLE_TOKEN = os.environ.get("AIRTABLE_TOKEN")
if not AIRTABLE_TOKEN:
    print("❌ AIRTABLE_TOKEN is not set.")
    sys.exit(1)

BASE_ID = "app0Ljjhrp3lTTpTO"
MAIN_TABLE = "Stakeholder Reference List"
VIEW_NAME = "Grid view"

# Linked field configs (keys MUST match field names in the main table)
LINKED_CONFIG = {
    "Economy Reference List": {"table": "Economy Reference List", "display": "Economy"},
    "Workstream":             {"table": "Workstream Reference List", "display": "Workstream"},
}

# Engagement detection: first matching field name will be used
ENGAGEMENT_FIELD_CANDIDATES = ["Engagement Title", "Engagement", "Engagements"]

# Optional: if you later make Engagement a linked table, fill these in
OPTIONAL_ENGAGEMENT_LINKED = {
    "table": None,           # e.g., "Engagement Reference List"
    "display": None,         # e.g., "Engagement Title"
}

WIDE_OUT = "Stakeholder_Reference_List.csv"
LONG_OUT = "Stakeholder_Reference_List_long.csv"

HEADERS = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}

# ------------------------------
# Helpers
# ------------------------------
def fetch_all_records(table: str, view: str | None = None, strict: bool = True) -> list[dict]:
    url = f"https://api.airtable.com/v0/{BASE_ID}/{quote(table)}"
    params = {}
    if view:
        params["view"] = view

    all_records = []
    while True:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
        try:
            data = resp.json()
        except json.JSONDecodeError:
            msg = f"❌ Non-JSON response from Airtable for table '{table}': {resp.text[:200]}"
            if strict:
                print(msg); sys.exit(1)
            else:
                print(msg); return []

        if "records" not in data:
            msg = f"❌ Error fetching '{table}': {data}"
            if strict:
                print(msg); sys.exit(1)
            else:
                print(msg); return []

        all_records.extend(data["records"])
        offset = data.get("offset")
        if not offset:
            break
        params["offset"] = offset
        time.sleep(0.1)

    print(f"✅ Fetched {len(all_records)} records from '{table}'")
    return all_records

def ensure_list(v):
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]

def pipe_join(values):
    vals = [str(x).strip() for x in values if str(x).strip()]
    return "|".join(vals) if vals else ""

def looks_like_airtable_ids(values):
    """Heuristic: all items are strings starting with 'rec'."""
    if not values:
        return False
    return all(isinstance(x, str) and x.startswith("rec") for x in values)

# ------------------------------
# Fetch main table first (so we can detect Engagement field)
# ------------------------------
main_records = fetch_all_records(MAIN_TABLE, view=VIEW_NAME, strict=True)
print(f"🔍 Retrieved {len(main_records)} records from '{MAIN_TABLE}'")

# Detect engagement field name actually present
engagement_field_name = None
for rec in main_records:
    fields = rec.get("fields", {})
    for cand in ENGAGEMENT_FIELD_CANDIDATES:
        if cand in fields:
            engagement_field_name = cand
            break
    if engagement_field_name:
        break

if engagement_field_name:
    print(f"🔎 Using engagement field: '{engagement_field_name}'")
else:
    print("ℹ️ No engagement field found among candidates; proceeding without engagement values.")

# ------------------------------
# Build linked ID -> display maps (Economy, Workstream)
# ------------------------------
linked_id_maps: dict[str, dict] = {}
for main_field, cfg in LINKED_CONFIG.items():
    table_name = cfg["table"]
    display_col = cfg["display"]
    records = fetch_all_records(table_name, strict=True)
    linked_id_maps[main_field] = {
        rec["id"]: rec.get("fields", {}).get(display_col, "Unknown") for rec in records
    }

# Optional: build engagement linked map if configured
engagement_link_map = None
if OPTIONAL_ENGAGEMENT_LINKED["table"] and OPTIONAL_ENGAGEMENT_LINKED["display"]:
    t = OPTIONAL_ENGAGEMENT_LINKED["table"]
    d = OPTIONAL_ENGAGEMENT_LINKED["display"]
    try:
        records = fetch_all_records(t, strict=False)
        if records:
            engagement_link_map = {rec["id"]: rec.get("fields", {}).get(d, "Unknown") for rec in records}
            print(f"✅ Built engagement linked map from '{t}' ({len(engagement_link_map)} items)")
    except Exception as e:
        print(f"⚠️ Could not build engagement linked map: {e}")

timestamp = datetime.utcnow().isoformat()

wide_rows, long_rows = [], []

for rec in main_records:
    fields = dict(rec.get("fields", {}))  # shallow copy

    # Resolve Economy & Workstream linked lists into names
    for main_field, cfg in LINKED_CONFIG.items():
        ids = ensure_list(fields.get(main_field))
        names = [linked_id_maps[main_field].get(_id, "Unknown") for _id in ids]
        disp = cfg["display"]  # "Economy" or "Workstream"
        fields[f"{disp} (Name)"] = ", ".join(names) if names else ""
        fields[f"{disp}_List"] = pipe_join(names)

    # Resolve Engagement (multi-select strings or linked IDs or empty)
    engagements = []
    if engagement_field_name:
        raw_vals = ensure_list(fields.get(engagement_field_name))
        if looks_like_airtable_ids(raw_vals) and engagement_link_map:
            engagements = [engagement_link_map.get(_id, _id) for _id in raw_vals]
        else:
            engagements = [str(x).strip() for x in raw_vals if str(x).strip()]
    # Store engagement as pipe-joined list + count
    fields["Engagement_List"] = pipe_join(engagements)
    fields["Engagement_Count"] = len(engagements)

    # Timestamp to force diffs
    fields["Last Updated"] = timestamp

    wide_rows.append(fields)

    # ---- Build LONG rows (normalize Workstream × Economy × Engagement) ----
    ws_vals = [s for s in fields.get("Workstream_List", "").split("|") if s] or [""]
    ec_vals = [s for s in fields.get("Economy_List", "").split("|") if s] or [""]
    en_vals = [s for s in fields.get("Engagement_List", "").split("|") if s] or [""]

    for ws, ec, en in product(ws_vals, ec_vals, en_vals):
        long_row = dict(fields)
        long_row["Workstream_Single"] = ws
        long_row["Economy_Single"] = ec
        long_row["Engagement_Single"] = en
        # Overwrite "(Name)" fields to the single values for clarity in long file
        long_row["Workstream (Name)"] = ws
        long_row["Economy (Name)"] = ec
        long_rows.append(long_row)

# ------------------------------
# Write WIDE CSV
# ------------------------------
if wide_rows:
    preferred = [
        "Title", "Organization Type", "Fiscal Year",
        "Workstream (Name)", "Economy (Name)",
        "Workstream_List", "Economy_List",
        "Engagement_List", "Engagement_Count",
        "Last Updated",
    ]
    other_fields = sorted({k for r in wide_rows for k in r.keys()} - set(preferred))
    fieldnames = preferred + other_fields

    with open(WIDE_OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for r in wide_rows:
            writer.writerow(r)

print(f"✅ Export complete: {WIDE_OUT}")

# ------------------------------
# Write LONG CSV
# ------------------------------
if long_rows:
    preferred_long = [
        "Title", "Organization Type", "Fiscal Year",
        "Workstream_Single", "Economy_Single", "Engagement_Single",
        "Workstream (Name)", "Economy (Name)",
        "Workstream_List", "Economy_List", "Engagement_List",
        "Engagement_Count",
        "Last Updated",
    ]
    other_fields_long = sorted({k for r in long_rows for k in r.keys()} - set(preferred_long))
    fieldnames_long = preferred_long + other_fields_long

    with open(LONG_OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames_long, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for r in long_rows:
            writer.writerow(r)

print(f"✅ Export complete: {LONG_OUT}")

# ------------------------------
# Sanity notes
# ------------------------------
if not engagement_field_name:
    print("ℹ️ No engagement field detected (checked: Engagement Title, Engagement, Engagements).")

