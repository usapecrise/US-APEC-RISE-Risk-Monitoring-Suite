#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Export Stakeholder Reference List (wide + long) for Tableau.

- Resolves linked fields:
    * "Economy Reference List"  -> Economy names (display "Economy")
    * "Workstream"              -> Workstream names (display "Workstream")
- Handles Engagements:
    * Detects the engagements field in main table: "Engagement Title" | "Engagement" | "Engagements"
    * If values look like Airtable record IDs (rec...), resolves them by scanning candidate tables
      for a readable display field (Engagement Title | Title | Workshop Title | Name).

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
    print("❌ AIRTABLE_TOKEN is not set."); sys.exit(1)

BASE_ID = "app0Ljjhrp3lTTpTO"
MAIN_TABLE = "Stakeholder Reference List"
VIEW_NAME = "Grid view"

# Keys MUST match field names in the main table that hold linked-record IDs
LINKED_CONFIG = {
    "Economy Reference List": {"table": "Economy Reference List", "display": "Economy"},
    "Workstream":             {"table": "Workstream Reference List", "display": "Workstream"},
}

# Engagements: main-table field candidates and candidate tables to resolve recIDs
ENGAGEMENT_FIELD_CANDIDATES = ["Engagement Title", "Engagement", "Engagements"]
CANDIDATE_ENGAGEMENT_TABLES = [
    # (table_name, candidate_display_columns in priority order)
    ("Engagement Reference List", ["Engagement Title", "Title", "Name"]),
    ("Workshop Master List",      ["Workshop Title", "Title", "Name"]),
    ("Engagements",               ["Title", "Name"]),
    ("Engagement Log",            ["Title", "Name"]),
]

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
            msg = f"❌ Non-JSON response from '{table}': {resp.text[:200]}"
            if strict: print(msg); sys.exit(1)
            else: print(msg); return []
        if "records" not in data:
            msg = f"❌ Error fetching '{table}': {data}"
            if strict: print(msg); sys.exit(1)
            else: print(msg); return []
        all_records.extend(data["records"])
        offset = data.get("offset")
        if not offset:
            break
        params["offset"] = offset
        time.sleep(0.1)
    print(f"✅ Fetched {len(all_records)} from '{table}'")
    return all_records

def ensure_list(v):
    if v is None: return []
    if isinstance(v, list): return v
    return [v]

def pipe_join(values):
    vals = [str(x).strip() for x in values if str(x).strip()]
    return "|".join(vals) if vals else ""

def looks_like_airtable_ids(values):
    if not values: return False
    return all(isinstance(x, str) and x.startswith("rec") for x in values)

def pick_first_present(fields_dict: dict, candidates: list[str]):
    for c in candidates:
        if c in fields_dict and str(fields_dict[c]).strip():
            return fields_dict[c]
    return None

# ------------------------------
# Fetch main records (to detect engagement field)
# ------------------------------
main_records = fetch_all_records(MAIN_TABLE, view=VIEW_NAME, strict=True)
print(f"🔍 Retrieved {len(main_records)} from '{MAIN_TABLE}'")

engagement_field_name = None
for rec in main_records:
    f = rec.get("fields", {})
    for cand in ENGAGEMENT_FIELD_CANDIDATES:
        if cand in f:
            engagement_field_name = cand
            break
    if engagement_field_name: break
if engagement_field_name:
    print(f"🔎 Engagement field detected: '{engagement_field_name}'")
else:
    print("ℹ️ No engagement field found (checked: Engagement Title, Engagement, Engagements)")

# ------------------------------
# Build linked maps (Economy/Workstream)
# ------------------------------
linked_id_maps: dict[str, dict] = {}
for main_field, cfg in LINKED_CONFIG.items():
    records = fetch_all_records(cfg["table"], strict=True)
    linked_id_maps[main_field] = {
        rec["id"]: rec.get("fields", {}).get(cfg["display"], "Unknown")
        for rec in records
    }

# ------------------------------
# Build engagement resolver (try multiple tables)
# ------------------------------
engagement_id_to_title = {}
for table_name, display_candidates in CANDIDATE_ENGAGEMENT_TABLES:
    try:
        recs = fetch_all_records(table_name, strict=False)
    except Exception as e:
        print(f"⚠️ Skipping '{table_name}': {e}")
        recs = []
    added = 0
    for r in recs:
        rid = r.get("id")
        title = pick_first_present(r.get("fields", {}), display_candidates)
        if rid and title and rid not in engagement_id_to_title:
            engagement_id_to_title[rid] = str(title).strip()
            added += 1
    if added:
        print(f"🔗 Engagement map: added {added} from '{table_name}'")

timestamp = datetime.utcnow().isoformat()
wide_rows, long_rows = [], []

# ------------------------------
# Transform records
# ------------------------------
for rec in main_records:
    fields = dict(rec.get("fields", {}))  # shallow copy

    # Resolve Economy & Workstream
    for main_field, cfg in LINKED_CONFIG.items():
        ids = ensure_list(fields.get(main_field))
        names = [linked_id_maps[main_field].get(_id, "Unknown") for _id in ids]
        disp = cfg["display"]
        fields[f"{disp} (Name)"] = ", ".join(names) if names else ""
        fields[f"{disp}_List"]  = pipe_join(names)

    # Resolve Engagements
    engagements_list = []
    if engagement_field_name:
        raw_vals = ensure_list(fields.get(engagement_field_name))
        if looks_like_airtable_ids(raw_vals):
            # Map recIDs -> titles if we can; otherwise keep recID fallback
            engagements_list = [engagement_id_to_title.get(x, x) for x in raw_vals]
        else:
            # Multi-select strings
            engagements_list = [str(x).strip() for x in raw_vals if str(x).strip()]

    fields["Engagement_List"]  = pipe_join(engagements_list)
    fields["Engagement_Count"] = len(engagements_list)
    # For debugging: keep the raw IDs if they existed and we failed to map all
    if engagement_field_name:
        raw_vals = ensure_list(fields.get(engagement_field_name))
        if looks_like_airtable_ids(raw_vals):
            fields["Engagement_IDs"] = pipe_join(raw_vals)

    fields["Last Updated"] = timestamp
    wide_rows.append(fields)

    # Build LONG rows (Workstream × Economy × Engagement)
    ws_list = [s for s in fields.get("Workstream_List", "").split("|") if s] or [""]
    ec_list = [s for s in fields.get("Economy_List", "").split("|") if s] or [""]
    en_list = [s for s in fields.get("Engagement_List", "").split("|") if s] or [""]

    for ws, ec, en in product(ws_list, ec_list, en_list):
        row = dict(fields)
        row["Workstream_Single"] = ws
        row["Economy_Single"]    = ec
        row["Engagement_Single"] = en
        # Overwrite "(Name)" to singletons in long output
        row["Workstream (Name)"] = ws
        row["Economy (Name)"]    = ec
        long_rows.append(row)

# ------------------------------
# Write WIDE CSV
# ------------------------------
if wide_rows:
    preferred = [
        "Title", "Organization Type", "Fiscal Year",
        "Workstream (Name)", "Economy (Name)",
        "Workstream_List", "Economy_List",
        "Engagement_List", "Engagement_Count", "Engagement_IDs",
        "Last Updated",
    ]
    other_fields = sorted({k for r in wide_rows for k in r.keys()} - set(preferred))
    fieldnames = preferred + other_fields
    with open(WIDE_OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for r in wide_rows:
            w.writerow(r)
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
        "Engagement_Count", "Engagement_IDs",
        "Last Updated",
    ]
    other_fields_long = sorted({k for r in long_rows for k in r.keys()} - set(preferred_long))
    fieldnames_long = preferred_long + other_fields_long
    with open(LONG_OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames_long, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for r in long_rows:
            w.writerow(r)
print(f"✅ Export complete: {LONG_OUT}")

# ------------------------------
# Final notes
# ------------------------------
if engagement_field_name and looks_like_airtable_ids([]):
    pass  # placeholder
unresolved = [
    rid for rid in pipe_join(ensure_list(rec.get("fields", {}).get(engagement_field_name, []))).split("|")
    if rid and rid.startswith("rec") and rid not in engagement_id_to_title
] if engagement_field_name else []
if unresolved:
    print(f"⚠️ Some engagement record IDs had no title mapping (first 5): {unresolved[:5]}")
