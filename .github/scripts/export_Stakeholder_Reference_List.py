#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Export Stakeholder Reference List (wide + long) for Tableau.

- Resolves linked fields:
    * "Economy Reference List"  -> Economy names (from "Economy Reference List" table, display col "Economy")
    * "Workstream"              -> Workstream names (from "Workstream Reference List" table, display col "Workstream")

- Treats "Engagement" as a multi-select (list of strings) or empty.
  (If you later convert Engagement to a linked table, we can add a resolver.)

- Outputs:
    Stakeholder_Reference_List.csv
    Stakeholder_Reference_List_long.csv

- Adds:
    * Workstream_List, Economy_List, Engagement_List (pipe-joined)
    * Engagement_Count
    * Last Updated
    * Long rows normalized on Workstream × Economy × Engagement with
      Workstream_Single, Economy_Single, Engagement_Single
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

# Keys MUST match the field names in the main table that hold linked-record IDs.
LINKED_CONFIG = {
    "Economy Reference List": {"table": "Economy Reference List", "display": "Economy"},
    "Workstream":             {"table": "Workstream Reference List", "display": "Workstream"},
}

# Outputs (change to "data/..." if desired)
WIDE_OUT = "Stakeholder_Reference_List.csv"
LONG_OUT = "Stakeholder_Reference_List_long.csv"

HEADERS = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}

# ------------------------------
# Helpers
# ------------------------------
def fetch_all_records(table: str, view: str | None = None) -> list[dict]:
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
            print(f"❌ Non-JSON response from Airtable for table '{table}': {resp.text[:200]}")
            sys.exit(1)

        if "records" not in data:
            print(f"❌ Error fetching '{table}': {data}")
            sys.exit(1)

        all_records.extend(data["records"])
        offset = data.get("offset")
        if not offset:
            break
        params["offset"] = offset
        time.sleep(0.1)

    print(f"✅ Fetched {len(all_records)} records from '{table}'")
    return all_records

def ensure_list(v):
    """Return [] for None, keep list as-is, or wrap scalars in a list."""
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]

def pipe_join(values):
    """Join a list of strings with '|', skipping empties/whitespace."""
    vals = [str(x).strip() for x in values if str(x).strip()]
    return "|".join(vals) if vals else ""

# ------------------------------
# Build linked ID -> display maps
# ------------------------------
linked_id_maps: dict[str, dict] = {}
for main_field, cfg in LINKED_CONFIG.items():
    table_name = cfg["table"]
    display_col = cfg["display"]
    records = fetch_all_records(table_name)
    id_to_display = {rec["id"]: rec.get("fields", {}).get(display_col, "Unknown") for rec in records}
    linked_id_maps[main_field] = id_to_display

# ------------------------------
# Fetch main table
# ------------------------------
main_records = fetch_all_records(MAIN_TABLE, view=VIEW_NAME)
print(f"🔍 Retrieved {len(main_records)} records from '{MAIN_TABLE}'")

timestamp = datetime.utcnow().isoformat()

wide_rows: list[dict] = []
long_rows: list[dict] = []

for rec in main_records:
    fields = dict(rec.get("fields", {}))  # shallow copy

    # Resolve linked names for Economy & Workstream
    for main_field, cfg in LINKED_CONFIG.items():
        ids = ensure_list(fields.get(main_field))
        names = [linked_id_maps[main_field].get(_id, "Unknown") for _id in ids]
        disp = cfg["display"]  # "Economy" or "Workstream"
        fields[f"{disp} (Name)"] = ", ".join(names) if names else ""
        fields[f"{disp}_List"] = pipe_join(names)

    # Handle Engagement as multi-select (list of strings) or empty
    engagements = ensure_list(fields.get("Engagement"))
    # If Airtable returns linked IDs here in the future, you can still count them; they’ll be strings.
    # Convert any non-strings to strings; trim whitespace
    engagements = [str(x).strip() for x in engagements if str(x).strip()]
    fields["Engagement_List"] = pipe_join(engagements)
    fields["Engagement_Count"] = len(engagements)

    # Force timestamp for pipeline diffs
    fields["Last Updated"] = timestamp

    wide_rows.append(fields)

    # ---- Build LONG rows (normalize Workstream × Economy × Engagement) ----
    ws_vals = fields.get("Workstream_List", "")
    ec_vals = fields.get("Economy_List", "")
    en_vals = fields.get("Engagement_List", "")

    ws_list = [s for s in ws_vals.split("|") if s] or [""]
    ec_list = [s for s in ec_vals.split("|") if s] or [""]
    en_list = [s for s in en_vals.split("|") if s] or [""]  # stays empty if none selected

    for ws, ec, en in product(ws_list, ec_list, en_list):
        long_row = dict(fields)
        long_row["Workstream_Single"] = ws
        long_row["Economy_Single"] = ec
        long_row["Engagement_Single"] = en
        # Overwrite human-friendly singles for clarity
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
# Sanity checks (non-fatal)
# ------------------------------
try:
    wide_titles = {r.get("Title", "") for r in wide_rows}
    long_titles = {r.get("Title", "") for r in long_rows}
    missing = wide_titles - long_titles
    if missing:
        print(f"⚠️  Titles missing in long export (sample): {sorted(list(missing))[:5]}")
    if not any(row.get("Economy_Single") for row in long_rows):
        print("⚠️  Economy_Single is empty for all rows (no economies selected).")
    if not any(row.get("Workstream_Single") for row in long_rows):
        print("⚠️  Workstream_Single is empty for all rows (no workstreams selected).")
    # This will be common now that Engagement is optional, so not warning on it.
except Exception as e:
    print(f"⚠️  Sanity check warning: {e}")
