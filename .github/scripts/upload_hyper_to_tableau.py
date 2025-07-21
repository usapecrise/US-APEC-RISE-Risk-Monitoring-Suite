import os
import glob
import pandas as pd
import xml.etree.ElementTree as ET
import tableauserverclient as TSC
from tableauhyperapi import (
    HyperProcess,
    Telemetry,
    Connection,
    TableDefinition,
    SqlType,
    Inserter,
    CreateMode
)

# ── CONFIG ─────────────────────────────────────────────────────────────────────
PAT_NAME       = os.environ["TABLEAU_PAT_NAME"]
PAT_SECRET     = os.environ["TABLEAU_PAT_SECRET"]
SITE_NAME      = os.environ["TABLEAU_SITE_NAME"]      # e.g. 'thecadmusgrouponline'
PROJECT_ID     = os.environ["TABLEAU_PROJECT_ID"]     # will be verified below
TABLEAU_SERVER = os.environ["TABLEAU_REST_URL"]       # e.g. 'https://prod-useast-a.online.tableau.com'

# ── CSV → HYPER ────────────────────────────────────────────────────────────────
def convert_csv_to_hyper(csv_path, hyper_path):
    df = pd.read_csv(csv_path).fillna("").astype(str)
    table_def = TableDefinition(table_name="Extract")
    for col in df.columns:
        table_def.add_column(col, SqlType.text())

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(
            endpoint=hyper.endpoint,
            database=hyper_path,
            create_mode=CreateMode.CREATE_AND_REPLACE
        ) as conn:
            conn.catalog.create_table(table_def)
            with Inserter(conn, table_def) as inserter:
                inserter.add_rows(rows=df.values.tolist())
                inserter.execute()
    size = os.path.getsize(hyper_path)
    print(f"📦 Created {hyper_path} ({size} bytes)")

# ── MAIN & PUBLISH ─────────────────────────────────────────────────────────────
def main():
    # Convert CSVs
    csv_files = glob.glob("*.csv")
    hyper_files = []
    for csv in csv_files:
        base = os.path.splitext(csv)[0]
        hyper = f"{base}.hyper"
        print(f"⚙️  Converting {csv} → {hyper}")
        convert_csv_to_hyper(csv, hyper)
        hyper_files.append((base, hyper))

    # Sign in via TSC
    print("🔑 Signing in to Tableau Cloud…")
    auth = TSC.PersonalAccessTokenAuth(PAT_NAME, PAT_SECRET, SITE_NAME)
    server = TSC.Server(TABLEAU_SERVER, use_server_version=True)
    with server.auth.sign_in(auth):
        # ← INSERTED SNIPPET: list all projects and their IDs
        all_projects, _ = server.projects.get()
        print("🍀 Available Projects:")
        for p in all_projects:
            print(f" • {p.name}: {p.id}")
        # ← end snippet

PROJECT_ID = os.environ["TABLEAU_PROJECT_ID"]
if not PROJECT_ID:
    raise RuntimeError("TABLEAU_PROJECT_ID is empty! Check your GitHub secret.")
print(f"🔑 Using project ID: {PROJECT_ID[:8]}…")  # prints only first 8 chars

        # Now publish using the PROJECT_ID secret (once you've confirmed it)
        for base, hyper in hyper_files:
            print(f"📤 Publishing {hyper} as '{base}' into project ID {PROJECT_ID}")
            ds_item = TSC.DatasourceItem(PROJECT_ID, name=base)
            server.datasources.publish(
                ds_item,
                hyper,
                mode=TSC.Server.PublishMode.Overwrite
            )
            print(f"✅ Published '{base}'")
        server.auth.sign_out()
    print("🚪 Signed out of Tableau")

if __name__ == "__main__":
    main()

