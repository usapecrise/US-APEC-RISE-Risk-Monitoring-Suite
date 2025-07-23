import os
import glob
import pandas as pd
from tableauhyperapi import (
    HyperProcess,
    Telemetry,
    Connection,
    TableDefinition,
    SqlType,
    Inserter,
    CreateMode
)
import tableauserverclient as TSC

# ── CONFIG ─────────────────────────────────────────────────────────────────────
PAT_NAME       = os.environ["TABLEAU_PAT_NAME"]
PAT_SECRET     = os.environ["TABLEAU_PAT_SECRET"]
SITE_NAME      = os.environ["TABLEAU_SITE_NAME"]      # e.g. 'thecadmusgrouponline'
PROJECT_ID     = os.environ["TABLEAU_PROJECT_ID"]     # Project LUID (UUID)
TABLEAU_SERVER = os.environ["TABLEAU_REST_URL"]       # e.g. 'https://prod-useast-a.online.tableau.com'

# ── CONVERT CSV TO HYPER ─────────────────────────────────────────────────────────
def convert_csv_to_hyper(csv_path: str, hyper_path: str):
    """Convert a CSV file to a Hyper extract with all TEXT columns."""
    df = pd.read_csv(csv_path).fillna("").astype(str)

    table_def = TableDefinition(table_name="Extract")
    for col in df.columns:
        table_def.add_column(col, SqlType.text())

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_path, create_mode=CreateMode.CREATE_AND_REPLACE) as conn:
            conn.catalog.create_table(table_def)
            with Inserter(conn, table_def) as inserter:
                inserter.add_rows(rows=df.values.tolist())
                inserter.execute()

    size = os.path.getsize(hyper_path)
    print(f"📦 Created {hyper_path} ({size} bytes)")

# ── MAIN & PUBLISH ─────────────────────────────────────────────────────────────
def main():
    csv_files = glob.glob("*.csv")
    hyper_files = []
    print("🗂️  CSV files found:", csv_files)

    for csv in csv_files:
        base = os.path.splitext(csv)[0]
        hyper = f"{base}.hyper"
        print(f"⚙️  Converting {csv} → {hyper}")
        convert_csv_to_hyper(csv, hyper)
        hyper_files.append((base, hyper))

    # Authenticate
    print("🔑 Signing in to Tableau Cloud…")
    auth = TSC.PersonalAccessTokenAuth(PAT_NAME, PAT_SECRET, SITE_NAME)
    server = TSC.Server(TABLEAU_SERVER, use_server_version=True)

    with server.auth.sign_in(auth):
        if not PROJECT_ID:
            raise RuntimeError("❌ TABLEAU_PROJECT_ID is empty! Check your GitHub secret.")
        print(f"🔑 Using project ID: {PROJECT_ID[:8]}…")

        # Find schedule to use (e.g. named "Extract Refresh", or change this as needed)
        schedules, _ = server.schedules.get()
        schedule = next((s for s in schedules if "Extract Refresh" in s.name), None)

        if not schedule:
            print("⚠️ No 'Extract Refresh' schedule found on server. Skipping scheduling.")
        else:
            print(f"🗓️  Found schedule: {schedule.name} (ID: {schedule.id})")

        # Publish each .hyper file
        for base, hyper in hyper_files:
            print(f"📤 Publishing {hyper} as '{base}' into project ID {PROJECT_ID}")
            ds_item = TSC.DatasourceItem(project_id=PROJECT_ID, name=base)
            published_ds = server.datasources.publish(
                ds_item,
                hyper,
                mode=TSC.Server.PublishMode.Overwrite
            )
            print(f"✅ Published '{base}' (Datasource ID: {published_ds.id})")

            # Schedule extract refresh
            if schedule:
                task = TSC.ExtractRefreshTaskItem(datasource_id=published_ds.id, schedule_id=schedule.id)
                job = server.tasks.run(task)
                print(f"⏳ Scheduled refresh job started: {job.id}")

        server.auth.sign_out()
    print("🚪 Signed out of Tableau")

if __name__ == "__main__":
    main()
