import os
import glob
import pandas as pd
from tableauhyperapi import (
    HyperProcess, Telemetry, Connection, TableDefinition,
    SqlType, Inserter, CreateMode, TableName
)
import tableauserverclient as TSC

# ── CONFIG ──────────────────────────────────────────────
PAT_NAME       = os.environ["TABLEAU_PAT_NAME"]
PAT_SECRET     = os.environ["TABLEAU_PAT_SECRET"]
SITE_NAME      = os.environ["TABLEAU_SITE_NAME"]
PROJECT_ID     = os.environ["TABLEAU_PROJECT_ID"]
TABLEAU_SERVER = os.environ["TABLEAU_REST_URL"]

# Optional: Workbooks to refresh after publish
WORKBOOKS_TO_REFRESH = [
    "US APEC-RISE Dashboard"
]

# ── FIXED MAPPINGS: CSV → EXTRACT NAME ──────────────────
EXTRACT_NAME_MAP = {
    "OT1.csv": "OT1 Extract",
    "OT2.csv": "OT2 Extract",
    "OT3.csv": "OT3 Extract",
    "OT4.csv": "OT4 Extract",
    "OT5.csv": "OT5 Extract",
    "OC1.csv": "OC1 Extract",
    "OC2.csv": "OC2 Extract",
    "OC3.csv": "OC3 Extract",
    "OC4.csv": "OC4 Extract",
    "OC5.csv": "OC5 Extract",
    "OC6.csv": "OC6 Extract",
    "OC7.csv": "OC7 Extract",
    "KPI_Targets.csv": "KPI Target Reference",
    "Workshop_Master_List.csv': Workshop Master List
}

# ── CONVERT CSV TO HYPER ───────────────────────────────
def convert_csv_to_hyper(csv_path: str, hyper_path: str):
    df = pd.read_csv(csv_path)

    def map_dtype(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return SqlType.int()
        elif pd.api.types.is_float_dtype(dtype):
            return SqlType.double()
        else:
            return SqlType.text()

    table_def = TableDefinition(table_name=TableName("Extract"))
    for col in df.columns:
        sql_type = map_dtype(df[col].dtype)
        table_def.add_column(col, sql_type)
        if sql_type == SqlType.text():
            df[col] = df[col].astype(str).fillna("")

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_path, create_mode=CreateMode.CREATE_AND_REPLACE) as conn:
            conn.catalog.create_table(table_def)
            with Inserter(conn, table_def) as inserter:
                inserter.add_rows(rows=df.itertuples(index=False, name=None))
                inserter.execute()

    print(f"📦 Created {hyper_path} ({os.path.getsize(hyper_path)} bytes)")

# ── TRIGGER WORKBOOK REFRESH ───────────────────────────
def trigger_workbook_refresh(server, workbook_name):
    print(f"🔁 Searching for workbook '{workbook_name}'...")
    all_workbooks, _ = server.workbooks.get()
    matched = [wb for wb in all_workbooks if wb.name == workbook_name]

    if not matched:
        print(f"❌ Workbook '{workbook_name}' not found.")
        return

    workbook = matched[0]
    print(f"🔄 Triggering refresh for workbook '{workbook.name}' (ID: {workbook.id})")
    try:
        job = server.workbooks.refresh(workbook)
        print(f"⏳ Refresh job submitted (Job ID: {job.id})")
    except Exception as e:
        print(f"❌ Failed to refresh workbook '{workbook.name}': {e}")

# ── MAIN EXECUTION ─────────────────────────────────────
def main():
    csv_files = glob.glob("*.csv")
    print("🗂️ Found CSVs:", csv_files)

    auth = TSC.PersonalAccessTokenAuth(PAT_NAME, PAT_SECRET, SITE_NAME)
    server = TSC.Server(TABLEAU_SERVER, use_server_version=True)

    with server.auth.sign_in(auth):
        for csv_file in csv_files:
            if csv_file not in EXTRACT_NAME_MAP:
                print(f"⚠️ Skipping unrecognized file: {csv_file}")
                continue

            extract_name = EXTRACT_NAME_MAP[csv_file]
            hyper_path = f"{os.path.splitext(csv_file)[0]}.hyper"

            print(f"🔄 Converting {csv_file} → {hyper_path}")
            convert_csv_to_hyper(csv_file, hyper_path)

            print(f"📤 Publishing '{hyper_path}' as '{extract_name}' into project ID {PROJECT_ID}")
            ds_item = TSC.DatasourceItem(project_id=PROJECT_ID, name=extract_name)
            ds_item.connection_credentials = None

            published_ds = server.datasources.publish(
                ds_item,
                hyper_path,
                mode=TSC.Server.PublishMode.Overwrite
            )

            print(f"✅ Overwrote extract: '{extract_name}' (Datasource ID: {published_ds.id})")

        # Refresh workbooks if configured
        for wb_name in WORKBOOKS_TO_REFRESH:
            trigger_workbook_refresh(server, wb_name)

    print("✅ Finished uploading extracts and refreshing workbooks.")

if __name__ == "__main__":
    main()
