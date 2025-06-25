import os
import requests
import pandas as pd
from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, CreateMode
import tableauserverclient as TSC

# === CONFIGURATION ===
CSV_URL = "https://github.com/usapecrise/US-APEC-RISE-Risk-Monitoring-Suite/blob/main/data/risk_signals.csv"
HYPER_NAME = "risk_signals.hyper"
DATASOURCE_NAME = "APEC Risk Signals"
PROJECT_NAME = "Default"

# === SECRETS (injected via GitHub Actions) ===
TABLEAU_SITE_ID = os.environ["TABLEAU_SITE_ID"]
TABLEAU_TOKEN_NAME = os.environ["TABLEAU_TOKEN_NAME"]
TABLEAU_TOKEN_SECRET = os.environ["TABLEAU_TOKEN_SECRET"]

# === STEP 1: Download the CSV ===
def download_csv():
    response = requests.get(CSV_URL)
    response.raise_for_status()
    df = pd.read_csv(pd.compat.StringIO(response.text))
    return df

# === STEP 2: Convert CSV to .hyper ===
def create_hyper(df):
    if os.path.exists(HYPER_NAME):
        os.remove(HYPER_NAME)

    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=HYPER_NAME, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            table_def = TableDefinition(
                table_name="Extract.RiskSignals",
                columns=[TableDefinition.Column(col, SqlType.text()) for col in df.columns]
            )
            connection.catalog.create_table(table_def)
            connection.execute_insert(table_def.table_name, df.itertuples(index=False, name=None))

# === STEP 3: Publish to Tableau Cloud ===
def publish_to_tableau():
    auth = TSC.PersonalAccessTokenAuth(
        token_name=TABLEAU_TOKEN_NAME,
        personal_access_token=TABLEAU_TOKEN_SECRET,
        site_id=TABLEAU_SITE_ID
    )
    server = TSC.Server("https://prod-useast-a.online.tableau.com", use_server_version=True)

    with server.auth.sign_in(auth):
        all_projects = list(TSC.Pager(server.projects))
        project = next((p for p in all_projects if p.name == PROJECT_NAME), None)

        all_datasources = list(TSC.Pager(server.datasources))
        existing = next((ds for ds in all_datasources if ds.name == DATASOURCE_NAME), None)

        if existing:
            server.datasources.delete(existing.id)

        new_ds = TSC.DatasourceItem(project_id=project.id, name=DATASOURCE_NAME)
        server.datasources.publish(new_ds, HYPER_NAME, mode=TSC.Server.PublishMode.CreateNew)
        print(f"✅ Published '{DATASOURCE_NAME}' to Tableau Cloud")

# === MAIN ===
if __name__ == "__main__":
    print("📥 Downloading CSV...")
    df = download_csv()
    print("🧪 Converting to .hyper...")
    create_hyper(df)
    print("🚀 Publishing to Tableau...")
    publish_to_tableau()
