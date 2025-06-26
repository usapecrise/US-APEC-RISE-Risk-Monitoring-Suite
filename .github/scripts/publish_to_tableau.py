import pandas as pd
import requests
from io import StringIO
import os
from tableauhyperapi import (
    HyperProcess,
    Connection,
    Telemetry,
    TableDefinition,
    SqlType,
    Inserter,
    CreateMode,
    TableName
)
import tableauserverclient as TSC

# STEP 1: Download CSV from GitHub
csv_url = "https://raw.githubusercontent.com/usapecrise/US-APEC-RISE-Risk-Monitoring-Suite/main/data/risk_signals.csv"
response = requests.get(csv_url)
response.raise_for_status()
df = pd.read_csv(StringIO(response.text))
print("✅ CSV downloaded and loaded into DataFrame")

# STEP 2: Define Hyper schema
table_name = TableName("Extract", "Risk_Signals")
table_definition = TableDefinition(
    table_name=table_name,
    columns=[
        TableDefinition.Column("Economy", SqlType.text()),
        TableDefinition.Column("Workstream", SqlType.text()),
        TableDefinition.Column("Assumption", SqlType.text()),
        TableDefinition.Column("Scenario", SqlType.text()),
        TableDefinition.Column("Justification", SqlType.text()),
        TableDefinition.Column("Signal Strength", SqlType.text()),
        TableDefinition.Column("Signal Strength (Numeric)", SqlType.int())
    ]
)

# STEP 3: Create .hyper file
hyper_path = "risk_signals.hyper"
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(endpoint=hyper.endpoint, database=hyper_path, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
        connection.catalog.create_schema("Extract")
        connection.catalog.create_table(table_definition)
        with Inserter(connection, table_definition) as inserter:
            inserter.add_rows(rows=df.itertuples(index=False, name=None))
            inserter.execute()
print(f"✅ Hyper file created: {hyper_path}")

# STEP 4: Publish to Tableau Cloud
token_name = os.environ["TABLEAU_TOKEN_NAME"]
token_value = os.environ["TABLEAU_TOKEN_VALUE"]
site_id = os.environ.get("TABLEAU_SITE_ID", "")

tableau_auth = TSC.PersonalAccessTokenAuth(
    token_name=token_name,
    personal_access_token=token_value,
    site_id=site_id
)

server = TSC.Server("https://prod-useast-a.online.tableau.com", use_server_version=True)

with server.auth.sign_in(tableau_auth):
    all_projects, _ = server.projects.get()
    project = next((p for p in all_projects if p.name == "US APEC-RISE"), None)
    if not project:
        raise RuntimeError("❌ Tableau project 'US APEC-RISE' not found.")

    datasource = TSC.HyperDatasourceItem(
        name="APEC Risk Signals",
        project_id=project.id
    )

    published_ds = server.datasources.publish(
        datasource,
        hyper_path,
        mode=TSC.Server.PublishMode.Overwrite
    )

    print(f"✅ Successfully published to Tableau Cloud: {published_ds.name} in project '{project.name}'")
