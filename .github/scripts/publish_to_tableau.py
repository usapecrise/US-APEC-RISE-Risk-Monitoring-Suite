import pandas as pd
import requests
from io import StringIO
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

# Step 1: Download CSV from GitHub
csv_url = "https://raw.githubusercontent.com/usapecrise/US-APEC-RISE-Risk-Monitoring-Suite/main/data/risk_signals.csv"
response = requests.get(csv_url)
response.raise_for_status()  # Fail loudly if URL is broken

# Step 2: Load into pandas
csv_data = StringIO(response.text)
df = pd.read_csv(csv_data)

# Step 3: Define Hyper table schema
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

# Step 4: Create the .hyper file and insert data
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(endpoint=hyper.endpoint, database="risk_signals.hyper", create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
        connection.catalog.create_schema("Extract")
        connection.catalog.create_table(table_definition)

        # Insert rows from DataFrame into the Hyper file
        with Inserter(connection, table_definition) as inserter:
            inserter.add_rows(rows=df.itertuples(index=False, name=None))
            inserter.execute()

print("✅ Hyper file created and populated successfully: risk_signals.hyper")

import tableauserverclient as TSC
import os

# Step 5: Publish to Tableau Cloud using environment variables
token_name = os.environ["ScenarioPush"]
token_value = os.environ["LwLRC1TQQQa6Xo73kvgQ7g==:HmKlfJ18jBJoJoUMlifI9jwHTGAT8P0Q"]
site_id = os.environ.get("thecadmusgrouponline", "")

tableau_auth = TSC.PersonalAccessTokenAuth(
    token_name=token_name,
    personal_access_token=token_value,
    site_id=site_id
)

server = TSC.Server("https://us-east-1a.online.tableau.com", use_server_version=True)

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
        "risk_signals.hyper",
        mode=TSC.Server.PublishMode.Overwrite
    )

    print(f"✅ Published to Tableau Cloud: {published_ds.name} in project 'US APEC-RISE'")

