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
