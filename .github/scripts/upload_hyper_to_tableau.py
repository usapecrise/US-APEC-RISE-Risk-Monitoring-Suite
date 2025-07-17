import os
import glob
import requests
import csv
from tableauhyperapi import (
    HyperProcess, Telemetry, Connection, CreateMode,
    SqlType, TableDefinition, Inserter, TableName
)

# Load environment variables
TOKEN_NAME = os.environ['TABLEAU_TOKEN_NAME']
TOKEN_SECRET = os.environ['TABLEAU_TOKEN_SECRET']
SITE_ID = os.environ['TABLEAU_SITE_ID']
PROJECT_ID = os.environ['TABLEAU_PROJECT_ID']
API_VERSION = "3.21"
BASE_URL = f"https://prod-useast-a.online.tableau.com/api/{API_VERSION}"

print("🚦 Starting Tableau Hyper upload script")
print(f"📁 Current directory: {os.getcwd()}")
print(f"🔍 Looking for CSVs in: {os.listdir('.')}")
csv_files = glob.glob("*.csv")
print(f"🗂️ Matched CSV files: {csv_files}")

# Step 1: Sign in to Tableau
signin_payload = {
    "credentials": {
        "personalAccessTokenName": TOKEN_NAME,
        "personalAccessTokenSecret": TOKEN_SECRET,
        "site": {"contentUrl": SITE_ID}
    }
}
auth_headers = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}

