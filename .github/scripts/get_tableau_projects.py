import os
import requests

# Load from GitHub secrets (or replace with actual values for testing)
TOKEN_NAME = os.environ['TABLEAU_TOKEN_NAME']
TOKEN_SECRET = os.environ['TABLEAU_TOKEN_SECRET']
SITE_ID = os.environ['TABLEAU_SITE_ID']
API_VERSION = "3.21"

# Step 1: Sign in to Tableau
auth_payload = {
    "credentials": {
        "personalAccessTokenName": TOKEN_NAME,
        "personalAccessTokenSecret": TOKEN_SECRET,
        "site": { "contentUrl": SITE_ID }
    }
}

auth_response = requests.post(
    f"https://api.tableau.com/api/{API_VERSION}/auth/signin",
    json=auth_payload
)
auth_response.raise_for_status()

auth_token = auth_response.json()['credentials']['token']
site_id = auth_response.json()['credentials']['site']['id']
headers = { "X-Tableau-Auth": auth_token }

# Step 2: Get list of all projects
project_url = f"https://api.tableau.com/api/{API_VERSION}/sites/{site_id}/projects"
r = requests.get(project_url, headers=headers)
r.raise_for_status()

projects = r.json()["projects"]["project"]

print("\n📁 Tableau Projects and their IDs:\n")
for project in projects:
    print(f"{project['name']} → {project['id']}")
