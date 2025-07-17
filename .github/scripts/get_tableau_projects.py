import os
import requests

# Load from environment
TOKEN_NAME = os.environ['TABLEAU_TOKEN_NAME']
TOKEN_SECRET = os.environ['TABLEAU_TOKEN_SECRET']
SITE_ID = os.environ['TABLEAU_SITE_ID']
API_VERSION = "3.21"

# Use your Tableau Cloud domain
BASE_URL = f"https://prod-useast-a.online.tableau.com/api/{API_VERSION}"

# Sign in
payload = {
    "credentials": {
        "personalAccessTokenName": TOKEN_NAME,
        "personalAccessTokenSecret": TOKEN_SECRET,
        "site": { "contentUrl": SITE_ID }
    }
}

auth_response = requests.post(
    f"{BASE_URL}/auth/signin",
    json=payload,
    timeout=15
)
auth_response.raise_for_status()

auth_token = auth_response.json()['credentials']['token']
site_id = auth_response.json()['credentials']['site']['id']
headers = {"X-Tableau-Auth": auth_token}

# Get list of projects
project_url = f"{BASE_URL}/sites/{site_id}/projects"
r = requests.get(project_url, headers=headers)
r.raise_for_status()

projects = r.json()["projects"]["project"]

print("\n📁 Tableau Projects and their IDs:\n")
for project in projects:
    print(f"{project['name']} → {project['id']}")
