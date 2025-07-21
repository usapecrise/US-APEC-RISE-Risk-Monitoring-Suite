import os
import tableauserverclient as TSC

# ── CONFIG ─────────────────────────────────────────────────────────────────────
PAT_NAME       = os.environ["TABLEAU_PAT_NAME"]
PAT_SECRET     = os.environ["TABLEAU_PAT_SECRET"]
SITE_NAME      = os.environ["TABLEAU_SITE_NAME"]      # e.g. 'thecadmusgrouponline'
PROJECT_ID     = os.environ["TABLEAU_PROJECT_ID"]     # Project LUID (UUID) to refresh
TABLEAU_SERVER = os.environ["TABLEAU_REST_URL"]       # e.g. 'https://prod-useast-a.online.tableau.com'

def main():
    # Authenticate
    auth   = TSC.PersonalAccessTokenAuth(PAT_NAME, PAT_SECRET, SITE_NAME)
    server = TSC.Server(TABLEAU_SERVER, use_server_version=True)

    with server.auth.sign_in(auth):
        # List all datasources in the site
        all_ds, _ = server.datasources.get()
        # Filter for those in our project
        to_refresh = [ds for ds in all_ds if ds.project_id == PROJECT_ID]

        if not to_refresh:
            print(f"⚠️ No datasources found in project ID {PROJECT_ID}")
        for ds in to_refresh:
            print(f"🔄 Refreshing extract for '{ds.name}' (ID: {ds.id})")
            server.datasources.refresh(ds)
            print(f"✅ Refreshed '{ds.name}'")

        server.auth.sign_out()
    print("🚪 Signed out of Tableau")

if __name__ == "__main__":
    main()
