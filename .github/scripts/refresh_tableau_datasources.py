import os
import tableauserverclient as TSC

# ── CONFIG ─────────────────────────────────────────────────────────────────────
PAT_NAME       = os.environ["TABLEAU_PAT_NAME"]
PAT_SECRET     = os.environ["TABLEAU_PAT_SECRET"]
SITE_NAME      = os.environ["TABLEAU_SITE_NAME"]      # e.g. 'thecadmusgrouponline'
PROJECT_ID     = os.environ["TABLEAU_PROJECT_ID"]     # Project LUID (UUID)
TABLEAU_SERVER = os.environ["TABLEAU_REST_URL"]       # e.g. 'https://prod-useast-a.online.tableau.com'

def main():
    print("🔑 Signing in to Tableau Cloud…")
    auth = TSC.PersonalAccessTokenAuth(PAT_NAME, PAT_SECRET, SITE_NAME)
    server = TSC.Server(TABLEAU_SERVER, use_server_version=True)

    with server.auth.sign_in(auth):
        if not PROJECT_ID:
            raise RuntimeError("❌ TABLEAU_PROJECT_ID is empty! Check your GitHub secret.")
        print(f"🔍 Targeting project ID: {PROJECT_ID[:8]}…")

        # 1) Get available refresh schedules
        schedules, _ = server.schedules.get()
        try:
            schedule = next(s for s in schedules if "Extract Refresh" in s.name)
            print(f"🗓️  Using schedule '{schedule.name}' (ID: {schedule.id})")
        except StopIteration:
            print("❌ No 'Extract Refresh' schedule found on the server.")
            return

        # 2) Find datasources in the specified project
        all_ds, _ = server.datasources.get()
        to_refresh = [ds for ds in all_ds if ds.project_id == PROJECT_ID]

        if not to_refresh:
            print(f"⚠️ No datasources found in project {PROJECT_ID}")
        else:
            for ds in to_refresh:
                print(f"🔄 Queuing refresh for '{ds.name}' (Datasource ID: {ds.id})")
                task = TSC.ExtractRefreshTaskItem(datasource_id=ds.id, schedule_id=schedule.id)
                job = server.tasks.run(task)
                print(f"⏳ Refresh job started: {job.id}")

        server.auth.sign_out()
    print("🚪 Signed out of Tableau")

if __name__ == "__main__":
    main()
