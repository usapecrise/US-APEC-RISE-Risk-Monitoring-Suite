import os
import tableauserverclient as TSC

# ── CONFIG ─────────────────────────────────────────────────────────────────────
PAT_NAME       = os.environ["TABLEAU_PAT_NAME"]
PAT_SECRET     = os.environ["TABLEAU_PAT_SECRET"]
SITE_NAME      = os.environ["TABLEAU_SITE_NAME"]      # e.g. 'thecadmusgrouponline'
PROJECT_ID     = os.environ["TABLEAU_PROJECT_ID"]     # your project LUID
TABLEAU_SERVER = os.environ["TABLEAU_REST_URL"]       # e.g. 'https://prod-useast-a.online.tableau.com'

def main():
    # 1) Sign in
    auth   = TSC.PersonalAccessTokenAuth(PAT_NAME, PAT_SECRET, SITE_NAME)
    server = TSC.Server(TABLEAU_SERVER, use_server_version=True)
    with server.auth.sign_in(auth):
        # 2) Grab the default Extract Refresh schedule
        schedules, _ = server.schedules.get()
        try:
            sched = next(s for s in schedules if "Extract Refresh" in s.name)
        except StopIteration:
            print("❌ No 'Extract Refresh' schedule found on the server.")
            return

        print(f"🗓️  Using schedule '{sched.name}' (ID: {sched.id})")

        # 3) List datasources in the target project
        all_ds, _ = server.datasources.get()
        to_refresh = [ds for ds in all_ds if ds.project_id == PROJECT_ID]
        if not to_refresh:
            print(f"⚠️ No datasources found in project {PROJECT_ID}")
        for ds in to_refresh:
            # 4) Create & run an extract refresh task
            task = TSC.ExtractRefreshTaskItem(datasource_id=ds.id, schedule_id=sched.id)
            print(f"🔄 Queuing refresh for '{ds.name}' (DataSource ID: {ds.id})")
            job = server.tasks.run(task)
            print(f"⏳ Job started: {job.id}")

        # 5) Sign out
        server.auth.sign_out()
    print("🚪 Signed out of Tableau")

if __name__ == "__main__":
    main()

