# nb_refresh_sql_endpoint v1.0
# Syncs the lakehouse SQL analytics endpoint after the Silver notebooks, so the warehouse loaders see this week's rows.
# Replaces the fixed Wait in the pipeline. Fails if any table reports a sync failure.

# ---------------- CELL 0: PARAMETERS (mark as the parameter cell) ----------------
LAKEHOUSE_NAME = "lh_silver"
TIMEOUT_MINUTES = 15


# ---------------- CELL 1: REFRESH ----------------
import json, time
import requests
import notebookutils

API = "https://api.fabric.microsoft.com/v1"
workspace_id = notebookutils.runtime.context["currentWorkspaceId"]
try:
    token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
except Exception:
    token = notebookutils.credentials.getToken("pbi")
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def call(method, url, **kw):
    r = requests.request(method, url, headers=headers, timeout=60, **kw)
    if r.status_code >= 400:
        raise RuntimeError(f"{method} {url} returned {r.status_code}: {r.text[:500]}")
    return r

lakehouses = call("GET", f"{API}/workspaces/{workspace_id}/lakehouses").json().get("value", [])
match = [l for l in lakehouses if l.get("displayName") == LAKEHOUSE_NAME]
if not match:
    raise RuntimeError(f"No lakehouse named {LAKEHOUSE_NAME} in this workspace.")
endpoint_id = match[0]["properties"]["sqlEndpointProperties"]["id"]

r = call("POST", f"{API}/workspaces/{workspace_id}/sqlEndpoints/{endpoint_id}/refreshMetadata",
         json={"timeout": {"timeUnit": "Minutes", "value": TIMEOUT_MINUTES}})

if r.status_code == 202:
    op_url = r.headers.get("Location")
    deadline = time.time() + TIMEOUT_MINUTES * 60 + 60
    while True:
        time.sleep(int(r.headers.get("Retry-After", 10)))
        r = call("GET", op_url)
        status = r.json().get("status") if r.content else None
        if status in ("Succeeded", "Failed", "Canceled") or time.time() > deadline:
            break
    if status != "Succeeded":
        raise RuntimeError(f"Endpoint refresh ended with status {status}: {r.text[:500]}")
    r = call("GET", op_url.rstrip("/") + "/result")

body = r.json() if r.content else {}
tables = body.get("value", body) if isinstance(body, dict) else body
failed = [t for t in tables if str(t.get("status", "")).lower() == "failure"]
synced = [t for t in tables if str(t.get("status", "")).lower() == "success"]
print(f"{LAKEHOUSE_NAME}: {len(synced)} tables synced, {len(tables) - len(synced) - len(failed)} unchanged, {len(failed)} failed")
if failed:
    raise RuntimeError("SQL endpoint sync failed for: " + ", ".join(
        f"{t.get('tableName')} ({(t.get('error') or {}).get('message', '')})" for t in failed))


# ---------------- CELL 2: EXIT ----------------
notebookutils.notebook.exit(json.dumps({"lakehouse": LAKEHOUSE_NAME, "synced": len(synced), "failed": len(failed)}))
