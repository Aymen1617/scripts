import logging
import requests
import time
from datetime import datetime, timedelta

# -----------------------------
# CONFIG
# -----------------------------
STRAPI_BASE_URL = "http://localhost:1337"
STRAPI_API_TOKEN = "d0183c684eef0a77fc4b8998e570ba14f511c08a93acd2b3e6e1d27544a8de88a8355cd0d205d01d18f64e5dc648ecd8d4f1b5bbc729b345ac453373aaa887abd7f790097c84515d24b16520f892406d2518c0fcd53a9fff3dcffc52923d66bd40a115e56d59995e08573a2185798306ab470de98dc5fac4364729ed0a4eae1d"

HEADERS = {
    "Authorization": f"Bearer {STRAPI_API_TOKEN}",
    "Content-Type": "application/json"
}

# -----------------------------
# LOGGING
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s"
)

# -----------------------------
# STRAPI HELPERS
# -----------------------------
def strapi_get(url, params=None):
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json()

def update_snapshot_status(snapshot_id, status):
    url = f"{STRAPI_BASE_URL}/api/snapshots/{snapshot_id}"
    payload = {"data": {"snapshotStatus": status}}
    resp = requests.put(url, json=payload, headers=HEADERS)
    resp.raise_for_status()
    logging.info(f"[Snapshot {snapshot_id}] Status updated to {status}")

def delete_connector(connector_id):
    url = f"{STRAPI_BASE_URL}/api/connectors/{connector_id}"
    resp = requests.delete(url, headers=HEADERS)
    resp.raise_for_status()
    logging.info(f"Deleted connector {connector_id}")

def update_connector_state(connector_id, state):
    url = f"{STRAPI_BASE_URL}/api/connectors/{connector_id}"
    payload = {"data": {"state": state}}
    resp = requests.put(url, json=payload, headers=HEADERS)
    resp.raise_for_status()
    logging.info(f"Updated connector {connector_id} state to {state}")

# -----------------------------
# SNAPSHOT PROCESSING
# -----------------------------
def process_done_snapshots():
    logging.info("Starting snapshot verification process")
    try:
        snapshots = strapi_get(f"{STRAPI_BASE_URL}/api/snapshots?filters[isActive][$eq]=true&filters[snapshotStatus][$eq]=done").get("data", [])
        logging.info(f"Fetched {len(snapshots)} done snapshots")
    except Exception as e:
        logging.error(f"Failed to fetch snapshots: {e}")
        return

    for snapshot in snapshots:
        snapshot_id = snapshot["id"]
        logging.info(f"[Snapshot {snapshot_id}] Processing snapshot...")
        # Here you would call verify_non_incremental_snapshot(snapshot)
        # Since full logic is long, call your existing function directly
        try:
            from snapshot_verifier import verify_non_incremental_snapshot
            result = verify_non_incremental_snapshot(snapshot)
            status = result.get("status", "error")
            update_snapshot_status(snapshot_id, status)
            logging.info(f"[Snapshot {snapshot_id}] Snapshot verification finished with status: {status}")
        except Exception as e:
            logging.error(f"[Snapshot {snapshot_id}] Verification failed: {e}", exc_info=True)

# -----------------------------
# ENTRY POINT
# -----------------------------
if __name__ == "__main__":
    logging.info("Snapshot verification job started")
    process_done_snapshots()
    logging.info("Snapshot verification job finished")
