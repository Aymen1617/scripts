import logging
import requests
import time
from datetime import datetime, timedelta

# -----------------------------
# CONFIG
# -----------------------------
STRAPI_BASE_URL = "http://localhost:1337"
STRAPI_API_TOKEN = "0c9c66ed56aedc41aa9163aea437089587a801313281710a06de35237a57947715a8987842338cb1c77903f87bf139d9ddd02e8c213b00c63ad8158ab3a5ff456077cdfedb545e728ed61f09c19afd03f6708acff61fbe4443e87c1428e46b7067a75ff3ced4b62cf921bd1da7bee467a9299307d030a71c60c1a0221ac4947"
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
