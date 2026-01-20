
from datetime import datetime
import logging
import requests

# -----------------------------
# CONFIG
# -----------------------------
STRAPI_BASE_URL = "http://localhost:1337"
STRAPI_API_TOKEN = "d0183c684eef0a77fc4b8998e570ba14f511c08a93acd2b3e6e1d27544a8de88a8355cd0d205d01d18f64e5dc648ecd8d4f1b5bbc729b345ac453373aaa887abd7f790097c84515d24b16520f892406d2518c0fcd53a9fff3dcffc52923d66bd40a115e56d59995e08573a2185798306ab470de98dc5fac4364729ed0a4eae1d"
HEADERS = {
    "Authorization": f"Bearer {STRAPI_API_TOKEN}",
    "Content-Type": "application/json"
}

VALID_STATES = ["Unassigned", "Running", "Paused", "Failed"]

# -----------------------------
# LOGGING
# -----------------------------
logging.basicConfig(
    level=print,
    format="[%(asctime)s] %(levelname)s - %(message)s"
)

# -----------------------------
# STRAPI HELPERS
# -----------------------------
def fetch_connectors():
    url = f"{STRAPI_BASE_URL}/api/connectors"
    params = {
        "populate": "connect",
        "pagination[pageSize]": 1000
    }
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json().get("data", [])


def update_connector_state(connector_id, state):
    url = f"{STRAPI_BASE_URL}/api/connectors/{connector_id}"
    payload = {"data": {"state": state}}
    resp = requests.put(url, headers=HEADERS, json=payload)
    resp.raise_for_status()
    print(f"Updated connector {connector_id} → {state}")


# -----------------------------
# MAIN LOGIC
# -----------------------------
def sync_connector_states():
    print("Starting connector state sync job")

    connectors = fetch_connectors()
    print(f"Fetched {len(connectors)} connectors from Strapi")

    if not connectors:
        print("No connectors found")
        return

    for connector in connectors:
        connector_id = connector["id"]
        attrs = connector.get("attributes", {})
        name = attrs.get("name")
        current_state = attrs.get("state")

        print(f"Processing connector: {name}")

        # Skip Unassigned & SoftDelete
        if current_state in ["Unassigned", "SoftDelete"]:
            continue

        connect = attrs.get("connect", {}).get("data")
        connect_attrs = connect.get("attributes", {}) if connect else {}

        host = connect_attrs.get("host")
        port = connect_attrs.get("port")

        if not host or not port:
            logging.warning(f"{name}: missing host/port")
            continue

        base_url = f"http://{host}:{port}"
        status_url = f"{base_url}/connectors/{name}/status"

        try:
            resp = requests.get(status_url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            kafka_state = data["connector"]["state"].capitalize()

            task_states = [t["state"] for t in data.get("tasks", [])]
            if "FAILED" in task_states:
                kafka_state = "Failed"

            if kafka_state not in VALID_STATES:
                kafka_state = "Unassigned"

            if kafka_state != current_state:
                print(f"{name}: {current_state} → {kafka_state}")
                update_connector_state(connector_id, kafka_state)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logging.warning(f"{name}: not found in Kafka")
                update_connector_state(connector_id, "Unassigned")
            else:
                logging.error(f"{name}: HTTP error {e}")

        except Exception as e:
            logging.error(f"{name}: error {e}", exc_info=True)

    print("Connector state sync job finished")


# -----------------------------
# ENTRY POINT
# -----------------------------
if __name__ == "__main__":
    print("Job started at %s", datetime.utcnow())
    sync_connector_states()
    print("Job completed")
