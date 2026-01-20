
import logging
import requests
import json
from datetime import datetime

# ==========================
# STRAPI CONFIG
# ==========================

STRAPI_BASE_URL = "http://localhost:1337"
STRAPI_API_TOKEN = "d0183c684eef0a77fc4b8998e570ba14f511c08a93acd2b3e6e1d27544a8de88a8355cd0d205d01d18f64e5dc648ecd8d4f1b5bbc729b345ac453373aaa887abd7f790097c84515d24b16520f892406d2518c0fcd53a9fff3dcffc52923d66bd40a115e56d59995e08573a2185798306ab470de98dc5fac4364729ed0a4eae1d"

HEADERS = {
    "Authorization": f"Bearer {STRAPI_API_TOKEN}",
    "Content-Type": "application/json"
}

# ==========================
# STRAPI HELPERS
# ==========================

def fetch_pending_snapshots():
    url = (
        f"{STRAPI_BASE_URL}/api/snapshots?"
        f"filters[isActive][$eq]=true&"
        f"filters[snapshotStatus][$eq]=pending&"
        f"populate=*"
    )
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json().get("data", [])


def update_snapshot_status(snapshot_id, status):
    url = f"{STRAPI_BASE_URL}/api/snapshots/{snapshot_id}"
    payload = {"data": {"snapshotStatus": status}}
    requests.put(url, headers=HEADERS, json=payload).raise_for_status()


def update_snapshot_time(snapshot_id, start, end):
    url = f"{STRAPI_BASE_URL}/api/snapshots/{snapshot_id}"
    payload = {"data": {"startAt": start, "endAt": end}}
    requests.put(url, headers=HEADERS, json=payload).raise_for_status()


def save_snapshot_query(snapshot_id, query):
    url = f"{STRAPI_BASE_URL}/api/snapshots/{snapshot_id}"
    payload = {"data": {"query": query}}
    requests.put(url, headers=HEADERS, json=payload).raise_for_status()


def get_connector(db_id, table_name):
    url = f"{STRAPI_BASE_URL}/api/connectors"
    params = {
        "filters[database_instance][id][$eq]": db_id,
        "filters[tables][name][$eqi]": table_name,
        "populate": "*"
    }
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    data = r.json().get("data", [])
    if not data:
        raise ValueError(f"No connector found for table {table_name}")
    return data[0]["attributes"]


def pause_sink_connector(snapshot_id, sink_db_id, table_name):
    url = f"{STRAPI_BASE_URL}/api/connectors"
    params = {
        "filters[database_instance][id][$eq]": sink_db_id,
        "filters[tables][name][$eqi]": table_name,
        "populate": "*"
    }
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()

    connector = r.json()["data"][0]
    pause_url = f"{STRAPI_BASE_URL}/api/connectors/{connector['id']}"
    requests.put(
        pause_url,
        headers=HEADERS,
        json={"data": {"state": "Paused"}}
    ).raise_for_status()

    logging.info(f"[Snapshot {snapshot_id}] Sink connector paused")


# ==========================
# CORE LOGIC
# ==========================

def generate_incremental_query(snapshot_attrs):
    table = snapshot_attrs["tableName"]
    return f"SELECT * FROM {table};"


def create_snapshot_connector(snapshot_id, base_connector, config, db_attrs, db_id, table, query):
    payload = {
        "data": {
            "name": f"snapshot_{table}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
            "database_instance": db_id,
            "extra_config": config,
            "tables": [t["id"] for t in base_connector.get("tables", {}).get("data", [])],
            "snapshot": snapshot_id,
            "isSnapshotConnector": True,
            "isActive": True
        }
    }
    url = f"{STRAPI_BASE_URL}/api/connectors"
    requests.post(url, headers=HEADERS, json=payload).raise_for_status()


# ==========================
# MAIN PIPELINE
# ==========================

def run_pipeline():
    logging.info("Starting snapshot pipeline")

    snapshots = fetch_pending_snapshots()
    if not snapshots:
        print("no snapshots found here!")
        logging.info("No pending snapshots")
        return

    start_times = {}

    # Mark processing
    for s in snapshots:
        start_times[s["id"]] = datetime.utcnow().isoformat() + "Z"
        update_snapshot_status(s["id"], "processing")

    # Process snapshots
    for s in snapshots:
        snapshot_id = s["id"]
        attrs = s["attributes"]
        table = attrs["tableName"]

        logging.info(f"[Snapshot {snapshot_id}] Processing")

        if attrs.get("type") == "Incremental":
            query = generate_incremental_query(attrs)
            save_snapshot_query(snapshot_id, query)

        else:
            sink_db = attrs["database_instance_sink"]["data"]
            pause_sink_connector(snapshot_id, sink_db["id"], table)

            query = f"SELECT * FROM {table};"
            save_snapshot_query(snapshot_id, query)

            source_db = attrs["database_instance_source"]["data"]
            sink_db = attrs["database_instance_sink"]["data"]

            source_connector = get_connector(source_db["id"], table)
            sink_connector = get_connector(sink_db["id"], table)

            create_snapshot_connector(
                snapshot_id,
                source_connector,
                source_connector.get("config", {}),
                source_db["attributes"],
                source_db["id"],
                table,
                query
            )

            create_snapshot_connector(
                snapshot_id,
                sink_connector,
                sink_connector.get("config", {}),
                sink_db["attributes"],
                sink_db["id"],
                table,
                query
            )

        update_snapshot_status(snapshot_id, "done")
        update_snapshot_time(
            snapshot_id,
            start_times[snapshot_id],
            datetime.utcnow().isoformat() + "Z"
        )

    logging.info("Snapshot pipeline finished successfully")


# ==========================
# ENTRY POINT
# ==========================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s - %(message)s"
    )
    run_pipeline()
