# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# import logging

# from server_checker import check_server
# from airflow.providers.mysql.hooks.mysql import MySqlHook
# from concurrent.futures import ThreadPoolExecutor
# import pandas as pd
# import requests

# DEFAULT_ARGS = {
#     "owner": "airflow",
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }


# class ServiceHealthChecker:
#     STATUS_COLUMNS = {
#         "brokers": "broker_status",
#         "connects": "connect_status",
#         "schema_registries": "registry_status",
#     }

#     def __init__(self, mysql_conn_id="my_sql_conn", timeout=5, max_workers=10):
#         self.hook = MySqlHook(mysql_conn_id=mysql_conn_id)
#         self.timeout = timeout
#         self.max_workers = max_workers

#     # ---------------- Generic DB checks ---------------- #
#     def fetch_rows(self, table_name):
#         return self.hook.get_pandas_df(f"SELECT id, name, host, port FROM {table_name}")

#     def update_status(self, table_name, row_id, status):
#         column = self.STATUS_COLUMNS[table_name]
#         query = f"UPDATE {table_name} SET {column}=%s WHERE id=%s"
#         self.hook.run(query, parameters=(status, row_id))
#         logging.info(f"{table_name} ID {row_id} → {status}")

#     def check_single(self, row, table_name):
#         host, port = row["host"], int(row["port"])
#         row_id, name = row["id"], row["name"]

#         status = "Up" if check_server(host, port, self.timeout) else "Down"
#         logging.info(f"[{status}] {name} ({host}:{port})")
#         self.update_status(table_name, row_id, status)

#     def check_all(self, table_name):
#         df = self.fetch_rows(table_name)
#         if df.empty:
#             logging.warning(f"No rows in {table_name}")
#             return

#         with ThreadPoolExecutor(max_workers=min(self.max_workers, len(df))) as executor:
#             executor.map(lambda r: self.check_single(r, table_name), [row for _, row in df.iterrows()])

#     # ---------------- Cluster REST API checks ---------------- #
#     def check_brokers_via_rest_api(self):
#         clusters = self.hook.get_pandas_df(
#             "SELECT id, cluster_id, rest_api_url FROM clusters WHERE rest_api_url IS NOT NULL"
#         )
#         if clusters.empty:
#             logging.warning("No clusters with rest_api_url found")
#             return

#         def check_cluster(cluster):
#             try:
#                 resp = requests.get(f"{cluster['rest_api_url']}/v3/clusters/{cluster['cluster_id']}/brokers",
#                                     timeout=self.timeout)
#                 resp.raise_for_status()
#                 brokers = resp.json().get("data", [])
#                 for broker in brokers:
#                     host, port = broker["host"], broker["port"]
#                     status = "Up" if check_server(host, port, self.timeout) else "Down"
#                     self.hook.run(
#                         "UPDATE brokers SET broker_status=%s WHERE host=%s AND port=%s",
#                         parameters=(status, host, port)
#                     )
#                     logging.info(f"[{status}] Broker {host}:{port}")
#             except Exception as e:
#                 logging.error(f"Failed to fetch brokers from {cluster['rest_api_url']}: {e}")

#         # Check all clusters in parallel
#         with ThreadPoolExecutor(max_workers=min(self.max_workers, len(clusters))) as executor:
#             executor.map(lambda r: check_cluster(r[1]), clusters.iterrows())


# # ---------------- DAG functions ---------------- #
# def run_health_check(table_name, **_):
#     checker = ServiceHealthChecker()
#     checker.check_all(table_name)

# def run_cluster_check(**_):
#     checker = ServiceHealthChecker()
#     checker.check_brokers_via_rest_api()


# # ---------------- DAG ---------------- #
# with DAG(
#     dag_id="health_check_kafka_services",
#     default_args=DEFAULT_ARGS,
#     start_date=datetime(2025, 1, 1),
#     schedule="@hourly",
#     catchup=False,
#     tags=["monitoring"],
# ) as dag:

#     check_brokers = PythonOperator(
#         task_id="check_brokers",
#         python_callable=run_health_check,
#         op_kwargs={"table_name": "brokers"},
#     )

#     check_schema_registries = PythonOperator(
#         task_id="check_schema_registries",
#         python_callable=run_health_check,
#         op_kwargs={"table_name": "schema_registries"},
#     )

#     check_connects = PythonOperator(
#         task_id="check_connects",
#         python_callable=run_health_check,
#         op_kwargs={"table_name": "connects"},
#     )

#     check_clusters = PythonOperator(
#         task_id="check_clusters",
#         python_callable=run_cluster_check,
#     )

#     check_brokers >> check_schema_registries >> check_connects >> check_clusters
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.strapi_helper import fetch_by_doc, strapi_get, get_client

# -----------------------------
# Snapshot Retry Logic
# -----------------------------
def run_incremental_snapshot(snapshot):
    """Run incremental snapshot."""
    if snapshot["snapshotStatus"] == "done":
        logging.info(f"[Snapshot {snapshot['id']}] Already done, skipping")
        return

    try:
        # Placeholder for query generation
        query = f"SELECT * FROM {snapshot['tableName']} WHERE updated_at > last_run_time()"
        client = get_client()
        client.update_entry("snapshot", snapshot["id"], {"query": query, "snapshotStatus": "processing"})

        # Placeholder for actual snapshot execution
        logging.info(f"[Snapshot {snapshot['id']}] Executing incremental snapshot...")
        # run_snapshot(snapshot["id"]) # implement if needed

        client.update_entry("snapshot", snapshot["id"], {"snapshotStatus": "done"})
        logging.info(f"[Snapshot {snapshot['id']}] Incremental snapshot completed successfully")
    except Exception as e:
        logging.error(f"[Snapshot {snapshot['id']}] Incremental snapshot failed: {e}")
        client.update_entry("snapshot", snapshot["id"], {"snapshotStatus": "error"})


def run_non_incremental_snapshot(snapshot):
    """Run non-incremental snapshot."""
    try:
        query = f"SELECT * FROM {snapshot['tableName']}"  # Placeholder
        client = get_client()
        client.update_entry(
            "snapshot",
            snapshot["id"],
            {
                "query": query,
                "snapshotStatus": "done",
                "endAt": datetime.utcnow().isoformat(),
            },
        )
        logging.info(f"[Snapshot {snapshot['id']}] Non-incremental snapshot completed successfully")
    except Exception as e:
        logging.error(f"[Snapshot {snapshot['id']}] Non-incremental snapshot failed: {e}")
        client.update_entry("snapshot", snapshot["id"], {"snapshotStatus": "error"})


def snapshot_retry_job():
    """Check Strapi for active snapshots and execute them."""
    logging.info("[Snapshot Retry] Checking for active snapshots...")
    now = datetime.utcnow()
    pending_snapshots = strapi_get(
        "snapshot",
        filters={"isActive": True, "snapshotStatus": "pending"},
        populate=[
            "connector",
            "connector.database_instance",
            "connector.tables",
            "connector.connector_template",
            "database_instance_source",
            "database_instance_sink",
            "database_instance_child",
        ],
    )

    if not pending_snapshots:
        logging.info("[Snapshot Retry] No active snapshots found.")
        return

    ready_snapshots = [
        s for s in pending_snapshots
        if not s.get("scheduledAt") or datetime.fromisoformat(s["scheduledAt"]) <= now
    ]
    logging.info(f"[Snapshot Retry] Found {len(ready_snapshots)} snapshots ready to execute")

    for snapshot in ready_snapshots:
        # Check conflicts
        same_table = strapi_get(
            "snapshot",
            filters={
                "id": {"$ne": snapshot["id"]},
                "tableName": snapshot["tableName"],
                "snapshotStatus": {"$in": ["processing"]},
                "isActive": True,
            },
        )
        if same_table:
            logging.warning(f"[Snapshot {snapshot['id']}] Skipping due to conflict on table {snapshot['tableName']}")
            continue

        if snapshot["type"] == "Incremental":
            run_incremental_snapshot(snapshot)
        elif snapshot["type"] == "Non-Incremental":
            run_non_incremental_snapshot(snapshot)
        else:
            logging.warning(f"[Snapshot {snapshot['id']}] Unknown snapshot type: {snapshot['type']}")


# -----------------------------
# Airflow DAG
# -----------------------------
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="snapshot_retry_dag",
    default_args=DEFAULT_ARGS,
    description="Retry snapshots from Strapi using existing Strapi client",
    schedule="@hourly",
    start_date=datetime(2026, 1, 5),
    catchup=False,
    max_active_runs=1,
    tags=["snapshot", "strapi"],
) as dag:

    run_snapshots = PythonOperator(
        task_id="run_snapshot_retry",
        python_callable=snapshot_retry_job,
    )

    run_snapshots

