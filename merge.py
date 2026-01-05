from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

from server_checker import check_server
from airflow.providers.mysql.hooks.mysql import MySqlHook
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import requests

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


class ServiceHealthChecker:
    STATUS_COLUMNS = {
        "brokers": "broker_status",
        "connects": "connect_status",
        "schema_registries": "registry_status",
    }

    def __init__(self, mysql_conn_id="my_sql_conn", timeout=5, max_workers=10):
        self.hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        self.timeout = timeout
        self.max_workers = max_workers

    # ---------------- Generic DB checks ---------------- #
    def fetch_rows(self, table_name):
        return self.hook.get_pandas_df(f"SELECT id, name, host, port FROM {table_name}")

    def update_status(self, table_name, row_id, status):
        column = self.STATUS_COLUMNS[table_name]
        query = f"UPDATE {table_name} SET {column}=%s WHERE id=%s"
        self.hook.run(query, parameters=(status, row_id))
        logging.info(f"{table_name} ID {row_id} → {status}")

    def check_single(self, row, table_name):
        host, port = row["host"], int(row["port"])
        row_id, name = row["id"], row["name"]

        status = "Up" if check_server(host, port, self.timeout) else "Down"
        logging.info(f"[{status}] {name} ({host}:{port})")
        self.update_status(table_name, row_id, status)

    def check_all(self, table_name):
        df = self.fetch_rows(table_name)
        if df.empty:
            logging.warning(f"No rows in {table_name}")
            return

        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(df))) as executor:
            executor.map(lambda r: self.check_single(r, table_name), [row for _, row in df.iterrows()])

    # ---------------- Cluster REST API checks ---------------- #
    def check_brokers_via_rest_api(self):
        clusters = self.hook.get_pandas_df(
            "SELECT id, cluster_id, rest_api_url FROM clusters WHERE rest_api_url IS NOT NULL"
        )
        if clusters.empty:
            logging.warning("No clusters with rest_api_url found")
            return

        def check_cluster(cluster):
            try:
                resp = requests.get(f"{cluster['rest_api_url']}/v3/clusters/{cluster['cluster_id']}/brokers",
                                    timeout=self.timeout)
                resp.raise_for_status()
                brokers = resp.json().get("data", [])
                for broker in brokers:
                    host, port = broker["host"], broker["port"]
                    status = "Up" if check_server(host, port, self.timeout) else "Down"
                    self.hook.run(
                        "UPDATE brokers SET broker_status=%s WHERE host=%s AND port=%s",
                        parameters=(status, host, port)
                    )
                    logging.info(f"[{status}] Broker {host}:{port}")
            except Exception as e:
                logging.error(f"Failed to fetch brokers from {cluster['rest_api_url']}: {e}")

        # Check all clusters in parallel
        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(clusters))) as executor:
            executor.map(lambda r: check_cluster(r[1]), clusters.iterrows())


# ---------------- DAG functions ---------------- #
def run_health_check(table_name, **_):
    checker = ServiceHealthChecker()
    checker.check_all(table_name)

def run_cluster_check(**_):
    checker = ServiceHealthChecker()
    checker.check_brokers_via_rest_api()


# ---------------- DAG ---------------- #
with DAG(
    dag_id="health_check_kafka_services",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["monitoring"],
) as dag:

    check_brokers = PythonOperator(
        task_id="check_brokers",
        python_callable=run_health_check,
        op_kwargs={"table_name": "brokers"},
    )

    check_schema_registries = PythonOperator(
        task_id="check_schema_registries",
        python_callable=run_health_check,
        op_kwargs={"table_name": "schema_registries"},
    )

    check_connects = PythonOperator(
        task_id="check_connects",
        python_callable=run_health_check,
        op_kwargs={"table_name": "connects"},
    )

    check_clusters = PythonOperator(
        task_id="check_clusters",
        python_callable=run_cluster_check,
    )

    check_brokers >> check_schema_registries >> check_connects >> check_clusters
