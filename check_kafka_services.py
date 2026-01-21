# full_service_health_checker.py

import socket
from pathlib import Path
from dotenv import load_dotenv
import os
import mysql.connector as mysql_connector
import requests

# -----------------------
# Load environment variables
# -----------------------
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

DATABASE_HOST = os.getenv("DATABASE_HOST", "localhost")
DATABASE_PORT = int(os.getenv("DATABASE_PORT", 3306))
DATABASE_NAME = os.getenv("DATABASE_NAME", "km_2")
DATABASE_USERNAME = os.getenv("DATABASE_USERNAME", "admin")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD", "Admin123!")

# -----------------------
# Simple TCP check
# -----------------------
def check_tcp(host: str, port: int, timeout: int = 5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False

# -----------------------
# Generic DB table check
# -----------------------
def check_table(table_name: str, status_column: str):
    """
    Fetch rows from a table, check TCP connectivity, and update status.
    """
    try:
        conn = mysql_connector.connect(
            host=DATABASE_HOST,
            port=DATABASE_PORT,
            user=DATABASE_USERNAME,
            password=DATABASE_PASSWORD,
            database=DATABASE_NAME
        )
        cursor = conn.cursor(dictionary=True)

        cursor.execute(f"SELECT id, name, host, port FROM {table_name}")
        rows = cursor.fetchall()

        if not rows:
            print(f"No rows found in table {table_name}")
            return

        for row in rows:
            host, port = row["host"], int(row["port"])
            row_id, name = row["id"], row["name"]

            status = "Up" if check_tcp(host, port) else "Down"
            print(f"[{status}] {table_name[:-1].capitalize()} {name} ({host}:{port})")

            # Update the status in the DB
            cursor.execute(
                f"UPDATE {table_name} SET {status_column} = %s WHERE id = %s",
                (status, row_id)
            )
            conn.commit()

    except Exception as e:
        print(f"Failed to check {table_name}: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# -----------------------
# Cluster REST API check
# -----------------------
def check_clusters_via_rest_api(timeout=5):
    """
    Fetch clusters from DB, call their REST API, check brokers, update status.
    """
    try:
        conn = mysql_connector.connect(
            host=DATABASE_HOST,
            port=DATABASE_PORT,
            user=DATABASE_USERNAME,
            password=DATABASE_PASSWORD,
            database=DATABASE_NAME
        )
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT id, cluster_id, rest_api_url FROM clusters WHERE rest_api_url IS NOT NULL")
        clusters = cursor.fetchall()

        if not clusters:
            print("No clusters with rest_api_url found")
            return

        for cluster in clusters:
            try:
                url = f"{cluster['rest_api_url']}/v3/clusters/{cluster['cluster_id']}/brokers"
                resp = requests.get(url, timeout=timeout)
                resp.raise_for_status()
                brokers = resp.json().get("data", [])

                for broker in brokers:
                    host, port = broker["host"], broker["port"]
                    status = "Up" if check_tcp(host, port, timeout) else "Down"
                    print(f"[{status}] Broker {host}:{port}")

                    cursor.execute(
                        "UPDATE brokers SET broker_status=%s WHERE host=%s AND port=%s",
                        (status, host, port)
                    )
                    conn.commit()

            except Exception as e:
                print(f"Failed to fetch brokers from {cluster['rest_api_url']}: {e}")

    except Exception as e:
        print(f"Failed to fetch clusters: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# -----------------------
# Main runner
# -----------------------
def run_all_checks():
    print("Starting broker checks...")
    check_table("brokers", "broker_status")

    print("Starting schema registry checks...")
    check_table("schema_registries", "registry_status")

    print("Starting connect checks...")
    check_table("connects", "connect_status")

    print("Starting cluster REST API checks...")
    check_clusters_via_rest_api()
    
    print("All health checks completed!")

# -----------------------
# Execute when run directly
# -----------------------
if __name__ == "__main__":
    run_all_checks()
