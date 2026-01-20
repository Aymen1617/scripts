from datetime import datetime, timedelta
import os
import shutil
import logging
import subprocess
from zipfile import ZipFile, ZIP_DEFLATED
from dotenv import load_dotenv

# -----------------------------
# LOAD ENV
# -----------------------------
load_dotenv()  # Load environment variables from .env

DB_HOST = os.getenv("DATABASE_HOST", "localhost")
DB_PORT = os.getenv("DATABASE_PORT", "3306")
DB_USER = os.getenv("DATABASE_USERNAME", "admin")
DB_PASSWORD = os.getenv("DATABASE_PASSWORD", "Admin123!")
DB_NAME = os.getenv("DATABASE_NAME", "strapi")

BACKUP_DIR = os.getenv("DB_BACKUP_DIR", "/opt/airflow/backups")
UPLOADS_DIR = os.getenv("UPLOADS_DIR", "/opt/airflow/strapi/public/uploads")

RETENTION_DAYS = 30

# -----------------------------
# LOGGING
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s"
)

# -----------------------------
# HELPERS
# -----------------------------
def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def backup_mysql():
    logging.info("Starting MySQL backup")

    today = datetime.now().strftime("%Y-%m-%d")
    logging.info(f"Today's date: {today}")
    backup_path = os.path.join(BACKUP_DIR, today)
    ensure_dir(backup_path)

    sql_file = os.path.join(backup_path, "backup.sql")
    logging.info(f"SQL backup file path: {sql_file}")

    cmd = [
        "mysqldump",
        "-h", DB_HOST,
        "-P", DB_PORT,
        "-u", DB_USER,
        f"-p{DB_PASSWORD}",
        DB_NAME
    ]
    logging.info(f"Running command: {' '.join(cmd)}")
    with open(sql_file, "w") as f:
        subprocess.run(cmd, stdout=f, check=True)

    logging.info(f"MySQL backup created at {sql_file}")


def zip_uploads():
    logging.info("Starting uploads backup")

    today = datetime.now().strftime("%Y-%m-%d")
    backup_path = os.path.join(BACKUP_DIR, today)
    ensure_dir(backup_path)

    zip_path = os.path.join(backup_path, "uploads.zip")

    with ZipFile(zip_path, "w", ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(UPLOADS_DIR):
            for file in files:
                full_path = os.path.join(root, file)
                arcname = os.path.relpath(full_path, UPLOADS_DIR)
                zipf.write(full_path, arcname)

    logging.info(f"Uploads zipped at {zip_path}")


def cleanup_old_backups():
    logging.info("Cleaning old backups")

    now = datetime.now()
    if not os.path.exists(BACKUP_DIR):
        logging.warning(f"Backup dir {BACKUP_DIR} does not exist, skipping cleanup")
        return

    for folder in os.listdir(BACKUP_DIR):
        folder_path = os.path.join(BACKUP_DIR, folder)

        if not os.path.isdir(folder_path):
            continue

        try:
            folder_date = datetime.strptime(folder, "%Y-%m-%d")
        except ValueError:
            continue

        age = (now - folder_date).days
        if age > RETENTION_DAYS:
            shutil.rmtree(folder_path)
            logging.info(f"Deleted old backup folder: {folder_path}")


# -----------------------------
# ENTRY POINT
# -----------------------------
if __name__ == "__main__":
    logging.info("Backup job started")
    try:
        backup_mysql()
        zip_uploads()
        cleanup_old_backups()
        logging.info("Backup job finished successfully")
    except Exception as e:
        logging.error(f"Backup job failed: {e}", exc_info=True)
