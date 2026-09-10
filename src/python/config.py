
import os
from pathlib import Path

# Project root = two folders up from this file (python -> src -> root)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DIR = PROJECT_ROOT / "data" / "raw"
JAR_PATH = PROJECT_ROOT / "jars" / "postgresql-42.7.3.jar"

# Read from environment if set, otherwise use the local default.
# Docker sets PG_HOST; your laptop doesn't, so it gets "localhost".
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ.get("PG_DB", "movielens")
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "Asr@1699#")

# The connection string Java uses.
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}?reWriteBatchedInserts=true"

# Settings Spark passes to the Java driver on every write.
JDBC_PROPS = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
    "batchsize": "20000",
}