"""
Configuration loader for the Finance ETL pipeline.
Reads credentials and settings from environment variables or .env file.
"""
import os
from dotenv import load_dotenv, find_dotenv

# Try to find and load .env from current directory or parents
dotenv_path = find_dotenv()
if dotenv_path:
    load_dotenv(dotenv_path)
else:
    # Fallback to project root .env
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(base_dir, ".env"))

# Is running inside Airflow/Docker?
IS_DOCKER_OR_AIRFLOW = bool(
    os.getenv("AIRFLOW_HOME") or os.getenv("DOCKER_CONTAINER") or os.path.exists("/.dockerenv")
)

# Host resolution: inside Docker, Windows host is host.docker.internal
DEFAULT_HOST = "host.docker.internal" if IS_DOCKER_OR_AIRFLOW else "localhost"
PG_HOST = os.getenv("PG_HOST", DEFAULT_HOST)
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DATABASE = os.getenv("PG_DATABASE", "finance_db")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "1234")

# Airflow host fallback
AIRFLOW_PG_HOST = os.getenv("AIRFLOW_PG_HOST", "host.docker.internal")

# Stock symbols to track
SYMBOLS = ["AAPL", "MSFT", "GOOGL", "NVDA", "AMZN"]

# Yahoo Finance download period
HISTORY_PERIOD = "1y"


def get_pg_connection_string(use_airflow_host: bool = False) -> str:
    """Build a SQLAlchemy connection string for PostgreSQL."""
    host = AIRFLOW_PG_HOST if (use_airflow_host or IS_DOCKER_OR_AIRFLOW) else PG_HOST
    return f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{host}:{PG_PORT}/{PG_DATABASE}"


def get_pg_connection_params(use_airflow_host: bool = False) -> dict:
    """Return psycopg2.connect() keyword arguments."""
    host = AIRFLOW_PG_HOST if (use_airflow_host or IS_DOCKER_OR_AIRFLOW) else PG_HOST
    return {
        "host": host,
        "port": PG_PORT,
        "dbname": PG_DATABASE,
        "user": PG_USER,
        "password": PG_PASSWORD,
    }
