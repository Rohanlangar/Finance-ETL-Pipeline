"""Test PostgreSQL connectivity and create finance_db if it doesn't exist."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

load_dotenv()

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE", "finance_db")


def test_connection():
    """Connect to default 'postgres' DB, then create finance_db if needed."""
    print(f"[1/3] Connecting to PostgreSQL at {PG_HOST}:{PG_PORT} as '{PG_USER}'...")
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT,
            user=PG_USER, password=PG_PASSWORD,
            dbname="postgres",
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        print("  [OK] Connected to PostgreSQL successfully!")
    except Exception as e:
        print(f"  [FAIL] Connection failed: {e}")
        sys.exit(1)

    # Check / create finance_db
    print(f"\n[2/3] Checking if database '{PG_DATABASE}' exists...")
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (PG_DATABASE,))
    exists = cur.fetchone()

    if exists:
        print(f"  [OK] Database '{PG_DATABASE}' already exists.")
    else:
        print(f"  [INFO] Creating database '{PG_DATABASE}'...")
        cur.execute(f'CREATE DATABASE "{PG_DATABASE}"')
        print(f"  [OK] Database '{PG_DATABASE}' created.")

    cur.close()
    conn.close()

    # Verify connection to finance_db
    print(f"\n[3/3] Verifying connection to '{PG_DATABASE}'...")
    try:
        conn2 = psycopg2.connect(
            host=PG_HOST, port=PG_PORT,
            user=PG_USER, password=PG_PASSWORD,
            dbname=PG_DATABASE,
        )
        cur2 = conn2.cursor()
        cur2.execute("SELECT version();")
        version = cur2.fetchone()[0]
        print(f"  [OK] Connected to '{PG_DATABASE}'!")
        print(f"  PostgreSQL version: {version}")
        cur2.close()
        conn2.close()
    except Exception as e:
        print(f"  [FAIL] Failed to connect to '{PG_DATABASE}': {e}")
        sys.exit(1)

    print("\nAll checks passed! Ready to build the ETL pipeline.")


if __name__ == "__main__":
    test_connection()
