"""Small Postgres operations from Python. Not for bulk data - Spark does that."""

import psycopg2

from config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD


def get_connection():
    """Open a connection to Postgres."""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def execute(sql, params=None):
    """Run a statement that returns nothing: CREATE, INSERT, DELETE."""
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, params)
    finally:
        conn.close()


def fetch_all(sql, params=None):
    """Run a query and return all rows."""
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


def count_rows(schema, table):
    """Return how many rows are in a table. Returns 0 if it doesn't exist."""
    rows = fetch_all(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    if rows[0][0] == 0:
        return 0

    rows = fetch_all(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
    return rows[0][0]