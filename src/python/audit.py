"""Ingestion log. Records every file loaded, and prevents duplicate loads."""

from db import execute, fetch_all

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS audit.ingestion_log (
    log_id      BIGSERIAL PRIMARY KEY,
    source_name TEXT        NOT NULL,
    file_name   TEXT,
    row_count   BIGINT,
    write_mode  TEXT,
    status      TEXT        NOT NULL,
    message     TEXT,
    load_tstmp  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def create_log_table():
    """Make the log table if it isn't there. Safe to call every run."""
    execute(CREATE_TABLE)


def log_load(source_name, file_name, row_count, write_mode, status, message=None):
    """Write one row into the log."""
    execute(
        """
        INSERT INTO audit.ingestion_log
            (source_name, file_name, row_count, write_mode, status, message)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (source_name, file_name, row_count, write_mode, status, message),
    )


def get_loaded_files(source_name):
    """
    Return the file names already loaded successfully for this source.

    This is what stops duplicates. Before loading a file, we check
    whether it's in here. If yes, we skip it.
    """
    rows = fetch_all(
        """
        SELECT DISTINCT file_name FROM audit.ingestion_log
        WHERE source_name = %s AND status = 'SUCCESS' AND file_name IS NOT NULL
        """,
        (source_name,),
    )
    return {row[0] for row in rows}