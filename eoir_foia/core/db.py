"""Database operations."""
from contextlib import contextmanager
import psycopg

@contextmanager
def get_db_connection():
    """Get a database connection."""
    conn = None
    try:
        conn = psycopg.connect(conninfo="")  # Connection string from settings
        yield conn
    finally:
        if conn:
            conn.close()
