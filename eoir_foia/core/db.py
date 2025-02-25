"""Database operations."""
from contextlib import contextmanager
import psycopg
from datetime import datetime
from typing import Optional
from eoir_foia.settings import DATABASE_URL

@contextmanager
def get_db_connection():
    """Get a database connection."""
    conn = None
    try:
        conn = psycopg.connect(conninfo=DATABASE_URL)
        yield conn
    finally:
        if conn:
            conn.close()

def init_download_tracking():
    """Initialize download tracking table."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS download_history (
                    id SERIAL PRIMARY KEY,
                    download_date TIMESTAMP NOT NULL,
                    content_length BIGINT NOT NULL,
                    last_modified TIMESTAMP NOT NULL,
                    etag TEXT NOT NULL,
                    local_path TEXT NOT NULL,
                    status TEXT NOT NULL
                )
            """)
        conn.commit()

def get_latest_download() -> Optional[dict]:
    """Get most recent download record."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM download_history 
                ORDER BY download_date DESC 
                LIMIT 1
            """)
            result = cur.fetchone()
            if result:
                return dict(zip(
                    ['id', 'download_date', 'content_length', 
                     'last_modified', 'etag', 'local_path', 'status'],
                    result
                ))
    return None

def record_download(
    content_length: int,
    last_modified: datetime,
    etag: str,
    local_path: str,
    status: str
):
    """Record a download attempt."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO download_history 
                (download_date, content_length, last_modified, 
                 etag, local_path, status)
                VALUES (NOW(), %s, %s, %s, %s, %s)
            """, (content_length, last_modified, etag, local_path, status))
        conn.commit()
