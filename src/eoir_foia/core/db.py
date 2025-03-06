"""Database operations."""
from contextlib import contextmanager
import psycopg
from datetime import datetime
from typing import Optional
from eoir_foia.settings import DATABASE_URL
from eoir_foia.settings import ADMIN_URL
from eoir_foia.core.models import FileMetadata

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

@contextmanager
def get_admin_connection():
    """Get an admin connection to create database."""
    conn = None
    try:
        conn = psycopg.connect(conninfo=ADMIN_URL)
        conn.autocommit = True
        yield conn
    finally:
        if conn:
            conn.close()

def create_database():
    """Create database if it doesn't exist."""
    try:
        # Try connecting to target database first
        with get_db_connection():
            return False
    except psycopg.OperationalError:
        # Database doesn't exist, create it
        with get_admin_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE DATABASE {pg_db}")
        return True

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
                );
                CREATE INDEX IF NOT EXISTS download_history_id_idx ON download_history(id);
            """)
        conn.commit()

def get_latest_download() -> Optional[FileMetadata]:
    """Get most recent successful download record."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT content_length, last_modified, etag
                FROM download_history 
                WHERE status = 'completed'
                ORDER BY download_date DESC 
                LIMIT 1
            """)
            result = cur.fetchone()
            if result:
                return FileMetadata(
                    content_length=result[0],
                    last_modified=result[1],
                    etag=result[2]
                )
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
