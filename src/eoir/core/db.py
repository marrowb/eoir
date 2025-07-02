"""Database operations."""

from contextlib import contextmanager
from datetime import datetime
from typing import Optional

import psycopg

from eoir_foia.core.db_utils import db_operation
from eoir_foia.core.models import FileMetadata
from eoir_foia.settings import ADMIN_URL, DATABASE_URL, pg_db


@contextmanager
def get_db_connection():
    """Get a database connection."""
    conn = None
    try:
        conn = psycopg.connect(conninfo=DATABASE_URL)
        yield conn.cursor()
    finally:
        if conn:
            conn.commit()
            conn.close()


def get_connection():
    """Get a direct database connection (caller responsible for closing)."""
    return psycopg.connect(conninfo=DATABASE_URL)


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


@db_operation
def create_database():
    """Create database if it doesn't exist."""
    # Try connecting to target database first
    with get_db_connection():
        return False

    # If we get here, database doesn't exist, so create it
    with get_admin_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {pg_db}")
    return True


@db_operation
def init_download_tracking():
    """Initialize download tracking table."""
    with get_db_connection() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS eoir_foia_download_history (
                id SERIAL PRIMARY KEY,
                download_date TIMESTAMP NOT NULL,
                content_length BIGINT NOT NULL,
                last_modified TIMESTAMP NOT NULL,
                etag TEXT NOT NULL,
                local_path TEXT NOT NULL,
                status TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS eoir_foia_download_history_id_idx ON eoir_foia_download_history(id);
        """
        )


@db_operation
def get_latest_download() -> Optional[FileMetadata]:
    """Get most recent successful download record."""
    with get_db_connection() as cur:
        cur.execute(
            """
            SELECT content_length, last_modified, etag, local_path
            FROM eoir_foia_download_history 
            WHERE status = 'completed'
            ORDER BY download_date DESC 
            LIMIT 1
        """
        )
        result = cur.fetchone()
        if result:
            return FileMetadata(
                content_length=result[0],
                last_modified=result[1],
                etag=result[2],
                local_path=result[3],
            )
    return None


@db_operation
def record_download_in_history(
    content_length: int,
    last_modified: datetime,
    etag: str,
    local_path: str,
    status: str,
):
    """Record a download attempt."""
    with get_db_connection() as cur:
        cur.execute(
            """
            INSERT INTO eoir_foia_download_history 
            (download_date, content_length, last_modified, 
             etag, local_path, status)
            VALUES (NOW(), %s, %s, %s, %s, %s)
        """,
            (content_length, last_modified, etag, local_path, status),
        )
