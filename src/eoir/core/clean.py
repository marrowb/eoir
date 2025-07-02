"""Core CSV cleaning functionality for EOIR FOIA data."""

import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import psycopg
import structlog

from eoir.core.csv import CleanCsv
from eoir.core.db import get_connection, get_latest_download
from eoir.settings import DOWNLOAD_DIR, JSON_DIR

logger = structlog.get_logger()


def build_postfix() -> str:
    """Generate table postfix from latest download metadata (MM_YY format)."""
    latest_download_metadata = get_latest_download()
    if not latest_download_metadata:
        postfix = datetime.now()
    else:
        postfix = latest_download_metadata.last_modified

    return postfix.strftime("%m_%y")


def get_download_dir(user_path: Optional[str] = None) -> Path:
    """Find CSV files directory, preferring user path over latest download."""
    if user_path:
        path = Path(user_path)
        if not path.exists():
            raise FileNotFoundError(f"Specified path does not exist: {user_path}")
        if not path.is_dir():
            raise FileNotFoundError(f"Specified path is not a directory: {user_path}")
        return path

    metadata = get_latest_download()
    if metadata:
        dated_dir = DOWNLOAD_DIR / f"{metadata.last_modified:%m%d%y}-FOIA-TRAC-FILES"
        if dated_dir.exists() and dated_dir.is_dir():
            return dated_dir

    if DOWNLOAD_DIR.exists():
        dated_folders = [
            d
            for d in DOWNLOAD_DIR.iterdir()
            if d.is_dir() and "FOIA-TRAC-FILES" in d.name
        ]
        if dated_folders:
            latest_folder = max(dated_folders, key=lambda x: x.stat().st_mtime)
            return latest_folder

    raise FileNotFoundError(
        f"No CSV files directory found. Please check {DOWNLOAD_DIR} or specify --path"
    )


def get_csv_files(directory: Path) -> List[Path]:
    """Get main CSV files from directory, excluding lookups."""
    csv_files = []

    with open(f"{JSON_DIR}/tables.json", "r") as f:
        tables_map = json.load(f)

    for file_path in directory.glob("*.csv"):
        # Skip lookup files and other non-main CSV files
        parent_name = file_path.parent.name.lower()
        if parent_name != "lookup" and file_path.name in tables_map.keys():
            csv_files.append(file_path)

    return sorted(csv_files)


def check_for_null_bytes(directory: Path) -> bool:
    """Check if any CSV file in directory contains null bytes."""
    csv_files = list(directory.glob("*.csv"))
    if not csv_files:
        return False

    # Check first available CSV file
    sample_file = csv_files[0]
    chunk_size = 1024 * 1024  # 1MB chunks

    try:
        with open(sample_file, "rb") as f:
            while chunk := f.read(chunk_size):
                if b"\x00" in chunk:
                    logger.info(f"Found null bytes in {sample_file.name}")
                    return True
        return False
    except Exception as e:
        logger.warning(f"Could not check {sample_file} for null bytes: {e}")
        return False


def remove_null_bytes_subprocess(directory: Path, parallel_workers: int = 6) -> None:
    """Remove null bytes from all CSV files using Perl subprocess."""
    logger.info(f"Removing null bytes from CSV files in {directory}")

    cmd = f'find "{directory}" -name "*.csv" -type f | xargs -n 1 -P {parallel_workers} perl -pi -e "s/\\0//g"'

    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        raise Exception(f"Null byte removal failed: {result.stderr}")

    logger.info("Successfully removed null bytes from all CSV files")


def clean_single_file(csv_file: Path, postfix: str) -> Dict:
    """Clean and load CSV file to database, returning processing results."""
    try:
        _csv = CleanCsv(str(csv_file))

        print(f"Copying {os.path.abspath(csv_file)} to table {_csv.table}_{postfix}")

        _csv.copy_to_table(postfix)

        rows_copied = _csv.row_count - _csv.empty_pk
        print(
            f"Copied {rows_copied} of {_csv.row_count} rows to {_csv.table}_{postfix}"
        )
        print(f"There were {_csv.empty_pk} rows with no primary keys")

        return {
            "csv_file": str(csv_file),
            "table_name": f"{_csv.table}_{postfix}",
            "rows_processed": _csv.row_count,
            "rows_loaded": rows_copied,
            "empty_primary_keys": _csv.empty_pk,
            "file_size_mb": csv_file.stat().st_size / (1024 * 1024),
            "success": True,
        }

    except Exception as e:
        logger.error(f"Error processing {csv_file}: {e}")
        return {
            "csv_file": str(csv_file),
            "table_name": f"unknown_{postfix}",
            "success": False,
            "error": str(e),
            "rows_processed": 0,
            "rows_loaded": 0,
            "empty_primary_keys": 0,
        }
