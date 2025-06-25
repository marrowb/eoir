"""Core CSV cleaning functionality for EOIR FOIA data."""

import json
import os
from pathlib import Path
from typing import Dict, List, Optional

import structlog

from eoir_foia.core.csv import CsvValidator, dump_counts
from eoir_foia.core.db import get_data_postfix, get_db_connection, get_latest_download
from eoir_foia.core.models import FileMetadata
from eoir_foia.settings import DOWNLOAD_DIR, JSON_DIR

logger = structlog.get_logger()


def build_postfix(metadata: Optional[FileMetadata] = None) -> str:
    """
    Generate table postfix from FileMetadata date.

    Args:
        metadata: Optional FileMetadata object. If None, gets latest download.

    Returns:
        Postfix string in MM_YY format (e.g., "06_25" for June 2025)

    Raises:
        ValueError: If no download metadata available
    """
    if metadata is None:
        # Use existing function from db.py
        return get_data_postfix()

    # Format last_modified date as MM_YY for custom metadata
    date = metadata.last_modified
    return f"{date.month:02d}_{date.year % 100:02d}"


def get_download_dir(user_path: Optional[str] = None) -> Path:
    """
    Find CSV files directory.

    Args:
        user_path: Optional user-specified directory path

    Returns:
        Path object pointing to directory containing CSV files

    Raises:
        FileNotFoundError: If directory doesn't exist or contains no CSV files
    """
    if user_path:
        path = Path(user_path)
        if not path.exists():
            raise FileNotFoundError(f"Specified path does not exist: {user_path}")
        if not path.is_dir():
            raise FileNotFoundError(f"Specified path is not a directory: {user_path}")
        return path

    # Get latest download metadata
    metadata = get_latest_download()
    if metadata:
        # Construct path from metadata (e.g., downloads/062425-FOIA-TRAC-FILES/)
        dated_dir = DOWNLOAD_DIR / f"{metadata.last_modified:%m%d%y}-FOIA-TRAC-FILES"
        if dated_dir.exists() and dated_dir.is_dir():
            return dated_dir

    # Fallback: scan DOWNLOAD_DIR for most recent dated folder
    if DOWNLOAD_DIR.exists():
        dated_folders = [
            d
            for d in DOWNLOAD_DIR.iterdir()
            if d.is_dir() and "FOIA-TRAC-FILES" in d.name
        ]
        if dated_folders:
            # Sort by modification time, newest first
            latest_folder = max(dated_folders, key=lambda x: x.stat().st_mtime)
            return latest_folder

    raise FileNotFoundError(
        f"No CSV files directory found. Please check {DOWNLOAD_DIR} or specify --path"
    )


def get_table_name(csv_filename: str, postfix: str) -> str:
    """
    Build full table name from CSV file and postfix.

    Args:
        csv_filename: Name of CSV file (e.g., "tbl_schedule.csv")
        postfix: Table postfix (e.g., "06_25")

    Returns:
        Full table name (e.g., "foia_schedule_06_25")

    Raises:
        KeyError: If CSV filename not found in tables.json mapping
    """
    tables_path = os.path.join(JSON_DIR, "tables.json")
    with open(tables_path, "r") as f:
        tables = json.load(f)

    if csv_filename not in tables:
        raise KeyError(f"CSV file '{csv_filename}' not found in tables mapping")

    base_name = tables[csv_filename]
    return f"{base_name}_{postfix}"


def validate_table_exists(table_name: str) -> bool:
    """
    Check if a table exists in the database.

    Args:
        table_name: Name of table to check

    Returns:
        True if table exists, False otherwise
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """,
                (table_name,),
            )
            return cur.fetchone()[0]


def copy_csv_to_table(validator: CsvValidator, table_name: str) -> Dict:
    """
    Load CSV data into existing PostgreSQL table using COPY.

    Args:
        validator: CsvValidator instance with loaded CSV
        table_name: Name of target database table

    Returns:
        Dictionary with load statistics and results
    """
    # Validate table exists
    if not validate_table_exists(table_name):
        raise Exception(
            f"Table '{table_name}' does not exist. Run tx.py to create tables first."
        )

    logger.info(f"Loading data into table: {table_name}")

    # Statistics tracking
    rows_processed = 0
    rows_loaded = 0
    total_modifications = 0
    empty_primary_keys = 0

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Disable triggers during bulk load for performance
            cur.execute("SET session_replication_role = replica;")

            # Use COPY FROM STDIN for efficient bulk loading
            copy_statement = f"COPY {table_name} FROM STDIN WITH (FORMAT TEXT, DELIMITER '|', NULL '\\N')"

            with cur.copy(copy_statement) as copy:
                for row in validator.validate_and_process_rows():
                    rows_processed += 1

                    # Convert row to pipe-delimited format
                    # Replace empty strings with \N for NULL values
                    formatted_row = "|".join(
                        "\\N" if (not cell or validator.is_nul_like(cell)) else cell
                        for cell in row
                    )

                    try:
                        copy.write(formatted_row + "\n")
                        rows_loaded += 1
                    except Exception as e:
                        logger.warning(f"Failed to load row {rows_processed}: {e}")
                        continue

            # Re-enable triggers
            cur.execute("SET session_replication_role = DEFAULT;")

        conn.commit()

    # Collect statistics from validator
    total_modifications = len(validator.modifications)
    empty_primary_keys = len(validator.empty_primary_keys)
    data_quality_issues = len(
        [row for row in validator.bad_rows if row.get("bad_values")]
    )

    return {
        "table_name": table_name,
        "rows_processed": rows_processed,
        "rows_loaded": rows_loaded,
        "total_modifications": total_modifications,
        "empty_primary_keys": empty_primary_keys,
        "data_quality_issues": data_quality_issues,
        "structural_issues": len(validator.bad_rows) - data_quality_issues,
        "success": True,
        "quality_report": validator.generate_quality_report(),
    }


def generate_validation_report(directory: Path, postfix: str) -> Dict:
    """
    Compare expected vs actual row counts using Count.txt and database tables.

    Args:
        directory: Path to directory containing Count.txt
        postfix: Table postfix (e.g., "06_25")

    Returns:
        Dictionary with validation results
    """
    count_file = directory / "Schema_Related" / "Count.txt"
    if not count_file.exists():
        raise FileNotFoundError(f"Count.txt not found at {count_file}")

    # Parse expected counts from Count.txt
    expected_counts = dump_counts(str(count_file))

    # Get actual counts from database
    actual_counts = {}
    missing_tables = []

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for csv_file, expected_count in expected_counts.items():
                try:
                    table_name = get_table_name(csv_file, postfix)

                    if validate_table_exists(table_name):
                        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                        actual_count = cur.fetchone()[0]
                        actual_counts[csv_file] = actual_count
                    else:
                        missing_tables.append(table_name)
                        actual_counts[csv_file] = 0

                except KeyError:
                    # CSV file not in tables mapping
                    logger.warning(f"No table mapping found for {csv_file}")
                    continue

    # Calculate differences and summary statistics
    total_expected = sum(expected_counts.values())
    total_actual = sum(actual_counts.values())
    differences = {}

    for csv_file in expected_counts:
        if csv_file in actual_counts:
            expected = expected_counts[csv_file]
            actual = actual_counts[csv_file]
            diff = actual - expected
            diff_pct = (diff / expected * 100) if expected > 0 else 0

            differences[csv_file] = {
                "expected": expected,
                "actual": actual,
                "difference": diff,
                "difference_pct": diff_pct,
                "status": "match" if diff == 0 else ("over" if diff > 0 else "under"),
            }

    return {
        "total_expected": total_expected,
        "total_actual": total_actual,
        "total_difference": total_actual - total_expected,
        "total_difference_pct": (
            ((total_actual - total_expected) / total_expected * 100)
            if total_expected > 0
            else 0
        ),
        "files_compared": len(differences),
        "missing_tables": missing_tables,
        "perfect_matches": len(
            [d for d in differences.values() if d["status"] == "match"]
        ),
        "differences": differences,
        "postfix": postfix,
    }


def get_csv_files(directory: Path) -> List[Path]:
    """
    Get list of CSV files in directory (excluding lookup files).

    Args:
        directory: Directory to search for CSV files

    Returns:
        List of CSV file paths
    """
    csv_files = []

    for file_path in directory.glob("*.csv"):
        # Skip lookup files and other non-main CSV files
        if file_path.parent.name.lower() != "lookup":
            csv_files.append(file_path)

    return sorted(csv_files)


def clean_single_file(csv_file: Path, postfix: str) -> Dict:
    """
    Clean and load a single CSV file.

    Args:
        csv_file: Path to CSV file
        postfix: Table postfix for database loading

    Returns:
        Dictionary with cleaning and loading results
    """
    try:
        # Initialize validator
        validator = CsvValidator(str(csv_file))

        # Get target table name
        table_name = get_table_name(csv_file.name, postfix)

        # Clean and load to database
        result = copy_csv_to_table(validator, table_name)
        result["csv_file"] = str(csv_file)
        result["file_size_mb"] = csv_file.stat().st_size / (1024 * 1024)

        return result

    except Exception as e:
        logger.error(f"Error processing {csv_file}: {e}")
        return {
            "csv_file": str(csv_file),
            "table_name": table_name if "table_name" in locals() else "unknown",
            "success": False,
            "error": str(e),
            "rows_processed": 0,
            "rows_loaded": 0,
        }

