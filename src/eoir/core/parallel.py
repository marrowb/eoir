"""Parallel processing for CSV files."""

import multiprocessing as mp
from pathlib import Path
from typing import List, Dict
import structlog

logger = structlog.get_logger()


def process_file_worker(args):
    """Worker function for multiprocessing."""
    csv_file, postfix = args

    from eoir.core.clean import clean_single_file

    try:
        return clean_single_file(csv_file, postfix)
    except Exception as e:
        logger.error(f"Worker failed processing {csv_file}: {e}")
        return {
            "csv_file": str(csv_file),
            "success": False,
            "error": str(e),
            "rows_processed": 0,
            "rows_loaded": 0,
            "empty_primary_keys": 0,
        }


def clean_files_parallel(
    files: List[Path], postfix: str, num_workers: int = None
) -> List[Dict]:
    """Process CSV files in parallel using multiprocessing."""
    if num_workers is None:
        num_workers = min(mp.cpu_count() - 1, 8)

    logger.info(f"Starting parallel processing with {num_workers} workers")

    work_items = [(file, postfix) for file in files]

    with mp.Pool(processes=num_workers) as pool:
        results = pool.map(process_file_worker, work_items)

    return results
