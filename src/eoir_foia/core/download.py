"""Core download functionality for EOIR FOIA data."""

import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import requests
import structlog
from zipfile_deflate64 import ZipFile

from eoir_foia.core.db import get_latest_download, record_download_in_history
from eoir_foia.core.models import FileMetadata
from eoir_foia.settings import DOWNLOAD_DIR, EOIR_FOIA_URL

logger = structlog.get_logger()


def check_file_status() -> Tuple[FileMetadata, FileMetadata, str]:
    """Check remote file status and compare with local version."""
    try:
        response = requests.head(EOIR_FOIA_URL)
        response.raise_for_status()
        current = FileMetadata.from_headers(response.headers)

        # Compare with latest download record
        local = get_latest_download()
        if not local:
            message = "No local data available."
        elif current != local:
            message = "New Version Available:"
        else:
            message = "Already have latest version:"

        return current, local, message
    except requests.RequestException as e:
        logger.error("Failed to check file status", error=str(e))
        raise


def unzip(metadata: FileMetadata) -> Path:
    """Extract FOIA ZIP file into dated directory using last_modified date."""
    zip_file = metadata.local_path
    extract_dir = DOWNLOAD_DIR
    dated_dir = extract_dir / f"{metadata.last_modified:%m%d%y}-FOIA-TRAC-FILES"
    extract_dir.mkdir(parents=True, exist_ok=True)

    with ZipFile(zip_file, "r", allowZip64=True) as zip_ref:
        zip_ref.extractall(extract_dir)

    extracted_items = [
        item
        for item in extract_dir.iterdir()
        if item.is_dir()
        and item != dated_dir
        and item.name not in (zip_file.stem, zip_file.name)
    ]

    if extracted_items and len(extracted_items) == 1:
        root_folder = extracted_items[0]

        if dated_dir.exists():
            shutil.rmtree(dated_dir)

        root_folder.rename(dated_dir)
        return dated_dir

    return extract_dir


def download_file(
    output_path: Path,
    metadata: FileMetadata,
    max_retries: int = 3,
    timeout: int = 30,
    progress_callback: Optional[callable] = None,
) -> Path:
    """Download EOIR FOIA ZIP file with retry logic and progress tracking."""
    retries = 0
    while retries <= max_retries:
        try:
            with requests.get(EOIR_FOIA_URL, stream=True, timeout=timeout) as response:
                response.raise_for_status()

                output_path.parent.mkdir(parents=True, exist_ok=True)
                total = int(response.headers.get("content-length", 0))
                downloaded = 0

                with open(output_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            downloaded += len(chunk)
                            f.write(chunk)
                            if progress_callback:
                                progress_callback(downloaded, total)

                actual_size = output_path.stat().st_size
                if actual_size != total:
                    logger.error(
                        f"Download incomplete: expected {total} bytes but got {actual_size} bytes"
                    )
                    if retries < max_retries:
                        retries += 1
                        logger.info(
                            f"Retrying download (attempt {retries}/{max_retries})..."
                        )
                        continue
                    else:
                        raise Exception("Download incomplete after maximum retries")

                record_download_in_history(
                    content_length=metadata.content_length,
                    last_modified=metadata.last_modified,
                    etag=metadata.etag,
                    local_path=str(output_path),
                    status="completed",
                )

                return output_path

        except requests.RequestException as e:
            logger.error("Failed to download file", error=str(e))
            if retries < max_retries:
                retries += 1
                logger.info(f"Retrying download (attempt {retries}/{max_retries})...")
            else:
                raise

    raise Exception("Failed to download file after maximum retries")
