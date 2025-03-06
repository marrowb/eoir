"""Core download functionality for EOIR FOIA data."""
from datetime import datetime
import requests
import zipfile
from pathlib import Path
import structlog
from typing import Optional, Tuple
from eoir_foia.settings import EOIR_FOIA_URL
from eoir_foia.core.db import get_latest_download, record_download_in_history
from eoir_foia.core.models import FileMetadata

logger = structlog.get_logger()

def check_file_status() -> Tuple[FileMetadata, FileMetadata, str]:
    """
    Check status of remote file.
    Returns (metadata, is_new_version)
    """
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

def unzip(zip_file: Path, output_dir: str) -> None:
    """Unzip the file and name it"""
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(output_dir)

def download_file(
    output_path: Path,
    metadata: FileMetadata,
    retry: bool = True,
    max_retries: int = 3,
    timeout: int = 30,
    progress_callback: Optional[callable] = None
) -> Path:
    """
    Download EOIR FOIA zip file.
    Returns path to downloaded file.
    """
    retries = 0
    while retries <= max_retries:
        try:
            with requests.get(EOIR_FOIA_URL, stream=True, timeout=timeout) as response:
                response.raise_for_status()
                
                # Ensure directory exists
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Write file with progress tracking
                total = int(response.headers.get('content-length', 0))
                downloaded = 0
                
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            downloaded += len(chunk)
                            f.write(chunk)
                            if progress_callback:
                                progress_callback(downloaded, total)
                
                # Verify file size after download
                actual_size = output_path.stat().st_size
                if actual_size != total:
                    logger.error(f"Download incomplete: expected {total} bytes but got {actual_size} bytes")
                    if retries < max_retries:
                        retries += 1
                        logger.info(f"Retrying download (attempt {retries}/{max_retries})...")
                        continue
                    else:
                        raise Exception("Download incomplete after maximum retries")
                
                # Record successful download
                record_download_in_history(
                    content_length=metadata.content_length,
                    last_modified=metadata.last_modified,
                    etag=metadata.etag,
                    local_path=str(output_path),
                    status="completed"
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
