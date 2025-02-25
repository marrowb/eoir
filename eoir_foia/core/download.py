"""Core download functionality for EOIR FOIA data."""
from dataclasses import dataclass
from datetime import datetime
import requests
from pathlib import Path
import structlog
from typing import Optional, Tuple
from eoir_foia.settings import EOIR_FOIA_URL

logger = structlog.get_logger()

@dataclass
class FileMetadata:
    """Metadata for EOIR FOIA download."""
    content_length: int
    last_modified: datetime
    etag: str
    
    @classmethod
    def from_headers(cls, headers: dict) -> "FileMetadata":
        """Create FileMetadata from response headers."""
        return cls(
            content_length=int(headers.get('Content-Length', 0)),
            last_modified=datetime.strptime(
                headers.get('Last-Modified', ''), 
                '%a, %d %b %Y %H:%M:%S GMT'
            ),
            etag=headers.get('ETag', '').strip('"')
        )

def check_file_status() -> Tuple[FileMetadata, bool]:
    """
    Check status of remote file.
    Returns (metadata, is_new_version)
    """
    try:
        response = requests.head(EOIR_FOIA_URL)
        response.raise_for_status()
        metadata = FileMetadata.from_headers(response.headers)
        
        # Compare with latest download record
        latest = get_latest_download()
        is_new = True
        
        if latest:
            is_new = (
                metadata.etag != latest.etag or
                metadata.content_length != latest.content_length
            )
        
        return metadata, is_new
    except requests.RequestException as e:
        logger.error("Failed to check file status", error=str(e))
        raise

def download_file(
    output_path: Path,
    metadata: FileMetadata,
    retry: bool = True,
    progress_callback: Optional[callable] = None
) -> Path:
    """
    Download EOIR FOIA zip file.
    Returns path to downloaded file.
    """
    try:
        with requests.get(EOIR_FOIA_URL, stream=True) as response:
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
            
            # Record successful download
            record_download(
                content_length=metadata.content_length,
                last_modified=metadata.last_modified,
                etag=metadata.etag,
                local_path=str(output_path),
                status="completed"
            )
            
        return output_path
    except requests.RequestException as e:
        logger.error("Failed to download file", error=str(e))
        if retry:
            logger.info("Retrying download...")
            return download_file(output_path, metadata, retry=False)
        raise
