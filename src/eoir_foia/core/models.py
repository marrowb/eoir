"""Common data models."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional


@dataclass
class FileMetadata:
    """Metadata for EOIR FOIA download."""

    content_length: int
    last_modified: datetime
    etag: str
    local_path: Optional[Path] = None

    @classmethod
    def from_headers(cls, headers: dict) -> "FileMetadata":
        """Create FileMetadata from response headers."""
        return cls(
            content_length=int(headers.get("Content-Length", 0)),
            last_modified=datetime.strptime(
                headers.get("Last-Modified", ""), "%a, %d %b %Y %H:%M:%S GMT"
            ),
            etag=headers.get("ETag", "").strip('"'),
        )

    def __eq__(self, value: object, /) -> bool:
        if type(value) != type(self):
            return False
        return self.etag == value.etag or self.content_length == value.content_length
