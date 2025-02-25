"""Application settings and configuration."""
import os
from pathlib import Path

# Database
DATABASE_URL = os.getenv("DATABASE_URL")

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR = Path("logs")

# Download
EOIR_FOIA_URL = "https://fileshare.eoir.justice.gov/FOIA-TRAC-Report.zip"
DOWNLOAD_DIR = Path("downloads")
