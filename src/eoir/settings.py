"""Application settings and configuration."""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database
pg_user = os.getenv("POSTGRES_USER", "eoir")
pg_pass = os.getenv("POSTGRES_PASSWORD", "password")
pg_host = os.getenv("POSTGRES_HOST", "localhost")
pg_port = os.getenv("POSTGRES_PORT", "5432")
pg_db = os.getenv("POSTGRES_DB", pg_user)

DATABASE_URL = f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
ADMIN_URL = f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/postgres"

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR = Path("logs")

# Download
EOIR_URL = "https://fileshare.eoir.justice.gov/FOIA-TRAC-Report.zip"
DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

# JSON Configuration
ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), "."))
METADATA_DIR = os.path.join(ROOT_DIR, "metadata")
JSON_DIR = os.path.join(METADATA_DIR, "json")
