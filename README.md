# EOIR FOIA Data Processing Tool

[![PyPI version](https://badge.fury.io/py/eoir.svg)](https://badge.fury.io/py/eoir)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

A high-performance tool for downloading, processing, and managing U.S. immigration court data from the Department of Justice's public FOIA releases.

## Overview

The Executive Office for Immigration Review (EOIR) releases anonymized immigration court data through FOIA requests. This tool automates the entire pipeline of downloading, extracting, cleaning, and loading this data into a PostgreSQL database for analysis.

## Features

- **Automated Downloads**: Fetch the latest FOIA data releases with progress tracking
- **Smart Extraction**: Automatically extract and organize ZIP files
- **Data Cleaning**: Clean and validate CSV files with parallel processing support
- **Database Management**: Load data into PostgreSQL with versioned table names
- **Pipeline Automation**: One-command execution of the entire workflow
- **Docker Support**: Fully containerized with Docker Compose
- **Progress Tracking**: Real-time progress bars and status updates
- **Incremental Updates**: Only download new data when available

## Requirements

- Python 3.10+
- PostgreSQL database
- Docker and Docker Compose (optional, for containerized deployment)

## Installation

### Install from PyPI

```bash
pip install eoir
```

### Local Development Installation

1. Clone the repository:
```bash
git clone https://github.com/marrowb/eoir.git
cd eoir
```

2. Install the package in development mode:
```bash
pip install -e .
```

3. Copy the environment template and configure:
```bash
cp .env.example .env
# Edit .env with your PostgreSQL credentials
```

### Docker Installation

1. Clone the repository:
```bash
git clone https://github.com/marrowb/eoir.git
cd eoir
```

2. Copy the environment template:
```bash
cp .env.example .env
# Edit .env if needed (defaults work for Docker)
```

3. Start the services:
```bash
docker-compose up -d
```

4. Access the application:
```bash
docker-compose exec app bash
# Or use the run script:
./run shell
```

## Quick Start

### Using Docker (Recommended)

```bash
# Run the complete pipeline
./run eoir run-pipeline

# Or run individual commands
./run eoir download status      # Check for new data
./run eoir download fetch       # Download latest data
./run eoir db init             # Initialize database
./run eoir clean               # Clean CSV files
```

### Local Development

```bash
# Run the complete pipeline
eoir run-pipeline

# Or run individual commands
eoir download status           # Check for new data
eoir download fetch           # Download latest data
eoir db init                  # Initialize database
eoir clean                    # Clean CSV files
```

## CLI Commands

### `eoir download`

Manage FOIA data downloads from the DOJ.

```bash
# Check if new data is available
eoir download status

# Download the latest FOIA release
eoir download fetch

# Download without extracting
eoir download fetch --no-unzip
```

### `eoir db`

Database management commands.

```bash
# Initialize database and create tables
eoir db init

# Create a database dump
eoir db dump

# Dump with custom output directory
eoir db dump -o /path/to/dumps
```

### `eoir clean`

Clean and process CSV files.

```bash
# Clean all CSV files in the latest download
eoir clean

# Clean with custom worker count
eoir clean --workers 16

# Clean specific input directory
eoir clean --input-dir /path/to/csvs
```

### `eoir run-pipeline`

Execute the complete data pipeline.

```bash
# Run full pipeline with defaults
eoir run-pipeline

# Run with custom settings
eoir run-pipeline --workers 16 --output-dir custom_dumps

# Skip download if data exists
eoir run-pipeline --skip-download
```

### `eoir config`

View configuration settings.

```bash
# Show current configuration
eoir config show
```

## Architecture

### Project Structure

```
eoir/
├── src/eoir/
│   ├── cli/               # Command-line interface modules
│   │   ├── download.py    # Download commands
│   │   ├── db.py          # Database commands
│   │   ├── clean.py       # CSV cleaning commands
│   │   └── pipeline.py    # Pipeline orchestration
│   ├── core/              # Core business logic
│   │   ├── download.py    # Download functionality
│   │   ├── db.py          # Database operations
│   │   ├── clean.py       # CSV processing
│   │   └── models.py      # Data models
│   ├── metadata/          # Data definitions
│   │   ├── foia_tables.sql      # Database schema
│   │   └── json/                # Table and column metadata
│   ├── logging/           # Structured logging
│   └── settings.py        # Configuration management
├── docker-compose.yml     # Docker services
├── Dockerfile            # Container definition
└── run                   # Development helper script
```

### Data Flow

1. **Download**: Fetches ZIP file from `https://fileshare.eoir.justice.gov/FOIA-TRAC-Report.zip`
2. **Extract**: Unzips to timestamped directory in `downloads/`
3. **Clean**: Processes CSV files to handle encoding and data issues
4. **Load**: Imports cleaned data into PostgreSQL with versioned table names
5. **Track**: Records download history and file metadata

### Database Schema

The tool creates versioned tables based on the download date. For example, a download on June 25th creates tables like:
- `foia_appeal_06_25`
- `foia_case_06_25`
- `foia_schedule_06_25`

See `src/eoir/metadata/foia_tables.sql` for the complete schema.

## Data Reference

### Processed Tables

The tool processes 20 different CSV files containing various immigration court records:

| CSV File | Database Table | Description |
|----------|----------------|-------------|
| `A_TblCase.csv` | `foia_case_XX_XX` | Case information |
| `tblAppeal.csv` | `foia_appeal_XX_XX` | Appeal records |
| `tbl_schedule.csv` | `foia_schedule_XX_XX` | Court schedules |
| `B_TblProceeding.csv` | `foia_proceeding_XX_XX` | Proceeding details |
| `tbl_EOIR_Attorney.csv` | `foia_atty_XX_XX` | Attorney information |

See `src/eoir/metadata/json/tables.json` for the complete mapping.

### Data Format

- **Encoding**: Latin-1
- **Delimiter**: Tab (`\t`)
- **Escape Character**: Backslash (`\\`)
- **Dialect**: Excel-tab

## Development

### Using the Run Script

The `run` script provides convenient commands for development:

```bash
./run eoir --help          # Run EOIR CLI
./run shell                # Start interactive shell
./run manage               # Database management
./run psql                 # PostgreSQL console
./run pip install package  # Install Python packages
./run yarn                 # Manage frontend (if applicable)
```

### Environment Variables

Configure the following in your `.env` file:

```env
# PostgreSQL Configuration
POSTGRES_USER=eoir
POSTGRES_PASSWORD=changeme
POSTGRES_DB=eoir
POSTGRES_HOST=postgres      # 'postgres' for Docker, 'localhost' for local
POSTGRES_PORT=5434         # External port (internal always 5432)

# Logging
LOG_LEVEL=INFO             # DEBUG, INFO, WARNING, ERROR
```

### Docker Development

```bash
# Build and start services
docker-compose up -d --build

# View logs
docker-compose logs -f app

# Stop services
docker-compose down

# Remove all data (including database)
docker-compose down -v
```

## Troubleshooting

### Common Issues

1. **Port conflicts**: If port 5434 is in use, change `POSTGRES_PORT` in `.env`
2. **Permission errors**: Ensure `downloads/` and `dumps/` directories are writable
3. **Memory issues**: Reduce worker count with `--workers` flag for large files
4. **Encoding errors**: The tool handles Latin-1 encoding automatically

### Debug Mode

Enable debug logging for troubleshooting:

```bash
export LOG_LEVEL=DEBUG
eoir run-pipeline
```

## Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature-name`)
3. Make your changes
4. Run tests (if available)
5. Submit a pull request

## License

[MIT License Copyright (c) 2025 Backlog Immigration LLC](LICENSE)

## Acknowledgments

This tool processes publicly available FOIA data from the U.S. Department of Justice Executive Office for Immigration Review.
