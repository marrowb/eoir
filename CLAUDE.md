# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an EOIR (Executive Office for Immigration Review) FOIA data processing tool that downloads, processes, and manages immigration court data from the DOJ's public FOIA releases.

## Development Commands

### Installation and Setup
```bash
pip install -e .  # Install package in development mode
```

### Main CLI Commands
```bash
eoir-foia --help                    # Show all available commands
eoir-foia db init                   # Initialize database and tables
eoir-foia download status           # Check for new files available
eoir-foia download fetch            # Download latest FOIA data
eoir-foia download fetch --no-unzip # Download without auto-extracting
eoir-foia clean                     # CSV cleaning operations (placeholder)
```

### Database Requirements
- PostgreSQL database required
- Correct credentials currently in .env file

## Architecture

### Package Structure
- `src/eoir_foia/` - Main package
  - `cli/` - Click command-line interface modules
    - `download.py` - File download commands with progress tracking
    - `db.py` - Database initialization commands
    - `clean.py` - CSV cleaning operations (skeleton)
  - `core/` - Core business logic
    - `download.py` - Download functionality with retry logic
    - `db.py` - Database operations and connection management
    - `db_utils.py` - Database utility functions
    - `models.py` - Data models and schemas
    - `csv.py` - CSV processing utilities
  - `metadata/` - Data structure definitions
  - `logging/` - Structured logging configuration
  - `settings.py` - Configuration and environment handling
  - `legacy/` - contains the cleancsv code and data type files that we will refactor

### Key Features
- Downloads EOIR FOIA ZIP files from DOJ with progress tracking
- Automatic file status checking (compares local vs remote)
- ZIP extraction with dated directory creation
- PostgreSQL integration for download history tracking
- Structured logging with configurable levels
- Environment-based configuration

### Data Flow
1. Check file status against remote DOJ server
2. Download if new version available
3. Extract ZIP to dated directory in downloads/
4. Track download history in PostgreSQL
5. Process CSV files (cleaning operations TBD)

# Refactor Cleancsv File
## Your Role
You are an expert data engineer concerned with maximum efficiency and clean, concise, and readable code. 

## About the Data
### Origins
This is a database dump from the immigration court's mysql database.

### Delimiter & Dialect
As far as I understand, it uses `\t` and `excel-tab`

### Escape Char
It uses `\\`

### Encoding
The .csv files we're cleaning are encoded with latin-1 encoding, I have a suspicion that some of the data cleaning I'm currently doing in the cleancsv file is a result of this. 

## About the Cleaning Process

### CleanCsv
* CRITICAL: this is a function from my current process that is not yet adapted to the new environment in this eoir directory

### Function Call Chain
* this is the overall function write a csv file to the database
```py
def clean_and_write(file: str, postfix: str):
    _csv = CleanCsv(file)
    _csv.replace_nul()
    print(f"Copying {os.path.abspath(file)} to table {_csv.table}")
    conn_string = app.config["SQLALCHEMY_DATABASE_URI"].replace("+psycopg", "")
    conn = psycopg.connect(conn_string)
    _csv.copy_to_table(conn, postfix)
    _csv.del_no_nul()
    print(
            f"Copied {_csv.row_count-_csv.empty_pk} of {_csv.row_count} rows to {_csv.table}"
            )
    print(f"There were {_csv.empty_pk} rows with no primary keys")
```

* `copy_to_table` is copying to a postgres table from python generator expressions
* `csv_gen_pk`  filters out lines with null primary keys
* `csv_gen`
  * reads the file with nul_bytes replaced
  * checks if the length of the row matches the length of the header
  * if the length of the row is more than the length of the header
    * Try using the datatype definitions file to identify bad values with a list of [(index, value)]
      * if there are bad values, try to remove the nul_like values and return a cleaned row
      * if there are no bad values, try to chop off blank values at the end of the line
  * `clean_row` - force cleans row using datatype files. at the end of the day, the data needs to be forced to be the right datatype to be ingested into the database.
  * if the length of the row is less than the header length, we append blank values

### Problems
* `JSON_DIR` needs to be redefined
* There are millions of rows in each of these files, we need to make the iteration maximally efficient. Currently, if there is a problem with a row we are spending a lot of compute repeatedly iterating over the row.
* Concerns about incorrect data modifications. If we are modifying rows by chopping off values, shifting the values etc. We may be creating further data integrity problems. We should count the number of rows we modify in each file, and randomly sample them for inspection after cleaning. This way we can allay some of these concerns.
* Primary Keys - this seems to mostly be a problem with the tbl_schedule.csv file, but there shouldn't be rows where the primary keys are null. These files are essentially a database dump, since the database can't have null primary keys, neither should our data. 
* There are some unused functions in the file

### Potential Solutions
* try switching to latin-1 encoding and see if this fixes some of the nul bytes problems
* track bad rows along with their modifications in objects on the CleanCSV object 
* Find a way to cut down on iterations over the row's values 

## How you can help
* Let's work with the tbl_schedule.csv file as it is the biggest and most painful
* Checkout a new branch: `git checkout -b cleancsv-refactor`
* Let's slowly (at most a few functions before we test) build up the new CleanCsv file integrated with EOIR
* Step 1: open the csv file with latin 1 encoding, create a generator in the same way as clean_csv using iterating over a csv reader and yielding rows, add the is_nul_like function verbatim
  * Test: run this code using python3 and see if we are still getting values like those in `is_nul_like`
  * Potential Pitfall: We may need to add the `replace_nul` function if there are still nul-bytes
* Step 2: Add back the header_length checks and the bad_values, do not modify the rows yet, let's just record the bad rows
* Step 3: We'll reassess from there


