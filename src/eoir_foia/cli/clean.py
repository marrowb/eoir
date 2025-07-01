"""CSV cleaning commands."""

import json
import multiprocessing as mp
from pathlib import Path
from typing import List, Optional

import click
import structlog

from eoir_foia.core.clean import (
    build_postfix,
    check_for_null_bytes,
    clean_single_file,
    get_csv_files,
    get_download_dir,
    remove_null_bytes_subprocess,
)
from eoir_foia.settings import JSON_DIR

logger = structlog.get_logger()


def _get_table_name(csv_filename: str) -> str:
    """Map CSV filename to table name via tables.json."""
    try:
        with open(f"{JSON_DIR}/tables.json", "r") as f:
            tables_map = json.load(f)
        return tables_map.get(csv_filename, "unknown")
    except FileNotFoundError:
        return "unknown"


def _display_file_selection_menu(csv_files: List[Path], postfix: str) -> None:
    """Show numbered menu of available CSV files."""
    click.echo("\nAvailable CSV files:")
    click.echo("0. [ALL FILES] - Process all CSV files")

    for i, csv_file in enumerate(csv_files, 1):
        table_name = _get_table_name(csv_file.name)
        click.echo(f"{i}. {table_name}_{postfix} - {csv_file.name}")


def _parse_selection(user_input: str, file_count: int) -> List[int]:
    """Parse comma-separated file selections and validate range."""
    try:
        selections = []
        parts = [part.strip() for part in user_input.split(",")]

        for part in parts:
            num = int(part)
            if num < 0 or num > file_count:
                raise ValueError(f"Selection {num} is out of range (0-{file_count})")
            selections.append(num)

        return selections
    except ValueError as e:
        raise click.ClickException(f"Invalid selection: {e}")


def _get_user_file_selection(csv_files: List[Path], postfix: str) -> List[Path]:
    """Interactive file selection from menu."""
    _display_file_selection_menu(csv_files, postfix)

    user_input = click.prompt(
        "\nEnter file numbers to process (comma-separated, 0 for all)", type=str
    )

    selections = _parse_selection(user_input, len(csv_files))

    if 0 in selections:
        return csv_files

    return [csv_files[i - 1] for i in selections]


@click.command()
@click.option(
    "--path",
    help="Directory containing CSV files. If not provided, uses latest download directory.",
)
@click.option(
    "--postfix",
    help="Table postfix (e.g., 06_25). If not provided, uses latest download date.",
)
@click.option(
    "--choose",
    is_flag=True,
    help="Display file selection menu instead of processing all files.",
)
@click.option(
    "--parallel",
    is_flag=True,
    help="Process files in parallel for faster execution.",
)
@click.option(
    "--workers",
    type=int,
    help="Number of parallel workers (default: CPU count - 1, max 8).",
)
def clean(
    path: Optional[str],
    postfix: Optional[str],
    choose: bool,
    parallel: bool,
    workers: Optional[int],
):
    """Process CSV files and load to database with optional interactive selection."""
    try:
        directory = get_download_dir(path)
        if not postfix:
            postfix = build_postfix()
            click.echo(f"Using postfix from latest download: {postfix}")

        click.echo(f"Processing directory: {directory}")

        # Auto-detect and remove null bytes if needed
        if check_for_null_bytes(directory):
            click.echo("Detected null bytes in CSV files. Removing them now...")
            remove_null_bytes_subprocess(directory)
            click.echo("Null byte removal completed")

        csv_files = get_csv_files(directory)
        if not csv_files:
            raise click.ClickException(f"No CSV files found in {directory}")

        if choose:
            files_to_process = _get_user_file_selection(csv_files, postfix)
            if not files_to_process:
                click.echo("No files selected.")
                return
        else:
            files_to_process = csv_files

        click.echo(f"\nProcessing {len(files_to_process)} CSV files")

        if parallel:
            from eoir_foia.core.parallel import clean_files_parallel

            worker_count = workers or min(mp.cpu_count() - 1, 8)
            click.echo(f"Using parallel processing with {worker_count} workers")

            results = clean_files_parallel(files_to_process, postfix, workers)

            total_rows_processed = sum(r.get("rows_processed", 0) for r in results)
            total_rows_loaded = sum(r.get("rows_loaded", 0) for r in results)
        else:
            results = []
            total_rows_processed = 0
            total_rows_loaded = 0

            with click.progressbar(files_to_process, label="Processing files") as files:
                for csv_file in files:
                    result = clean_single_file(csv_file, postfix)
                    results.append(result)
                    total_rows_processed += result.get("rows_processed", 0)
                    total_rows_loaded += result.get("rows_loaded", 0)

        successful = len([r for r in results if r.get("success", False)])
        failed = len(results) - successful

        click.echo("\n" + "=" * 60)
        click.echo("BATCH PROCESSING SUMMARY")
        click.echo("=" * 60)
        click.echo(f"Files processed: {len(files_to_process)}")
        click.echo(f"Successful: {successful}")
        if failed > 0:
            click.echo(click.style(f"Failed: {failed}", fg="red"))
        click.echo(f"Total rows processed: {total_rows_processed:,}")
        click.echo(f"Total rows loaded: {total_rows_loaded:,}")

        click.echo(f"\nIndividual File Results:")
        for result in results:
            file_name = Path(result["csv_file"]).name
            if result.get("success", False):
                status = click.style("✅", fg="green")
                details = f"{result['rows_loaded']:,} rows"
                if result.get("empty_primary_keys", 0) > 0:
                    details += f" ({result['empty_primary_keys']:,} empty PKs)"
            else:
                status = click.style("❌", fg="red")
                details = result.get("error", "Unknown error")[:50]

            click.echo(f"  {status} {file_name:30s} {details}")

    except Exception as e:
        logger.error("Clean failed", error=str(e))
        raise click.ClickException(str(e))
