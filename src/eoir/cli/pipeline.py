"""Pipeline command to orchestrate the complete EOIR data processing workflow."""

import subprocess
import sys
from pathlib import Path

import click
import structlog

from eoir.core.clean import build_postfix
from eoir.core.db import create_database, get_db_connection

logger = structlog.get_logger()


def run_cli_command(command_parts, stream_output=False):
    """Run an eoir CLI command using subprocess.
    
    Args:
        command_parts: List of command parts to run
        stream_output: If True, stream output in real-time (for progress bars)
    """
    # Check if we should use the run script
    if Path("./run").exists() and not Path("/.dockerenv").exists():
        # We're outside Docker, use the run script
        cmd = ["./run", "eoir"] + command_parts
    else:
        # We're inside Docker or eoir is installed
        cmd = ["eoir"] + command_parts
    
    if stream_output:
        # Stream output directly to terminal for real-time progress
        result = subprocess.run(cmd, stdin=subprocess.DEVNULL)
    else:
        # Capture output for processing
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.stdout:
            click.echo(result.stdout.rstrip())
    
    if result.returncode != 0:
        if not stream_output and result.stderr:
            click.echo(result.stderr.rstrip(), err=True)
        raise click.ClickException(f"Command failed: {' '.join(command_parts)}")
    
    return result


@click.command("run-pipeline")
@click.option(
    "--workers",
    "-w",
    type=int,
    default=8,
    help="Number of parallel workers for cleaning (default: 8)",
)
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(exists=False, file_okay=False, dir_okay=True),
    default="dumps",
    help="Directory for dump file (default: dumps)",
)
@click.option(
    "--skip-download",
    is_flag=True,
    help="Skip download if data already exists",
)
@click.option(
    "--no-unzip",
    is_flag=True,
    help="Download without extracting ZIP file",
)
def run_pipeline(workers, output_dir, skip_download, no_unzip):
    """Run complete EOIR data pipeline from download to dump."""
    click.echo("Starting EOIR data pipeline...")
    click.echo("=" * 50)
    
    # Step 1: Check database exists, create if needed
    click.echo("\n[1/6] Checking database...")
    try:
        with get_db_connection():
            click.echo("✓ Database exists")
    except Exception:
        click.echo("Database not found, creating...")
        try:
            if create_database():
                click.echo("✓ Database created")
            else:
                click.echo("✓ Database already exists")
        except Exception as e:
            click.echo(f"✗ Failed to create database: {e}", err=True)
            raise click.ClickException("Database creation failed")
    
    # Step 2: Initialize download tracking
    click.echo("\n[2/6] Initializing download tracking...")
    try:
        run_cli_command(["db", "init"])
        click.echo("✓ Download tracking initialized")
    except Exception as e:
        click.echo(f"✗ Failed to initialize: {e}", err=True)
        raise
    
    # Step 3: Download data (unless skipped)
    if not skip_download:
        click.echo("\n[3/6] Downloading FOIA data...")
        try:
            # First check status
            run_cli_command(["download", "status"])
            
            # Run download with streaming output for progress bar
            download_cmd = ["download", "fetch"]
            if no_unzip:
                download_cmd.append("--no-unzip")
            run_cli_command(download_cmd, stream_output=True)
            click.echo("✓ Download complete")
        except Exception as e:
            click.echo(f"✗ Download failed: {e}", err=True)
            raise
    else:
        click.echo("\n[3/6] Skipping download (--skip-download flag)")
    
    # Get postfix for table naming
    postfix = build_postfix()
    click.echo(f"\nUsing postfix: {postfix}")
    
    # Step 4: Create tables
    click.echo(f"\n[4/6] Creating FOIA tables with postfix {postfix}...")
    try:
        run_cli_command(["db", "create-all", "--postfix", postfix])
        click.echo("✓ Tables created")
    except Exception as e:
        click.echo(f"✗ Table creation failed: {e}", err=True)
        raise
    
    # Step 5: Clean data in parallel
    click.echo(f"\n[5/6] Cleaning CSV files (parallel with {workers} workers)...")
    try:
        run_cli_command(["clean", "--postfix", postfix, "--parallel", "--workers", str(workers)], stream_output=True)
        click.echo("✓ Data cleaning complete")
    except Exception as e:
        click.echo(f"✗ Data cleaning failed: {e}", err=True)
        raise
    
    # Step 6: Dump to file
    click.echo(f"\n[6/6] Dumping data to {output_dir}/...")
    try:
        # Create output directory if it doesn't exist
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        run_cli_command(["db", "dump", output_dir, "--postfix", postfix])
        click.echo(f"✓ Data dumped to {output_dir}/foia_{postfix}.dump")
    except Exception as e:
        click.echo(f"✗ Data dump failed: {e}", err=True)
        raise
    
    click.echo("\n" + "=" * 50)
    click.echo("✓ Pipeline completed successfully!")
    click.echo(f"  - Postfix: {postfix}")
    click.echo(f"  - Dump file: {output_dir}/foia_{postfix}.dump")