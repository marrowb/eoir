"""Pipeline command to orchestrate the complete EOIR data processing workflow."""

from pathlib import Path

import click
import structlog

from eoir.core.clean import build_postfix
from eoir.core.db import create_database, get_db_connection

logger = structlog.get_logger()


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
    ctx = click.get_current_context()
    
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
        from eoir.cli.db import init
        ctx.invoke(init)
        click.echo("✓ Download tracking initialized")
    except Exception as e:
        click.echo(f"✗ Failed to initialize: {e}", err=True)
        raise
    
    # Step 3: Download data (unless skipped)
    if not skip_download:
        click.echo("\n[3/6] Downloading FOIA data...")
        try:
            from eoir.cli.download import fetch
            ctx.invoke(fetch, no_unzip=no_unzip)
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
        from eoir.cli.db import create
        ctx.invoke(create, postfix=postfix)
        click.echo("✓ Tables created")
    except Exception as e:
        click.echo(f"✗ Table creation failed: {e}", err=True)
        raise
    
    # Step 5: Clean data in parallel
    click.echo(f"\n[5/6] Cleaning CSV files (parallel with {workers} workers)...")
    try:
        from eoir.cli.clean import clean
        ctx.invoke(clean, path=None, postfix=postfix, choose=False, 
                  parallel=True, workers=workers)
        click.echo("✓ Data cleaning complete")
    except Exception as e:
        click.echo(f"✗ Data cleaning failed: {e}", err=True)
        raise
    
    # Step 6: Dump to file
    click.echo(f"\n[6/6] Dumping data to {output_dir}/...")
    try:
        # Create output directory if it doesn't exist
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        from eoir.cli.db import dump
        ctx.invoke(dump, output_dir=output_dir, postfix=postfix)
        click.echo(f"✓ Data dumped to {output_dir}/foia_{postfix}.dump")
    except Exception as e:
        click.echo(f"✗ Data dump failed: {e}", err=True)
        raise
    
    click.echo("\n" + "=" * 50)
    click.echo("✓ Pipeline completed successfully!")
    click.echo(f"  - Postfix: {postfix}")
    click.echo(f"  - Dump file: {output_dir}/foia_{postfix}.dump")