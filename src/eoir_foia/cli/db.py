"""Database management commands."""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import click
import structlog

from eoir_foia.core.clean import build_postfix
from eoir_foia.core.db import (
    create_database,
    get_connection,
    get_db_connection,
    init_download_tracking,
)
from eoir_foia.settings import METADATA_DIR, pg_db, pg_host, pg_pass, pg_port, pg_user

logger = structlog.get_logger()


@click.group()
def db():
    """Database operations."""
    pass


@db.command()
def init():
    """Create database and tables if they don't exist."""
    try:
        if create_database():
            click.echo(f"Created database '{pg_db}'")
        else:
            click.echo(f"Database '{pg_db}' already exists")

        init_download_tracking()
        click.echo("Initialized download tracking table")

        # Create all FOIA tables
        create_all()

    except Exception as e:
        logger.error("Database initialization failed", error=str(e))
        raise click.ClickException(str(e))


@db.command()
@click.option(
    "--postfix",
    help="Table postfix (e.g., 06_25). If not provided, uses latest download date.",
)
def create_all(postfix=None):
    """Create all FOIA tables from tx.py."""
    try:
        if not postfix:
            postfix = build_postfix()
            click.echo(f"Using postfix from latest download: {postfix}")
        conn = get_connection()
        cursor = conn.cursor()
        tables = open(os.path.join(METADATA_DIR, "foia_tables.sql")).read()
        file = tables.replace("(%s)", postfix)
        cursor.execute(file)

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        logger.error("Table creation failed", error=str(e))
        raise click.ClickException(str(e))


@db.command("drop-foia")
@click.argument("postfix")
def drop_foia(postfix):
    """
    Drop all tables and cascade.
    """
    if not click.confirm("Are you sure you want to drop all tables?"):
        return

    with get_db_connection() as curs:
        curs.execute(
            f"""
            DROP TABLE IF EXISTS "foia_appeal_{postfix}";
            DROP TABLE IF EXISTS "foia_application_{postfix}";
            DROP TABLE IF EXISTS "foia_bond_{postfix}";
            DROP TABLE IF EXISTS "foia_charges_{postfix}";
            DROP TABLE IF EXISTS "foia_case_{postfix}";
            DROP TABLE IF EXISTS "foia_custody_{postfix}";
            DROP TABLE  IF EXISTS "foia_motion_{postfix}";
            DROP TABLE  IF EXISTS "foia_rider_{postfix}";
            DROP TABLE  IF EXISTS "foia_schedule_{postfix}";
            DROP TABLE  IF EXISTS "foia_proceeding_{postfix}";
            DROP TABLE  IF EXISTS "foia_juvenile_{postfix}";
            DROP TABLE  IF EXISTS "foia_fedcourts_{postfix}";
            DROP TABLE  IF EXISTS "foia_probono_{postfix}";
            DROP TABLE  IF EXISTS "foia_threembr_{postfix}";
            DROP TABLE  IF EXISTS "foia_atty_{postfix}";
            DROP TABLE  IF EXISTS "foia_caseid_{postfix}";
            DROP TABLE  IF EXISTS "foia_casepriority_{postfix}";
            DROP TABLE  IF EXISTS "foia_reps_{postfix}";
            """
        )
    return


@db.command()
@click.argument("output_dir", type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option(
    "--postfix",
    help="Table postfix (e.g., 06_25). If not provided, uses latest download date.",
)
def dump(output_dir, postfix):
    """Dump all FOIA tables to a compressed file using pg_dump."""
    # Check if pg_dump is available
    if not shutil.which("pg_dump"):
        raise click.ClickException(
            "pg_dump not found. Please ensure PostgreSQL client tools are installed."
        )
    
    try:
        # Use build_postfix if not provided
        if not postfix:
            postfix = build_postfix()
            click.echo(f"Using postfix from latest download: {postfix}")
        
        # Build output path
        output_path = Path(output_dir) / f"foia_{postfix}.dump"
        
        # Build pg_dump command
        dump_cmd = [
            "pg_dump",
            "-t", f"foia_*_{postfix}",
            "-U", pg_user,
            "-d", pg_db,
            "-h", pg_host,
            "-p", pg_port,
            "-Fc",  # Custom format (compressed)
        ]
        
        click.echo(f"Dumping FOIA tables with postfix '{postfix}' to {output_path}")
        
        # Set PGPASSWORD environment variable
        env = os.environ.copy()
        env["PGPASSWORD"] = pg_pass
        
        # Execute pg_dump
        with open(output_path, "wb") as dump_file:
            result = subprocess.run(
                dump_cmd,
                stdout=dump_file,
                stderr=subprocess.PIPE,
                env=env,
                text=False
            )
        
        if result.returncode != 0:
            # Clean up failed dump file
            if output_path.exists():
                output_path.unlink()
            raise click.ClickException(
                f"pg_dump failed: {result.stderr.decode() if result.stderr else 'Unknown error'}"
            )
        
        # Check if dump file was created and has content
        if not output_path.exists() or output_path.stat().st_size == 0:
            if output_path.exists():
                output_path.unlink()
            raise click.ClickException(
                "pg_dump completed but no data was written. Check if tables exist."
            )
        
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        click.echo(f"Successfully created dump: {output_path} ({file_size_mb:.2f} MB)")
        
    except Exception as e:
        logger.error("Database dump failed", error=str(e))
        raise click.ClickException(str(e))
