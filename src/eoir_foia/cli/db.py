"""Database management commands."""

import os
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
from eoir_foia.settings import METADATA_DIR, pg_db

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
