"""Database management commands."""
import click
import structlog
from eoir_foia.core.db import create_database
from eoir_foia.settings import pg_db

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
    except Exception as e:
        logger.error("Database initialization failed", error=str(e))
        raise click.ClickException(str(e))
