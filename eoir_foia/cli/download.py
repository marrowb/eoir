"""Download commands for EOIR FOIA data."""
import click
from typing import Optional


@click.group()
def download():
    """Download EOIR FOIA data files."""
    pass


@download.command()
def check():
    """Check if a new version is available."""
    click.echo("Checking for new version...")


@download.command()
@click.option(
    "--no-retry",
    is_flag=True,
    default=False,
    help="Disable automatic retry on failure",
)
def get(no_retry: bool):
    """Download latest EOIR FOIA data."""
    click.echo("Downloading latest version...")
    # Progress bar will be added here


@download.command()
def status():
    """Show current download status."""
    click.echo("Checking download status...")
