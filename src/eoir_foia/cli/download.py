"""Download commands for EOIR FOIA data."""
import click
from datetime import datetime
from pathlib import Path
import structlog
from typing import Optional
from eoir_foia.core.download import check_file_status, download_file
from eoir_foia.core.db import init_download_tracking
from eoir_foia.settings import DOWNLOAD_DIR

logger = structlog.get_logger()

@click.group()
def download():
    """Download EOIR FOIA data files."""
    init_download_tracking()


@download.command()
def check():
    """Check if a new version is available."""
    try:
        metadata, is_new = check_file_status()
        if is_new:
            click.echo("New version available:")
            click.echo(f"Size: {metadata.content_length:,} bytes")
            click.echo(f"Last modified: {metadata.last_modified}")
        else:
            click.echo("Already have latest version")
    except Exception as e:
        logger.error("Check failed", error=str(e))
        raise click.ClickException(str(e))


@download.command()
@click.option(
    "--no-retry",
    is_flag=True,
    default=False,
    help="Disable automatic retry on failure",
)
def fetch(no_retry: bool):
    """Download latest EOIR FOIA data."""
    try:
        metadata, is_new = check_file_status()
        
        if not is_new:
            click.echo("Already have latest version")
            return
            
        # Setup progress bar
        with click.progressbar(
            length=metadata.content_length,
            label='Downloading',
            fill_char='=',
            empty_char='-'
        ) as bar:
            def update_progress(downloaded: int, total: int):
                bar.update(downloaded - bar.pos)
            
            # Download with progress tracking
            output_path = DOWNLOAD_DIR / f"FOIA-TRAC-{metadata.last_modified:%Y%m}.zip"
            download_file(
                output_path=output_path,
                metadata=metadata,
                retry=not no_retry,
                progress_callback=update_progress
            )
            
        click.echo(f"\nDownload complete: {output_path}")
            
    except Exception as e:
        logger.error("Download failed", error=str(e))
        raise click.ClickException(str(e))


@download.command()
def status():
    """Show current download status."""
    try:
        latest, _ = check_file_status()
        if latest:
            click.echo("Latest download:")
            click.echo(f"Size: {latest.content_length:,} bytes")
            click.echo(f"Last Modified: {latest.last_modified}")
            click.echo(f"ETag: {latest.etag}")
        else:
            click.echo("No downloads recorded")
    except Exception as e:
        logger.error("Status check failed", error=str(e))
        raise click.ClickException(str(e))
