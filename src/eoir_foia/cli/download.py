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
def status():
    """See if new files are available."""
    try:
        current, local, message = check_file_status()
        click.echo(message + "\n")

        click.echo("Online version:")
        click.echo(f"Last modified: {current.last_modified}")
        click.echo(f"Size: {current.content_length:,} bytes")
        if local:
            click.echo("\nLocal Version:")
            click.echo(f"Last modified: {local.last_modified}")
            click.echo(f"Size: {local.content_length:,} bytes")
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

