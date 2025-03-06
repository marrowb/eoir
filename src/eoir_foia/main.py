import click
from .cli import db, clean, download

@click.group()
def cli():
    """EOIR FOIA data processing tools."""
    pass

cli.add_command(db.db)
cli.add_command(clean.clean)
cli.add_command(download.download)

if __name__ == "__main__":
    cli()
