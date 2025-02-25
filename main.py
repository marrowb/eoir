import click
from eoir_foia.cli import db, clean

@click.group()
def cli():
    """EOIR FOIA data processing tools."""
    pass

cli.add_command(db.db)
cli.add_command(clean.clean)

if __name__ == "__main__":
    cli()
