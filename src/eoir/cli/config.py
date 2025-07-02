"""Configuration management commands."""

import getpass
from pathlib import Path

import click
import structlog

from eoir.core.config import (
    get_current_database_config,
    read_env_file,
    test_database_connection,
    validate_database_config,
    write_env_file,
)

logger = structlog.get_logger()


@click.group()
def config():
    """Configuration management."""
    pass


@config.command()
def db():
    """Configure database connection interactively."""
    click.echo("Database Configuration")
    click.echo("=" * 30)

    current_config = get_current_database_config()

    host = click.prompt("PostgreSQL Host", default=current_config["POSTGRES_HOST"])

    port = click.prompt("PostgreSQL Port", default=current_config["POSTGRES_PORT"])

    user = click.prompt("PostgreSQL Username", default=current_config["POSTGRES_USER"])

    current_password = current_config["POSTGRES_PASSWORD"]
    if current_password:
        password_prompt = (
            f"PostgreSQL Password [current: {'*' * len(current_password)}]"
        )
        password = getpass.getpass(password_prompt + ": ")
        if not password:
            password = current_password
    else:
        password = getpass.getpass("PostgreSQL Password: ")

    database = click.prompt(
        "PostgreSQL Database", default=current_config["POSTGRES_DB"] or user
    )

    valid, error_msg = validate_database_config(host, port, user, password, database)
    if not valid:
        raise click.ClickException(f"Invalid configuration: {error_msg}")

    click.echo("\nTesting database connection...")
    success, error_msg = test_database_connection(host, port, user, password, database)

    if not success:
        if click.confirm(
            f"Connection test failed: {error_msg}\nDo you want to save the configuration anyway?"
        ):
            pass
        else:
            click.echo("Configuration cancelled.")
            return
    else:
        if error_msg:
            click.echo(f"✓ {error_msg}")
        else:
            click.echo("✓ Database connection successful!")

    # Read existing environment file
    env_vars = read_env_file()

    # Update database configuration
    env_vars.update(
        {
            "POSTGRES_HOST": host,
            "POSTGRES_PORT": port,
            "POSTGRES_USER": user,
            "POSTGRES_PASSWORD": password,
            "POSTGRES_DB": database,
        }
    )

    # Write updated environment file
    if write_env_file(env_vars):
        click.echo("✓ Configuration saved to .env file")
    else:
        raise click.ClickException("Failed to save configuration")


@config.command()
def show():
    """Show current configuration (passwords masked)."""
    click.echo("Current Configuration")
    click.echo("=" * 30)

    config_dict = get_current_database_config()

    for key, value in config_dict.items():
        if "PASSWORD" in key:
            masked_value = "*" * len(value) if value else "(not set)"
            click.echo(f"{key}: {masked_value}")
        else:
            click.echo(f"{key}: {value}")

    # Show additional environment info
    env_file = Path(".env")
    if env_file.exists():
        click.echo(f"\nEnvironment file: {env_file.absolute()}")
    else:
        click.echo(f"\nEnvironment file: {env_file.absolute()} (does not exist)")


@config.command()
@click.confirmation_option(
    prompt="Are you sure you want to test the database connection?"
)
def test():
    """Test current database configuration."""
    config_dict = get_current_database_config()

    click.echo("Testing database connection...")

    success, error_msg = test_database_connection(
        config_dict["POSTGRES_HOST"],
        config_dict["POSTGRES_PORT"],
        config_dict["POSTGRES_USER"],
        config_dict["POSTGRES_PASSWORD"],
        config_dict["POSTGRES_DB"],
    )

    if success:
        if error_msg:
            click.echo(f"✓ {error_msg}")
        else:
            click.echo("✓ Database connection successful!")
    else:
        raise click.ClickException(f"Connection failed: {error_msg}")
