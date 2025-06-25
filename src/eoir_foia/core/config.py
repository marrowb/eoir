"""Configuration management utilities."""
import os
import shutil
from pathlib import Path
from typing import Dict, Optional, Tuple
import psycopg
import structlog

logger = structlog.get_logger()


def test_database_connection(
    host: str, 
    port: str, 
    user: str, 
    password: str, 
    database: str
) -> Tuple[bool, Optional[str]]:
    """Test database connection with given parameters.
    
    Returns:
        Tuple of (success: bool, error_message: Optional[str])
    """
    try:
        # First try to connect to the specific database
        db_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return True, None
    except psycopg.OperationalError as e:
        # If database doesn't exist, try connecting to postgres database
        if "does not exist" in str(e):
            try:
                admin_url = f"postgresql://{user}:{password}@{host}:{port}/postgres"
                with psycopg.connect(admin_url) as conn:
                    pass
                return True, f"Database '{database}' doesn't exist but connection is valid"
            except psycopg.OperationalError as admin_e:
                return False, f"Connection failed: {admin_e}"
        return False, f"Connection failed: {e}"
    except Exception as e:
        return False, f"Unexpected error: {e}"


def read_env_file(env_path: Path = None) -> Dict[str, str]:
    """Read environment variables from .env file.
    
    Args:
        env_path: Path to .env file, defaults to .env in current directory
        
    Returns:
        Dictionary of environment variables
    """
    if env_path is None:
        env_path = Path(".env")
    
    env_vars = {}
    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    # Remove export prefix if present
                    key = key.replace('export ', '').strip()
                    env_vars[key] = value.strip()
    
    return env_vars


def write_env_file(env_vars: Dict[str, str], env_path: Path = None, backup: bool = True) -> bool:
    """Write environment variables to .env file.
    
    Args:
        env_vars: Dictionary of environment variables to write
        env_path: Path to .env file, defaults to .env in current directory
        backup: Whether to create a backup of existing file
        
    Returns:
        True if successful, False otherwise
    """
    if env_path is None:
        env_path = Path(".env")
    
    try:
        # Create backup if requested and file exists
        if backup and env_path.exists():
            backup_path = env_path.with_suffix('.env.backup')
            shutil.copy2(env_path, backup_path)
            logger.info(f"Created backup at {backup_path}")
        
        # Write new environment file
        with open(env_path, 'w') as f:
            for key, value in env_vars.items():
                f.write(f"export {key}={value}\n")
        
        logger.info(f"Updated environment file at {env_path}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to write environment file: {e}")
        return False


def validate_database_config(host: str, port: str, user: str, password: str, database: str) -> Tuple[bool, Optional[str]]:
    """Validate database configuration parameters.
    
    Returns:
        Tuple of (valid: bool, error_message: Optional[str])
    """
    # Check required fields
    if not all([host, port, user, password, database]):
        return False, "All database configuration fields are required"
    
    # Validate port number
    try:
        port_int = int(port)
        if not (1 <= port_int <= 65535):
            return False, "Port must be between 1 and 65535"
    except ValueError:
        return False, "Port must be a valid number"
    
    # Basic validation for host (allow localhost, IPs, and hostnames)
    if not host.strip():
        return False, "Host cannot be empty"
    
    # Basic validation for user and database names (PostgreSQL identifiers)
    for name, field in [(user, "username"), (database, "database name")]:
        if not name.replace('_', '').replace('-', '').isalnum():
            return False, f"{field} can only contain alphanumeric characters, underscores, and hyphens"
    
    return True, None


def get_current_database_config() -> Dict[str, str]:
    """Get current database configuration from environment.
    
    Returns:
        Dictionary with current database configuration
    """
    return {
        'POSTGRES_HOST': os.getenv('POSTGRES_HOST', 'localhost'),
        'POSTGRES_PORT': os.getenv('POSTGRES_PORT', '5432'),
        'POSTGRES_USER': os.getenv('POSTGRES_USER', 'eoir'),
        'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD', ''),
        'POSTGRES_DB': os.getenv('POSTGRES_DB', os.getenv('POSTGRES_USER', 'eoir'))
    }