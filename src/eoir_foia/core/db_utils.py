"""Database utility functions and decorators."""
from functools import wraps
import psycopg
from typing import Callable

def db_operation(f: Callable):
    """Decorator to handle database operation exceptions."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except psycopg.OperationalError as e:
            raise Exception(f"Database operation failed: {str(e)}")
        except psycopg.Error as e:
            raise Exception(f"Database error: {str(e)}")
    return wrapper
