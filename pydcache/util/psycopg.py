from __future__ import annotations

import logging
import psycopg2
import psycopg2.extensions
import psycopg2.extras
from typing import Any, Dict, List, Optional, Tuple, NoReturn
from urllib.parse import urlparse
import yaml

logger = logging.getLogger(__name__)

def create_connection(uri: str) -> psycopg2.extensions.connection:
    """Create a database connection from a URI.

    Args:
        uri: Database connection URI

    Returns:
        Database connection object

    Raises:
        psycopg2.Error: If connection fails
    """
    result = urlparse(uri)
    return psycopg2.connect(
        database=result.path[1:],
        user=result.username,
        password=result.password,
        host=result.hostname,
        port=result.port
    )


def update(
    conn: psycopg2.extensions.connection,
    sql: str,
    params: Optional[tuple] = None
) -> Any:
    """Update database records.

    Args:
        conn: Database connection
        sql: SQL statement
        params: Query parameters

    Returns:
        Query result

    Raises:
        psycopg2.Error: If query fails
    """
    return insert(conn, sql, params)


def insert(
    conn: psycopg2.extensions.connection,
    sql: str,
    params: Optional[tuple] = None
) -> Any:
    """Insert database records.

    Args:
        conn: Database connection
        sql: SQL statement
        params: Query parameters

    Returns:
        Query result

    Raises:
        psycopg2.Error: If query fails
    """
    cursor = None
    try:
        cursor = conn.cursor()
        if params:
            result = cursor.execute(sql, params)
        else:
            result = cursor.execute(sql)
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass


def insert_returning(
    conn: psycopg2.extensions.connection,
    sql: str,
    params: Optional[tuple] = None
) -> Dict[str, Any]:
    """Insert database record and return inserted row.

    Args:
        conn: Database connection
        sql: SQL statement
        params: Query parameters

    Returns:
        Dictionary containing inserted row

    Raises:
        psycopg2.Error: If query fails
    """
    cursor = None
    try:
        sql += " returning *"
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        result = cursor.fetchone()
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass


def select(
    conn: psycopg2.extensions.connection,
    sql: str,
    params: Optional[tuple] = None
) -> List[Dict[str, Any]]:
    """Select database records.

    Args:
        conn: Database connection
        sql: SQL statement
        params: Query parameters

    Returns:
        List of dictionaries containing selected rows

    Raises:
        psycopg2.Error: If query fails
    """
    cursor = None
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        return cursor.fetchall()
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
