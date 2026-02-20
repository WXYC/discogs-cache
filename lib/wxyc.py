"""WXYC MySQL connection utilities."""

from __future__ import annotations

from urllib.parse import urlparse

import pymysql


def connect_mysql(db_url: str):
    """Connect to MySQL using a URL string.

    Args:
        db_url: MySQL connection URL (mysql://user:pass@host:port/dbname).

    Returns:
        A pymysql connection object.
    """
    parsed = urlparse(db_url)
    return pymysql.connect(
        host=parsed.hostname or "localhost",
        port=parsed.port or 3306,
        user=parsed.username or "root",
        password=parsed.password or "",
        database=parsed.path.lstrip("/"),
        charset="utf8",
    )
