from contextlib import contextmanager
from typing import Iterator

import psycopg2
from psycopg2._psycopg import connection

from application.settings import settings

__all__ = ["connect_db"]


@contextmanager
def connect_db(db_dsn: str = None) -> Iterator[connection]:
    db_connection: connection = psycopg2.connect(settings.db_dsn)
    try:
        yield db_connection
    finally:
        db_connection.close()
