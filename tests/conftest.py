from typing import Iterator

import psycopg2
import pytest
from psycopg2._psycopg import connection, cursor as cursor_type

from application.settings import settings


@pytest.fixture(scope="session", autouse=True)
def connection_db() -> Iterator[connection]:
    db_connection: connection = psycopg2.connect(dsn=settings.db_dsn)

    with db_connection.cursor() as cursor:  # type: cursor_type
        query = """
        create table if not exists signals
        (
            id         serial                  primary key,
            type       varchar                 not null,
            pair       varchar                 not null,
            time_frame varchar                 not null,
            time       integer                 not null,
            CDL_PATTERN     varchar                 not null,
            created    timestamp default now() not null,
            extra      json
        )"""
        cursor.execute(query)
        db_connection.commit()

    try:
        yield db_connection
    finally:
        try:
            with connection_db.cursor() as cursor:  # type: cursor_type
                cursor.execute("DROP TABLE IF EXISTS signals")
        except Exception as e:
            print(e)

        db_connection.close()


@pytest.fixture(scope="session", autouse=True)
def clean_tables(connection_db: connection) -> None:
    yield

    with connection_db.cursor() as cursor:  # type: cursor_type
        cursor.execute("TRUNCATE signals")
        connection_db.commit()
