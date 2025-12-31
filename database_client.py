"""Simple Postgres client that can run ad-hoc queries."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Generator, Iterable, Sequence

import psycopg2
from psycopg2.extensions import connection as PGConnection
from psycopg2.extras import RealDictCursor


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    host: str = "metro-gis.c3g6kwq2e9oa.us-east-1.rds.amazonaws.com"
    port: int = 5432
    user: str = "postgres"
    password: str = "LandShark12"
    database: str = "postgres"


@dataclass
class DatabaseClient:
    config: DatabaseConfig

    @contextmanager
    def connect(self) -> Generator[PGConnection, None, None]:
        conn: PGConnection | None = None
        try:
            LOGGER.debug("Opening database connection to %s", self.config.host)
            conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                dbname=self.config.database,
                sslmode="require",
                cursor_factory=RealDictCursor,
            )
            yield conn
        finally:
            if conn is not None:
                conn.close()

    def query(self, sql: str, params: Sequence | None = None) -> Iterable[dict]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                if cur.description:
                    return cur.fetchall()
                return []

    def query_value(self, sql: str, params: Sequence | None = None):
        results = self.query(sql, params)
        if not results:
            return None
        row = results[0]
        return next(iter(row.values()))


def main() -> None:
    client = DatabaseClient(DatabaseConfig())
    LOGGER.info("Connected. Running health check query...")
    count = client.query_value("select count(*) from lots;")
    LOGGER.info("lots table row count: %s", count)


if __name__ == "__main__":
    main()
