import asyncio
from contextlib import asynccontextmanager
from typing import Optional

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


class DatabaseManager:
    def __init__(self, db_url):
        self.db_url = db_url
        self.pool: Optional[AsyncConnectionPool] = None

    async def initialize(self):
        try:
            self.pool = AsyncConnectionPool(
                self.db_url,
                min_size=1,
                max_size=10,
                kwargs={"row_factory": dict_row, "autocommit": False},
            )

            await self.pool.open()
        except Exception as e:
            raise

    async def close(self):
        if self.pool:
            await self.pool.close()

    @asynccontextmanager
    async def get_connection(self):
        if not self.pool:
            raise RuntimeError("Connection pool is not initialized.")

        async with self.pool.connection() as conn:
            yield conn

    @asynccontextmanager
    async def get_transaction(self):
        async with self.get_connection() as conn:
            async with conn.transaction():
                yield conn
