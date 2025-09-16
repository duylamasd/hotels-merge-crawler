import asyncio
from typing import Any

from psycopg.types.json import Jsonb

from database import DatabaseManager


class Persistent:
    def __init__(self, db: DatabaseManager):
        self.db = db

    async def sync_hotels(self, hotels: list[Any]):
        if not self.db.pool:
            raise RuntimeError("Database connection pool is not initialized.")
        try:
            async with self.db.get_transaction() as conn:
                values = [
                    (
                        hotel["hotel_id"],
                        hotel["destination_id"],
                        hotel["name"],
                        Jsonb(hotel["location"]),
                        hotel["description"],
                        Jsonb(hotel["images"]),
                        Jsonb(hotel["amenities"]),
                        hotel["booking_conditions"],
                    )
                    for hotel in hotels
                ]

                async with conn.cursor() as cur:
                    await cur.execute("DELETE FROM hotels")
                    async with cur.copy(
                        "COPY hotels (hotel_id, destination_id, name, location, description, images, amenities, booking_conditions) FROM STDIN WITH (FORMAT binary)"
                    ) as copy:
                        for row in values:
                            await copy.write_row(row)
        except Exception as e:
            raise
