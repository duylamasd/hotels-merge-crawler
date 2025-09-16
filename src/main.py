import asyncio
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv

from crawler import HotelCrawler
from database import DatabaseManager

load_dotenv()


@asynccontextmanager
async def create_crawler():
    db_url = os.environ.get("DB_URL")
    db_manager = DatabaseManager(db_url)
    crawler = HotelCrawler(db_manager)
    try:
        await crawler.initialize()
        yield crawler
    finally:
        await crawler.cleanup()


async def main():
    async with create_crawler() as crawler:
        await crawler.crawl()


if __name__ == "__main__":
    asyncio.run(main())
