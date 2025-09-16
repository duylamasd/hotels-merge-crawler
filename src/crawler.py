from typing import Optional

import aiohttp

from database import DatabaseManager
from persistent import Persistent


class HotelCrawlerSource:
    def __init__(self, name, url):
        self.name = name
        self.url = url

    def __repr__(self):
        return f"HotelCrawlerSource(name={self.name}, url={self.url})"

    async def crawl(self, http_session: aiohttp.ClientSession):
        try:
            async with http_session.get(self.url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                else:
                    return None
        except Exception as e:
            raise


class HotelCrawlerSources:
    def __init__(self, db_manager: DatabaseManager):
        self.acme = HotelCrawlerSource(
            name="acme",
            url="https://5f2be0b4ffc88500167b85a0.mockapi.io/suppliers/acme",
        )
        self.patagonia = HotelCrawlerSource(
            name="patagonia",
            url="https://5f2be0b4ffc88500167b85a0.mockapi.io/suppliers/patagonia",
        )
        self.paperflies = HotelCrawlerSource(
            name="paperflies",
            url="https://5f2be0b4ffc88500167b85a0.mockapi.io/suppliers/paperflies",
        )
        self.http_session: Optional[aiohttp.ClientSession] = None
        self.db_manager = db_manager
        self.persistent = Persistent(db_manager)

    async def initialize(self):
        connector = aiohttp.TCPConnector(
            use_dns_cache=True,
        )
        self.http_session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=60),
        )

    async def cleanup(self):
        if self.http_session:
            await self.http_session.close()

    async def crawl_all(self):
        if not self.http_session:
            raise RuntimeError("HTTP session is not initialized.")
        try:
            acme_data = await self.acme.crawl(self.http_session)
            patagonia_data = await self.patagonia.crawl(self.http_session)
            paperflies_data = await self.paperflies.crawl(self.http_session)

            hotels = self.merge_data(acme_data, patagonia_data, paperflies_data)
            await self.persistent.sync_hotels(hotels)
        except Exception as e:
            raise

    def merge_name(self, acme_hotel, patagonia_hotel, paperflies_hotel):
        acme_name = acme_hotel["Name"] or ""
        patagonia_name = patagonia_hotel["name"] if patagonia_hotel else ""
        paperflies_name = paperflies_hotel["hotel_name"] if paperflies_hotel else ""
        name = acme_name or patagonia_name or paperflies_name or ""

        return name.strip()

    def merge_location(self, acme_hotel, patagonia_hotel, paperflies_hotel):
        location = {}
        paperflies_location = paperflies_hotel["location"] if paperflies_hotel else None

        acme_latitude = acme_hotel["Latitude"]
        patagonia_latitude = patagonia_hotel["lat"] if patagonia_hotel else None
        location["latitude"] = (
            acme_latitude
            if acme_latitude is not None and type(acme_latitude) == float
            else (
                patagonia_latitude
                if patagonia_latitude is not None and type(patagonia_latitude) == float
                else None
            )
        )

        acme_longitude = acme_hotel["Longitude"]
        patagonia_longitude = patagonia_hotel["lng"] if patagonia_hotel else None
        location["longitude"] = (
            acme_longitude
            if acme_longitude is not None and type(acme_longitude) == float
            else (
                patagonia_longitude
                if patagonia_longitude is not None
                and type(patagonia_longitude) == float
                else None
            )
        )

        acme_address = acme_hotel["Address"] or ""
        patagonia_address = patagonia_hotel["address"] if patagonia_hotel else ""
        paperflies_address = (
            paperflies_location["address"] if paperflies_location else ""
        )
        address = patagonia_address or paperflies_address or acme_address or ""
        location["address"] = address.strip()

        city = acme_hotel["City"] or ""
        location["city"] = city.strip()

        acme_country = acme_hotel["Country"] or ""
        paperflies_country = (
            paperflies_location["country"] if paperflies_location else ""
        )
        country = paperflies_country or acme_country or ""
        location["country"] = country.strip()

        return location

    def merge_description(self, acme_hotel, patagonia_hotel, paperflies_hotel):
        acme_description = acme_hotel["Description"] or ""
        patagonia_info = patagonia_hotel["info"] if patagonia_hotel else ""
        paperflies_details = paperflies_hotel["details"] if paperflies_hotel else ""
        description = paperflies_details or patagonia_info or acme_description or ""

        return description.strip()

    def merge_images(self, acme_hotel, patagonia_hotel, paperflies_hotel):
        images = {}
        patagonia_images = patagonia_hotel["images"] if patagonia_hotel else None
        paperflies_images = paperflies_hotel["images"] if paperflies_hotel else None

        patagonia_rooms_images = patagonia_images["rooms"] if patagonia_images else []
        paperflies_rooms_images = (
            paperflies_images["rooms"] if paperflies_images else []
        )
        patagonia_rooms = [
            {"link": image["url"], "description": image["description"]}
            for image in patagonia_rooms_images
        ]
        paperflies_rooms = [
            {"link": image["link"], "description": image["caption"]}
            for image in paperflies_rooms_images
        ]
        rooms = list(patagonia_rooms) + list(paperflies_rooms)
        images["rooms"] = rooms

        paperflies_site_images = paperflies_images["site"] if paperflies_images else []
        site = [
            {"link": image["link"], "description": image["caption"]}
            for image in paperflies_site_images
        ]
        images["site"] = list(site)

        patagonia_amenities_images = (
            patagonia_images["amenities"] if patagonia_images else []
        )
        amenities = [
            {"link": image["url"], "description": image["description"]}
            for image in patagonia_amenities_images
        ]
        images["amenities"] = list(amenities)

        return images

    def merge_amenities(self, acme_hotel, patagonia_hotel, paperflies_hotel):
        paperflies_amenities = (
            paperflies_hotel["amenities"] if paperflies_hotel else None
        )
        if paperflies_amenities is not None:
            for i, v in enumerate(paperflies_amenities["general"]):
                paperflies_amenities["general"][i] = paperflies_amenities["general"][
                    i
                ].strip()
            for i, v in enumerate(paperflies_amenities["room"]):
                paperflies_amenities["room"][i] = paperflies_amenities["room"][
                    i
                ].strip()

        return paperflies_amenities

    def merge_booking_conditions(self, acme_hotel, patagonia_hotel, paperflies_hotel):
        paperflies_booking = (
            paperflies_hotel["booking_conditions"] if paperflies_hotel else None
        )
        if paperflies_booking is not None:
            for i, v in enumerate(paperflies_booking):
                paperflies_booking[i] = paperflies_booking[i].strip()

        return paperflies_booking

    def merge_data(self, acme_data, patagonia_data, paperflies_data):
        hotels_dict = {}

        acme_map = {hotel["Id"]: hotel for hotel in acme_data} if acme_data else {}
        patagonia_map = (
            {hotel["id"]: hotel for hotel in patagonia_data} if patagonia_data else {}
        )
        paperflies_map = (
            {hotel["hotel_id"]: hotel for hotel in paperflies_data}
            if paperflies_data
            else {}
        )

        for hotel_id in acme_map.keys():
            hotel = {}
            hotel["hotel_id"] = hotel_id
            destination_id = acme_map[hotel_id]["DestinationId"]
            hotel["destination_id"] = str(destination_id)

            acme_hotel = acme_map[hotel_id]
            patagonia_hotel = patagonia_map.get(hotel_id)
            paperflies_hotel = paperflies_map.get(hotel_id)

            name = self.merge_name(acme_hotel, patagonia_hotel, paperflies_hotel)
            hotel["name"] = name

            location = self.merge_location(
                acme_hotel, patagonia_hotel, paperflies_hotel
            )
            hotel["location"] = location

            description = self.merge_description(
                acme_hotel, patagonia_hotel, paperflies_hotel
            )
            hotel["description"] = description

            images = self.merge_images(acme_hotel, patagonia_hotel, paperflies_hotel)
            hotel["images"] = images

            amenities = self.merge_amenities(
                acme_hotel, patagonia_hotel, paperflies_hotel
            )
            hotel["amenities"] = amenities

            booking_conditions = self.merge_booking_conditions(
                acme_hotel, patagonia_hotel, paperflies_hotel
            )
            hotel["booking_conditions"] = booking_conditions

            hotels_dict[hotel_id] = hotel

        for hotel_id in patagonia_map.keys():
            if hotel_id in hotels_dict:
                continue

            hotel = {}
            hotel["hotel_id"] = hotel_id

            patagonia_hotel = patagonia_map[hotel_id]
            paperflies_hotel = paperflies_map.get(hotel_id)

            destination_id = patagonia_hotel["destination"]
            hotel["destination_id"] = str(destination_id)

            name = self.merge_name(None, patagonia_hotel, paperflies_hotel)
            hotel["name"] = name

            location = self.merge_location(None, patagonia_hotel, paperflies_hotel)
            hotel["location"] = location

            description = self.merge_description(
                None, patagonia_hotel, paperflies_hotel
            )
            hotel["description"] = description

            images = self.merge_images(None, patagonia_hotel, paperflies_hotel)
            hotel["images"] = images

            amenities = self.merge_amenities(None, patagonia_hotel, paperflies_hotel)
            hotel["amenities"] = amenities

            booking_conditions = self.merge_booking_conditions(
                None, patagonia_hotel, paperflies_hotel
            )
            hotel["booking_conditions"] = booking_conditions

            hotels_dict[hotel_id] = hotel

        for hotel_id in paperflies_map.keys():
            if hotel_id in hotels_dict:
                continue

            hotel = {}
            hotel["hotel_id"] = hotel_id

            paperflies_hotel = paperflies_map[hotel_id]

            destination_id = paperflies_hotel["destination_id"]
            hotel["destination_id"] = str(destination_id)

            name = self.merge_name(None, None, paperflies_hotel)
            hotel["name"] = name

            location = self.merge_location(None, None, paperflies_hotel)
            hotel["location"] = location

            description = self.merge_description(None, None, paperflies_hotel)
            hotel["description"] = description

            images = self.merge_images(None, None, paperflies_hotel)
            hotel["images"] = images

            amenities = self.merge_amenities(None, None, paperflies_hotel)
            hotel["amenities"] = amenities

            booking_conditions = self.merge_booking_conditions(
                None, None, paperflies_hotel
            )
            hotel["booking_conditions"] = booking_conditions

            hotels_dict[hotel_id] = hotel

        hotels = list(hotels_dict.values())
        return hotels


class HotelCrawler:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.sources = HotelCrawlerSources(db_manager)

    async def initialize(self):
        await self.sources.initialize()
        await self.db_manager.initialize()

    async def cleanup(self):
        await self.sources.cleanup()

    async def crawl(self):
        await self.sources.crawl_all()
