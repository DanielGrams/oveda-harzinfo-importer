import os

from project import logger
from project.session_client import SessionClient, UnprocessableEntityError


class ApiClient:
    def __init__(self):
        self.session_client = SessionClient()
        self.organization_id = os.getenv("ORGANIZATION_ID")

    def get_categories(self) -> int:
        logger.debug("Get categories")
        response = self.session_client.get("/event-categories?per_page=50")
        pagination = response.json()
        return pagination["items"]

    def insert_organizer(self, data: dict) -> int:
        logger.debug(f"Insert organizer {data['name']}")
        response = self.session_client.post(
            f"/organizations/{self.organization_id}/organizers", data=data
        )
        organizer = response.json()
        return organizer["id"]

    def update_organizer(self, organizer_id: int, data: dict):
        logger.debug(f"Update organizer {organizer_id} {data['name']}")
        self.session_client.put(f"/organizers/{organizer_id}", data=data)

    def upsert_organizer(self, data: dict) -> int:
        name = data["name"]
        logger.debug(f"Upsert organizer {name}")
        response = self.session_client.get(
            f"/organizations/{self.organization_id}/organizers?name={name}"
        )
        pagination = response.json()
        organizer = self._find_item_in_pagination(pagination, name)

        if not organizer:
            logger.debug(f"Organizer {name} does not exist")
            return self.insert_organizer(data)

        organizer_id = organizer["id"]
        logger.debug(
            f"Organizer {organizer_id} {name} already exists. No need to update."
        )
        return organizer_id

    def insert_place(self, data: dict) -> int:
        logger.debug(f"Insert place {data['name']}")
        response = self.session_client.post(
            f"/organizations/{self.organization_id}/places", data=data
        )
        place = response.json()
        return place["id"]

    def update_place(self, place_id: int, data: dict):
        logger.debug(f"Update place {place_id} {data['name']}")
        self.session_client.put(f"/places/{place_id}", data=data)

    def upsert_place(self, data: dict) -> int:
        name = data["name"]
        logger.debug(f"Upsert place {name}")

        response = self.session_client.get(
            f"/organizations/{self.organization_id}/places?name={name}"
        )
        pagination = response.json()
        place = self._find_item_in_pagination(pagination, name)

        if not place:
            logger.debug(f"Place {name} does not exist")
            return self.insert_place(data)

        place_id = place["id"]
        logger.debug(f"Place {place_id} {name} already exists")
        self.update_place(place_id, data)
        return place_id

    def insert_event(self, data: dict) -> int:
        logger.debug(f"Insert event {data['name']}")

        try:
            response = self.session_client.post(
                f"/organizations/{self.organization_id}/events", data=data
            )
        except UnprocessableEntityError as e:
            if e.json["errors"][0]["field"] == "photo":
                logger.warn("Retrying without photo")
                del data["photo"]
                response = self.session_client.post(
                    f"/organizations/{self.organization_id}/events", data=data
                )
            else:
                raise

        event = response.json()
        return event["id"]

    def update_event(self, event_id: int, data: dict):
        logger.debug(f"Update event {event_id} {data['name']}")

        try:
            self.session_client.put(f"/events/{event_id}", data=data)
        except UnprocessableEntityError as e:
            if e.json["errors"][0]["field"] == "photo":
                logger.warn("Retrying without photo")
                del data["photo"]
                self.session_client.put(f"/events/{event_id}", data=data)
            else:
                raise

    def delete_event(self, event_id: int):
        logger.debug(f"Delete event {event_id}")
        self.session_client.delete(f"/events/{event_id}")

    def _find_item_in_pagination(self, pagination: dict, name: str) -> dict:
        for item in pagination["items"]:
            if item["name"] == name:
                return item

        return None
