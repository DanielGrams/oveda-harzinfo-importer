from project import logger
from project.session_client import SessionClient


class ApiClient:
    def __init__(self, session: SessionClient, organization_id: int):
        self.session_client = session
        self.organization_id = organization_id

    def insert_organizer(self, name: str) -> int:
        logger.debug(f"Insert organizer {name}")
        data = {"name": name}
        response = self.session_client.post(
            f"/organizations/{self.organization_id}/organizers", data=data
        )
        organizer = response.json()
        return organizer["id"]

    def upsert_organizer(self, name: str) -> int:
        logger.debug(f"Upsert organizer {name}")
        response = self.session_client.get(
            f"/organizations/{self.organization_id}/organizers?name={name}"
        )
        pagination = response.json()
        organizer = self._find_item_in_pagination(pagination, name)

        if not organizer:
            logger.debug(f"Organizer {name} does not exist")
            return self.insert_organizer(name)

        organizer_id = organizer["id"]
        logger.debug(f"Organizer {organizer_id} {name} already exists. No need to update.")
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

    def _find_item_in_pagination(self, pagination: dict, name: str) -> dict:
        for item in pagination["items"]:
            if item["name"] == name:
                return item

        return None
