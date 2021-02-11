from project import logger, r
from project.harzinfo_loader import HarzinfoLoader
from project.api_client import ApiClient
import json
import hashlib


class Importer:
    def __init__(self):
        self.harzinfo_loader = HarzinfoLoader()
        self.api_client = ApiClient()
        self.stored_event_mapping = r.hgetall("event_mapping")
        self.stored_event_hashes = r.hgetall("event_hashes")
        self.stored_place_mapping = r.hgetall("place_mapping")
        self.stored_place_hashes = r.hgetall("place_hashes")
        self.new_event_count = 0
        self.updated_event_count = 0
        self.deleted_event_count = 0
        self.unchanged_event_count = 0
        self.uids = set()
        self.externalIds = set()
        self.place_hashes = set()
        self.categories = dict()
        self.category_mapping = {
            "Ausstellung/Kunst": "Art",
            "Führung/Besichtigung": "Tour",
            "Comedy": "Comedy",
            "Konzert/Musik": "Music",
            "Theater": "Theater",
            "Genuss/Gourmet": "Dining",
            "Gesundheit/Wellness": "Fitness",
            "Kinder/Jugend": "Family",
            "Markt/Flohmarkt": "Shopping",
            "Sport": "Sports",
            "Kabarett": "Art",
            "Musical": "Theater",
            "Weihnachtsmärkte": "Festival",
            "Stadt- und Volksfeste": "Festival",
            "Sonstiges": "Other",
            "Vortrag/Lesung": "Lecture",
        }

    def run(self):
        self._load_categories()
        self._import_cities()
        self._purge_events()
        self._purge_places()
        logger.info(
            f"Events: {self.unchanged_event_count} unchanged, {self.new_event_count} new, {self.updated_event_count} updated, {self.deleted_event_count} deleted"
        )

    def _load_categories(self):
        try:
            category_list = self.api_client.get_categories()
            self.categories = {c["name"]: {"id": c["id"]} for c in category_list}
        except Exception:
            logger.error("Categories Exception", exc_info=True)

    def _import_cities(self):
        for city in self.harzinfo_loader.cities:
            try:
                self._import_city(city)
            except Exception:
                logger.error("City Exception", exc_info=True)

    def _import_city(self, city: dict):
        organizer_name = city["short_name"] or city["title"]
        logger.info(f"Importing city {organizer_name}")
        organizer_id = self.api_client.upsert_organizer(organizer_name)

        response = self.harzinfo_loader.load_events(city)
        result = response["result"]

        for item in result:
            try:
                self._import_event(item, organizer_id)
            except Exception:
                logger.error("Item Exception", exc_info=True)

    def _import_event(self, item: dict, organizer_id: int) -> int:

        # Check for duplicates
        uid = str(item["uid"])
        externalId = item["externalId"]

        if uid in self.uids:
            logger.warn(f"Duplicate UID {uid}")
            return 0

        if externalId in self.externalIds:
            logger.warn(f"Duplicate externalId {externalId}")
            return 0

        self.uids.add(uid)
        self.externalIds.add(externalId)

        # Compare to stored hashes
        item_hash = self._hash_dict(item)
        event_id = 0

        if uid in self.stored_event_mapping:
            event_id = int(self.stored_event_mapping[uid])
            logger.debug(f"Found event for uid {uid} in mapping: {event_id}.")

            stored_event_hash = self.stored_event_hashes.get(uid, None)
            if item_hash == stored_event_hash:
                logger.debug("Event did not change. Nothing to do.")
                self.unchanged_event_count = self.unchanged_event_count + 1
                return event_id

        # Place
        place_id = self._import_place(organizer_id, item)

        # Event
        event = dict()
        event["external_link"] = self.harzinfo_loader.base_url + item["link"]
        event["rating"] = int(item["rating"])
        event["name"] = item["title"]
        event["start"] = item["date"] + ":00"
        event["booked_up"] = item["bookedUp"]
        event["price_info"] = item["price"]
        event["status"] = "cancelled" if item["canceled"] else "scheduled"
        event["place"] = {"id": place_id}
        event["organizer"] = {"id": organizer_id}
        self._add_categories(event, item)

        if event_id > 0:
            logger.debug("Event did change. Updating..")
            self.api_client.update_event(event_id, event)
            r.hset("event_hashes", uid, item_hash)
            self.updated_event_count = self.updated_event_count + 1
        else:
            logger.debug(f"Event for uid {uid} not in mapping. Inserting..")
            event_id = self.api_client.insert_event(event)
            r.hset("event_mapping", uid, event_id)
            r.hset("event_hashes", uid, item_hash)
            self.new_event_count = self.new_event_count + 1

        return event_id

    def _import_place(self, organizer_id: int, item: dict) -> int:
        place_name = item["location"]

        place = dict()
        place["name"] = place_name

        if "latitude" in item and "longitude" in item:
            meeting_point_latitude = item["latitude"]
            meeting_point_longitude = item["longitude"]
            if meeting_point_latitude and meeting_point_longitude:
                latitude = float(meeting_point_latitude)
                longitude = float(meeting_point_longitude)
                if latitude != 0 and longitude != 0:
                    location = dict()
                    location["latitude"] = latitude
                    location["longitude"] = longitude
                    place["location"] = location

        hash_key = f"{organizer_id}:{place_name}"
        place_hash = self._hash_dict(place)
        self.place_hashes.add(place_hash)

        if hash_key in self.stored_place_mapping:
            place_id = int(self.stored_place_mapping[hash_key])
            logger.debug(f"Found place {place_name} in mapping: {place_id}.")

            stored_place_hash = self.stored_place_hashes.get(hash_key, None)
            if place_hash == stored_place_hash:
                logger.debug("Place did not change. Nothing to do.")
            else:
                logger.debug("Place did change. Updating..")
                self.api_client.upsert_place(place_id, place)
                r.hset("place_hashes", hash_key, place_hash)
        else:
            logger.debug(f"Place {place_name} not in mapping. Inserting..")
            place_id = self.api_client.upsert_place(place)
            r.hset("place_mapping", hash_key, place_id)
            r.hset("place_hashes", hash_key, place_hash)

        return place_id

    def _add_categories(self, event: dict, item: dict):
        if "categories" not in item:
            return

        event_categories = list()
        tags = list()

        for item_category in item["categories"].values():
            is_category = False
            if item_category in self.category_mapping:
                category_name = self.category_mapping[item_category]
                if category_name in self.categories:
                    category = self.categories[category_name]
                    event_categories.append(category)
                    is_category = True

            if not is_category:
                tags.append(item_category)

        if len(event_categories) > 0:
            event["categories"] = event_categories

        if len(tags) > 0:
            event["tags"] = ",".join(tags)

    def _purge_events(self):
        for uid, event_id_str in self.stored_event_mapping.items():
            if uid in self.uids:
                continue
            self.api_client.delete_event(int(event_id_str))
            r.hdel("event_mapping", uid)
            r.hdel("event_hashes", uid)
            self.deleted_event_count = self.deleted_event_count + 1

    def _purge_places(self):
        for hash_key in self.stored_place_mapping.keys():
            if hash_key in self.place_hashes:
                continue

            r.hdel("place_mapping", hash_key)
            r.hdel("place_hashes", hash_key)

    def _hash_dict(self, item: dict):
        item_str = json.dumps(item, sort_keys=True, ensure_ascii=True)
        return hashlib.md5(item_str.encode("utf-8")).hexdigest()
