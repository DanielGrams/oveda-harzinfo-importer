from project import logger, r, now
from project.harzinfo_loader import HarzinfoLoader
from project.api_client import ApiClient
import json
import hashlib
import datetime


class Importer:
    def __init__(self):
        self.harzinfo_loader = HarzinfoLoader()
        self.api_client = ApiClient()
        self.start_time = None
        self.last_run = None
        self.stored_event_mapping = r.hgetall("event_mapping")
        self.stored_event_hashes = r.hgetall("event_hashes")
        self.stored_url_mapping = r.hgetall("url_mapping")
        self.stored_place_mapping = r.hgetall("place_mapping")
        self.stored_place_hashes = r.hgetall("place_hashes")
        self.stored_organizer_mapping = r.hgetall("organizer_mapping")
        self.stored_organizer_hashes = r.hgetall("organizer_hashes")
        self.new_event_count = 0
        self.updated_event_count = 0
        self.deleted_event_count = 0
        self.unchanged_event_count = 0
        self.uids_in_run = set()
        self.urls_in_run = set()
        self.place_hashes = set()
        self.organizer_hashes = set()
        self.categories = dict()
        self.event_type_mapping = {
            "ChildrensEvent": "Family",
            "ComedyEvent": "Comedy",
            "DanceEvent": "Dance",
            "EducationEvent": "Lecture",
            "ExhibitionEvent": "Exhibition",
            "Festival": "Festival",
            "FoodEvent": "Dining",
            "LiteraryEvent": "Book",
            "MusicEvent": "Music",
            "SportsEvent": "Sports",
            "TheaterEvent": "Theater",
        }
        self.event_status_mapping = {
            "EventScheduled": "scheduled",
            "EventCancelled": "cancelled",
            "EventMovedOnline": "movedOnline",
            "EventPostponed": "postponed",
            "EventRescheduled": "rescheduled",
        }

    def run(self):
        self._start_run()
        self._load_categories()
        self._import_events_from_sitemap()
        self._purge_events()
        self._purge_places()
        self._purge_organizers()
        self._finish_run()

    def _start_run(self):
        self.start_time = now

        if r.exists("last_run"):
            last_run_str = r.get("last_run")
            self.last_run = datetime.datetime.fromisoformat(last_run_str)

    def _finish_run(self):
        last_run_str = self.start_time.isoformat()
        r.set("last_run", last_run_str)

        logger.info(
            f"Events: {self.unchanged_event_count} unchanged, {self.new_event_count} new, {self.updated_event_count} updated, {self.deleted_event_count} deleted"
        )

    def _import_events_from_sitemap(self):
        sitememap_urls = self.harzinfo_loader.load_sitemap()

        for url, lastmod in sitememap_urls.items():
            try:
                self._import_event_from_sitemap(url, lastmod)
            except Exception:
                logger.error(f"Event Exception {url}", exc_info=True)

    def _import_event_from_sitemap(self, url: str, lastmod: str):
        logger.debug(f"Loading event at {url} from {lastmod}")

        if self.last_run:
            last_modified = datetime.datetime.fromisoformat(lastmod)
            if last_modified < self.last_run:
                if url in self.stored_url_mapping:
                    uid = self.stored_url_mapping[url]
                    logger.debug(
                        "Event was not modified since last run. Nothing to do."
                    )
                    self.uids_in_run.add(uid)
                    self.urls_in_run.add(url)
                    self.unchanged_event_count = self.unchanged_event_count + 1
                    return

        self._import_event_from_url(url)

    def _import_event_from_url(self, url: str) -> int:
        self.urls_in_run.add(url)

        # Load event
        item = self.harzinfo_loader.load_event(url)
        if not item:
            logger.warn("No event data.")
            r.hset("url_mapping", url, "nodata")
            return 0

        # Check for duplicates
        uid = item["identifier"][0]

        if uid in self.uids_in_run:
            logger.warn(f"Duplicate UID {uid}")
            return 0

        self.uids_in_run.add(uid)

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

        # Organizer
        organizer_id = self._import_organizer(item)

        # Place
        place_id = self._import_place(item)

        # Event
        event = dict()
        event["external_link"] = item["url"]
        event["name"] = item["name"]
        event["description"] = item["description"]
        event["start"] = item["startDate"]
        event["place"] = {"id": place_id}
        event["organizer"] = {"id": organizer_id}
        self._import_event_status(event, item)
        self._add_categories(event, item)
        self._add_tags(event, item)

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
            r.hset("url_mapping", url, uid)
            self.new_event_count = self.new_event_count + 1

        return event_id

    def _import_event_status(self, event, item: dict) -> str:
        if "eventStatus" in item:
            eventStatus = item["eventStatus"]

            if eventStatus in self.event_status_mapping:
                event["status"] = self.event_status_mapping[eventStatus]
                return

            if eventStatus == "ausgebucht":
                event["booked_up"] = True

        event["status"] = "scheduled"

    def _load_categories(self):
        try:
            category_list = self.api_client.get_categories()
            self.categories = {c["name"]: {"id": c["id"]} for c in category_list}
        except Exception:
            logger.error("Categories Exception", exc_info=True)

    def _import_organizer(self, item: dict) -> int:
        organizer_item = item["author"]

        if len(item["organizer"]) > 0 and item["organizer"][0]:
            organizer_item = item["organizer"][0]

        organizer_name = organizer_item["name"]

        organizer = dict()
        organizer["name"] = organizer_name

        if "url" in organizer_item:
            organizer["url"] = organizer_item["url"]

        if "email" in organizer_item:
            organizer["email"] = organizer_item["email"]

        if "telephone" in organizer_item:
            organizer["phone"] = organizer_item["telephone"]

        if "faxNumber" in organizer_item:
            organizer["fax"] = organizer_item["faxNumber"]

        if "address" in organizer_item:
            organizer["location"] = self._import_location(organizer_item["address"])

        hash_key = organizer_name
        organizer_hash = self._hash_dict(organizer)
        self.organizer_hashes.add(organizer_hash)

        if hash_key in self.stored_organizer_mapping:
            organizer_id = int(self.stored_organizer_mapping[hash_key])
            logger.debug(
                f"Found organizer {organizer_name} in mapping: {organizer_id}."
            )

            stored_organizer_hash = self.stored_organizer_hashes.get(hash_key, None)
            if organizer_hash == stored_organizer_hash:
                logger.debug("Organizer did not change. Nothing to do.")
            else:
                logger.debug("Organizer did change. Updating..")
                self.api_client.update_organizer(organizer_id, organizer)
                r.hset("organizer_hashes", hash_key, organizer_hash)
        else:
            logger.debug(f"Organizer {organizer_name} not in mapping. Inserting..")
            organizer_id = self.api_client.upsert_organizer(organizer)
            r.hset("organizer_mapping", hash_key, organizer_id)
            r.hset("organizer_hashes", hash_key, organizer_hash)

        return organizer_id

    def _import_place(self, item: dict) -> int:
        place_item = item["location"][0]
        place_name = place_item["name"]

        place = dict()
        place["name"] = place_name
        location = dict()

        if "address" in place_item:
            location = self._import_location(place_item["address"])

        if "coordinate" in item:
            lat_str, lon_str = item["coordinate"].split(",")
            latitude = float(lat_str)
            longitude = float(lon_str)
            if latitude != 0 and longitude != 0:
                location["latitude"] = latitude
                location["longitude"] = longitude

        place["location"] = location

        hash_key = place_name
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
                self.api_client.update_place(place_id, place)
                r.hset("place_hashes", hash_key, place_hash)
        else:
            logger.debug(f"Place {place_name} not in mapping. Inserting..")
            place_id = self.api_client.upsert_place(place)
            r.hset("place_mapping", hash_key, place_id)
            r.hset("place_hashes", hash_key, place_hash)

        return place_id

    def _import_location(self, address: dict) -> dict:
        location = dict()
        if "streetAddress" in address:
            location["street"] = address["streetAddress"]

        if "postalCode" in address:
            location["postalCode"] = address["postalCode"]

        if "addressLocality" in address:
            location["city"] = address["addressLocality"]

        if "addressCountry" in address:
            location["country"] = address["addressCountry"]
        return location

    def _add_categories(self, event: dict, item: dict):
        if "@type" not in item:
            return

        event_categories = list()

        for item_type in item["@type"]:
            if item_type in self.event_type_mapping:
                category_name = self.event_type_mapping[item_type]
                if category_name in self.categories:
                    category = self.categories[category_name]
                    event_categories.append(category)

        if len(event_categories) > 0:
            event["categories"] = event_categories

    def _add_tags(self, event: dict, item: dict):
        if "keywords" not in item:
            return

        tags = list()

        for keyword in item["keywords"].split(","):
            keyword = keyword.strip()
            if not keyword[0].islower():
                tags.append(keyword)

        if len(tags) > 0:
            event["tags"] = ",".join(tags)

    def _purge_events(self):
        for uid, event_id_str in self.stored_event_mapping.items():
            if uid in self.uids_in_run:
                continue
            self.api_client.delete_event(int(event_id_str))
            r.hdel("event_mapping", uid)
            r.hdel("event_hashes", uid)
            self.deleted_event_count = self.deleted_event_count + 1

        for url, uid in self.stored_url_mapping.items():
            if url in self.urls_in_run:
                continue
            r.hdel("url_mapping", url)

    def _purge_places(self):
        for hash_key in self.stored_place_mapping.keys():
            if hash_key in self.place_hashes:
                continue

            r.hdel("place_mapping", hash_key)
            r.hdel("place_hashes", hash_key)

    def _purge_organizers(self):
        for hash_key in self.stored_organizer_mapping.keys():
            if hash_key in self.organizer_hashes:
                continue

            r.hdel("organizer_mapping", hash_key)
            r.hdel("organizer_hashes", hash_key)

    def _hash_dict(self, item: dict):
        item_str = json.dumps(item, sort_keys=True, ensure_ascii=True)
        return hashlib.md5(item_str.encode("utf-8")).hexdigest()
