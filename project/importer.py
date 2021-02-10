from project import logger, r
from project.harzinfo_loader import HarzinfoLoader
from project.api_client import ApiClient


class Importer:
    def __init__(self):
        self.harzinfo_loader = HarzinfoLoader()
        self.api_client = ApiClient()
        self.event_mapping = r.hgetall("event_mapping")
        self.place_name_to_id = dict()
        self.uids = set()
        self.externalIds = set()
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
        for city in self.harzinfo_loader.cities:
            try:
                self._import_city(city)
            except Exception:
                logger.error("City Exception", exc_info=True)

    def _load_categories(self):
        try:
            category_list = self.api_client.get_categories()
            self.categories = {c["name"]: {"id": c["id"]} for c in category_list}
        except Exception:
            logger.error("Categories Exception", exc_info=True)

    def _import_city(self, city: dict):
        organizer_name = city["short_name"] or city["title"]
        logger.info(f"Importing city {organizer_name}")
        organizer_id = self.api_client.upsert_organizer(organizer_name)

        response = self.harzinfo_loader.load_events(city)
        result = response["result"]

        for item in result:
            try:
                place_id = self._import_place(item)
                self._import_event(item, organizer_id, place_id)
            except Exception:
                logger.error("Item Exception", exc_info=True)

        # TODO: Events, Organizer und Places entfernen, die wir nicht geparsed haben

    def _import_event(self, item: dict, organizer_id: int, place_id: int) -> int:
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

        if uid in self.event_mapping:
            event_id = int(self.event_mapping[uid])
            logger.debug(
                f"Found event for uid {uid} in mapping: {event_id}. Updating.."
            )
            self.api_client.update_event(event_id, event)
        else:
            logger.debug(f"Event for uid {uid} not in mapping. Inserting..")
            event_id = self.api_client.insert_event(event)
            r.hset("event_mapping", uid, event_id)

        return event_id

    def _import_place(self, item: dict) -> int:
        place_name = item["location"]

        if place_name in self.place_name_to_id:
            place_id = self.place_name_to_id[place_name]
            logger.debug(f"Place {place_id} {place_name} already upserted")
        else:
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

            place_id = self.api_client.upsert_place(place)
            self.place_name_to_id[place_name] = place_id

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
