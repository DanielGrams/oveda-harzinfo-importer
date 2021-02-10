import json
from urllib import request
from os import path
from authlib.integrations.requests_client import OAuth2Session
from project.api_client import ApiClient
from project.session_client import SessionClient
from project import logger
import datetime

base_url = "https://www.harzinfo.de"
url = base_url + "/?ndssearch=fullsearch&no_cache=1&L=0"

# TODO: Aus Datenbank/Redis lesen/schreiben

api_url = "http://127.0.0.1:5000"
organization_id = 1
client_id = "twnkgfilXnZxKQ03IplAGlam"
client_secret = "RjsCBHVhqORSOwgMbArP5BCDul4NPUkojLXwAUN87FdRW3DP"
scope = "event:write organizer:write place:write"

token = {
    "access_token": "gjnzfwRJGzqt3H8c3T0lrBXnvhJFR6ELLKkM1PW22j",
    "expires_in": 864000,
    "refresh_token": "VmmhskxGp2sMTPLJmzbdvcezIXDv7LXIFnB45MeGaVa8eLeo",
    "scope": "event:write organizer:write place:write",
    "token_type": "Bearer",
}


def update_token(token, refresh_token=None, access_token=None):
    pass
    # # TODO: Aus Datenbank/Redis lesen/schreiben
    # if refresh_token:
    #     item = OAuth2Token.find(name=name, refresh_token=refresh_token)
    # elif access_token:
    #     item = OAuth2Token.find(name=name, access_token=access_token)
    # else:
    #     return

    # item.access_token = token["access_token"]
    # item.refresh_token = token.get("refresh_token")
    # item.expires_at = token["expires_at"]
    # item.save()


session = OAuth2Session(
    client_id, client_secret, scope=scope, token=token, update_token=update_token
)
session_client = SessionClient(session, api_url + "/api/v1")
api_client = ApiClient(session_client, organization_id)


def response_from_url(city):
    body = request_object
    body["searchFilter"]["ndsdestinationdataevent"]["city"] = {
        str(city["id"]): city["short_name"] or city["title"]
    }
    # TODO: datum setzen
    req = request.Request(url, data=bytes(json.dumps(body), encoding="utf-8"))
    req.add_header("Content-Type", "application/json")
    return request.urlopen(req)


def load_json(debug, city):
    if debug:
        filename = "tmp/hi_%d.json" % (city["id"])

        if not path.exists(filename):
            response = response_from_url(city)
            with open(filename, "wb") as text_file:
                text_file.write(response.read())

        with open(filename) as json_file:
            return json.load(json_file)
    else:
        response = response_from_url(city)
        return json.load(response)


def parse_date_time_str(date_time_str):
    if not date_time_str:
        return None

    return datetime.datetime.fromisoformat(date_time_str + ":00")


def scrape(debug, city):
    # Organizer
    organizer_name = city["short_name"] or city["title"]
    logger.info(f"Scraping city {organizer_name}")
    organizer_id = api_client.upsert_organizer(organizer_name)

    response = load_json(debug, city)
    result = response["result"]
    uids = set()
    externalIds = set()
    place_name_to_id = dict()

    for item in result:
        try:
            uid = str(item["uid"])
            externalId = item["externalId"]

            if uid in uids:
                logger.warn(f"Duplicate UID {uid}")
                continue

            uids.add(uid)

            if externalId in externalIds:
                logger.warn(f"Duplicate externalId {externalId}")
                continue

            externalIds.add(externalId)

            # Place
            place_name = item["location"]

            if place_name in place_name_to_id:
                place_id = place_name_to_id[place_name]
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

                place_id = api_client.upsert_place(place)
                place_name_to_id[place_name] = place_id

            # Event
            event = dict()
            event["external_link"] = base_url + item["link"]
            event["rating"] = int(item["rating"])
            event["name"] = item["title"]
            event["start"] = item["date"] + ":00"
            event["booked_up"] = item["bookedUp"]
            event["price_info"] = item["price"]
            event["status"] = "cancelled" if item["canceled"] else "scheduled"

            # TODO: Kategorien aus API lesen und mappen

            # API: prüfen, ob Event vorhanden ist
            # API: Event POST oder PUT
            event["place"] = {"id": place_id}
            event["organizer"] = {"id": organizer_id}
            logger.debug("Got my event together")

            # TODO: In Redis merken, dass wir das Event eingelesen haben

        except Exception as e:
            logger.error("Item Exception", e)

    # API: Events entfernen, die wir nicht geparsed haben


url = "https://www.harzinfo.de/?ndssearch=fullsearch&no_cache=1&L=0"

with open("scrape_hi_req.json") as json_file:
    request_object = json.load(json_file)

with open("scrape_hi_cities.json") as json_file:
    cities = json.load(json_file)

for city in cities.values():
    try:
        scrape(True, city)
    except Exception as e:
        logger.error("City Exception", e)


# data = {
#   "name": "Juhu3"
# }
# resp = client.post(f"{api_url}/api/v1/organizations/{organization_id}/places", data=data)
# print(resp.json())
