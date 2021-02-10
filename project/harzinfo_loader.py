from urllib import request
import os
from project import data_path, today
import json
from dateutil.relativedelta import relativedelta


class HarzinfoLoader:
    def __init__(self):
        self.use_tmp = bool(os.getenv("USE_TMP", "0"))
        self.base_url = "https://www.harzinfo.de"
        self.url = self.base_url + "/?ndssearch=fullsearch&no_cache=1&L=0"

        with open(os.path.join(data_path, "request.json")) as json_file:
            self.request = json.load(json_file)

        with open(os.path.join(data_path, "cities.json")) as json_file:
            self.cities = json.load(json_file).values()

    def load_events(self, city: dict):
        if self.use_tmp:
            filename = "tmp/hi_%d.json" % (city["id"])

            if not os.path.exists(filename):
                response = self._load_events_from_url(city)
                with open(filename, "wb") as text_file:
                    text_file.write(response.read())

            with open(filename) as json_file:
                return json.load(json_file)
        else:
            response = self._load_events_from_url(city)
            return json.load(response)

    def _load_events_from_url(self, city: dict):
        body = self.request
        filter = body["searchFilter"]["ndsdestinationdataevent"]
        filter["city"] = {str(city["id"]): city["short_name"] or city["title"]}

        start_date = today.strftime("%Y-%m-%d")
        end_date = (today + relativedelta(years=1)).strftime("%Y-%m-%d")
        filter["startDate"] = start_date
        filter["endDate"] = end_date
        filter["searchWithoutDateBackupStart"] = start_date
        filter["searchWithoutDateBackupEnd"] = end_date

        req = request.Request(self.url, data=bytes(json.dumps(body), encoding="utf-8"))
        req.add_header("Content-Type", "application/json")
        return request.urlopen(req)
