import json
import os
from urllib import request

from bs4 import BeautifulSoup
from werkzeug.utils import secure_filename


class HarzinfoLoader:
    def __init__(self):
        self.use_tmp = os.getenv("USE_TMP", "False").lower() in ["true", "1"]
        self.base_url = "https://www.harzinfo.de"

    def load_sitemap(self):
        xml = self._load_data(
            self.base_url
            + "/sitemap.xml?sitemap=ndsdestinationdataevent&cHash=e286f0aef1548b7c25ffdcf9a075ca47"
        )
        xmlDict = {}
        soup = BeautifulSoup(xml, features="html.parser")
        url_tags = soup.find_all("url")

        for url_tag in url_tags:
            xmlDict[url_tag.findNext("loc").text] = url_tag.findNext("lastmod").text

        return xmlDict

    def load_event(self, absolute_url: str):
        html = self._load_data(absolute_url)
        soup = BeautifulSoup(html, features="html.parser")
        ld_json_string = soup.find("script", {"type": "application/ld+json"}).string
        ld_json_array = json.loads(ld_json_string)

        if len(ld_json_array) == 0:
            return None

        ld_json = ld_json_array[0]
        self._strip_ld_json(ld_json)

        if "description" in ld_json:
            desc_soup = BeautifulSoup(ld_json["description"], features="html.parser")
            for br in desc_soup.find_all("br"):
                br.replace_with("\n" + br.text)
            ld_json["description"] = desc_soup.text

        coordinate_div = soup.find("div", attrs={"data-position": True})
        if coordinate_div:
            ld_json["coordinate"] = coordinate_div["data-position"]

        return ld_json

    def _strip_ld_json(self, ld_json: dict) -> dict:
        for k, v in ld_json.items():
            if isinstance(v, dict):
                self._strip_ld_json(v)
            elif isinstance(v, str):
                ld_json[k] = v.strip()

    def _load_data(self, absolute_url: str):
        if self.use_tmp:
            filename = f"tmp/{secure_filename(absolute_url)}.txt"

            if not os.path.exists(filename):
                response = self._load_data_from_url(absolute_url)
                with open(filename, "wb") as text_file:
                    text_file.write(response.read())

            with open(filename) as data_file:
                return data_file.read()
        else:
            response = self._load_data_from_url(absolute_url)
            return response.read()

    def _load_data_from_url(self, absolute_url: str):
        req = request.Request(absolute_url)
        return request.urlopen(req)
