from project import logger
from authlib.integrations.requests_client import OAuth2Session
from requests import Response
from typing import Any
import json


class SessionClient:
    def __init__(self, session: OAuth2Session, base_url: str):
        self.session = session
        self.base_url = base_url

    def complete_url(self, url: str) -> str:
        return self.base_url + url

    def status_code_or_raise(self, response: Response, code: int):
        logger.debug(f"Response: {response.content}")
        if response.status_code != code:
            raise ValueError(f"Expected {code}, but was {response.status_code}")

    def get(self, url: str) -> Response:
        url = self.complete_url(url)
        logger.debug(f"GET {url}")
        response = self.session.get(url)
        self.status_code_or_raise(response, 200)
        return response

    def post(self, url: str, data: Any) -> Response:
        url = self.complete_url(url)
        logger.debug(f"POST {url}\n{json.dumps(data)}")
        response = self.session.post(url, json=data)
        self.status_code_or_raise(response, 201)
        return response

    def put(self, url: str, data: Any) -> Response:
        url = self.complete_url(url)
        logger.debug(f"PUT {url}\n{json.dumps(data)}")
        response = self.session.put(url, json=data)
        self.status_code_or_raise(response, 204)
        return response
