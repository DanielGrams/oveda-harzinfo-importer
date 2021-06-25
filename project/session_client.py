import json
import os
from typing import Any

from authlib.integrations.requests_client import OAuth2Session
from requests import Response

from project import logger, r


def _update_token(token, refresh_token=None, access_token=None):
    if token.get("refresh_token", None):
        r.hset("token", "refresh_token", token.get("refresh_token"))

    if token.get("access_token", None):
        r.hset("token", "access_token", token.get("access_token"))

    if token.get("expires_at", None):
        r.hset("token", "expires_at", token.get("expires_at"))
        r.hdel("token", "expires_in")


class UnprocessableEntityError(ValueError):
    def __init__(self, message, response):
        self.json = response.json()
        super().__init__(message)


class SessionClient:
    def __init__(self):
        self.base_url = os.getenv("API_URL") + "/api/v1"
        self.scope = "event:write organizer:write place:write"
        self.client_id = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("CLIENT_SECRET")
        self.token = self._load_token()
        self.session = OAuth2Session(
            self.client_id,
            self.client_secret,
            scope=self.scope,
            token=self.token,
            update_token=_update_token,
            token_endpoint_auth_method="client_secret_post",
            token_endpoint=os.getenv("API_URL") + "/oauth/token",
        )

    def complete_url(self, url: str) -> str:
        return self.base_url + url

    def status_code_or_raise(self, response: Response, code: int):
        logger.debug(f"Response: {response.status_code} {response.content}")
        if response.status_code == code:
            return

        msg = f"Expected {code}, but was {response.status_code}"

        if response.status_code == 422:
            raise UnprocessableEntityError(msg, response)

        raise ValueError(msg, response)

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

    def delete(self, url: str) -> Response:
        url = self.complete_url(url)
        logger.debug(f"DELETE {url}")
        response = self.session.delete(url)
        self.status_code_or_raise(response, 204)
        return response

    def _load_token(self) -> dict:
        token = r.hgetall("token")

        if "access_token" not in token:
            from_env = {
                "access_token": os.getenv("ACCESS_TOKEN"),
                "refresh_token": os.getenv("REFRESH_TOKEN"),
                "expires_in": -1,
                "scope": self.scope,
                "token_type": "Bearer",
            }
            r.hset("token", mapping=from_env)

        token = r.hgetall("token")

        if "expires_in" in token:
            token["expires_in"] = int(token["expires_in"])

        if "expires_at" in token:
            token["expires_at"] = int(token["expires_at"])

        return token
