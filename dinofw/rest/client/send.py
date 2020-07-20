import logging
import sys
import traceback
from abc import ABC

import eventlet
from flask import request

from dinofw import environ
from dinofw import utils
from dinofw.utils.activity import ActivityBuilder


class BaseClientResource(ABC):
    def __init__(self, env):
        self.env = env
        self.request = request
        self.logger = logging.getLogger(__name__)


class SendResource(BaseClientResource):
    def async_post(self, json):
        self.logger.debug(f"POST request: {str(json)}")

        if "payload" not in json:
            raise RuntimeError("no key [payload] in json message")

        msg_content = json.get("payload")
        if msg_content is None or len(msg_content.strip()) == 0:
            raise RuntimeError("payload may not be blank")

        target_ids = json.get("users")
        group_id = json.get("group_id")
        group_name = json.get("group_name")

        user_id = str(json.get("user_id", 0))
        user_name = utils.b64d(json.get("user_name", utils.b64e("admin")))
        namespace = json.get("namespace", "/")

        data = ActivityBuilder.activity_for_message(user_id, user_name)
        data["target"] = {
            "objectType": "group",
            "id": group_id,
            "displayName": group_name,
            "url": namespace,
        }

        data["object"] = {
            "objectType": "message",
            "content": msg_content
        }

        for target_id in target_ids:
            try:
                environ.env.out_of_scope_emit(
                    "message",
                    data,
                    room=str(target_id),
                    json=True,
                    namespace="/",
                    broadcast=True,
                )
            except Exception as e:
                self.logger.error(f"could not /send message to target {target_id}: {str(e)}")
                self.logger.exception(traceback.format_exc())
                environ.env.capture_exception(sys.exc_info())

    def do_post(self):
        is_valid, msg, json = self.validate_json(self.request, silent=False)
        if not is_valid:
            self.logger.error(f"invalid json: {msg}")
            raise RuntimeError("invalid json")

        if json is None:
            raise RuntimeError("no json in request")
        if not isinstance(json, dict):
            raise RuntimeError("need a dict")

        eventlet.spawn_n(self.async_post, dict(json))

    def validate_json(self, req, silent: bool):
        try:
            data = req.get_data()
        except Exception as e:
            self.logger.error(f"could not get data from request: {str(e)}")
            self.logger.exception(traceback.format_exc())
            environ.env.capture_exception(sys.exc_info())
            return False, str(e), None

        return True, "", data
