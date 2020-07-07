import logging
import sys
import traceback

import eventlet
from flask import request

from dinofw import environ
from dinofw import utils
from dinofw.rest.base import BaseResource
from dinofw.utils.activity import ActivityBuilder

logger = logging.getLogger(__name__)


def fail(error_message):
    return {"status": "FAIL", "message": error_message}


class SendResource(BaseResource):
    def __init__(self):
        super(SendResource, self).__init__()
        self.request = request

    def async_post(self, json):
        logger.debug(f"POST request: {str(json)}")

        if "content" not in json:
            raise RuntimeError("no key [content] in json message")

        msg_content = json.get("content")
        if msg_content is None or len(msg_content.strip()) == 0:
            raise RuntimeError("content may not be blank")

        user_id = str(json.get("user_id", 0))
        user_name = utils.b64d(json.get("user_name", utils.b64e("admin")))
        object_type = json.get("object_type")
        target_id = str(json.get("target_id"))
        namespace = json.get("namespace", "/ws")
        target_name = json.get("target_name")

        data = ActivityBuilder.activity_for_message(user_id, user_name)
        data["target"] = {
            "objectType": object_type,
            "id": target_id,
            "displayName": target_name,
            "url": namespace,
        }
        data["object"] = {"content": msg_content}

        # TODO: save to db

        if not environ.env.cache.user_is_in_multicast(target_id):
            logger.info(f"user {target_id} is offline, dropping message: {str(json)}")
            return

        try:
            environ.env.out_of_scope_emit(
                "message",
                data,
                room=target_id,
                json=True,
                namespace="/ws",
                broadcast=True,
            )
        except Exception as e:
            logger.error(f"could not /send message to target {target_id}: {str(e)}")
            logger.exception(traceback.format_exc())
            environ.env.capture_exception(sys.exc_info())

    def do_post(self):
        is_valid, msg, json = self.validate_json(self.request, silent=False)
        if not is_valid:
            logger.error(f"invalid json: {msg}")
            raise RuntimeError("invalid json")

        if json is None:
            raise RuntimeError("no json in request")
        if not isinstance(json, dict):
            raise RuntimeError("need a dict")

        eventlet.spawn_n(self.async_post, dict(json))
