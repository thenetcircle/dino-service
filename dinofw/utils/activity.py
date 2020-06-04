import logging
import traceback
import sys
from datetime import datetime
from uuid import uuid4 as uuid
from activitystreams import parse as as_parser

from dinofw import environ
from dinofw import utils
from dinofw.config import ConfigKeys

logger = logging.getLogger(__name__)


class ActivityBuilder:
    @staticmethod
    def activity_for_login(
        user_id: str,
        user_name: str,
        encode_attachments: bool = True,
        include_history: bool = False,
    ) -> dict:
        try:
            sid = environ.env.request.sid
        except Exception as e:
            logger.error('could not get sid for user "{}": {}'.format(user_id, str(e)))
            logger.exception(traceback.format_exc())
            environ.env.capture_exception(sys.exc_info())
            sid = ""

        response = ActivityBuilder.enrich(
            {
                "actor": {
                    "id": user_id,
                    "displayName": utils.b64e(user_name),
                    "content": sid,
                    "attachments": utils.get_user_info_attachments_for(
                        user_id, encode_atts=encode_attachments, include_ua=True
                    ),
                },
                "verb": "login",
            }
        )

        if include_history:
            groups = environ.env.groups.groups_for(user_id, limit=100)
            if len(groups) > 0:
                group_attachments = ActivityBuilder.format_group_attachments(groups)
                response["object"] = {
                    "objectType": "groups",
                    "attachments": group_attachments,
                }

        return response

    @staticmethod
    def format_group_attachments(groups: list):
        # TODO: dino handles read/unread or do communities?
        attachments = list()

        for group in groups:
            attachments.append(
                {
                    "id": group["id"],
                    "objectType": "group",
                    "author": group["owner"],
                    "published": group["created"],
                    "updated": group["updated"],
                    "displayName": group["name"],
                    "content": group["users_list"],
                    "summary": group["preview"],
                }
            )

        return attachments

    @staticmethod
    def activity_for_message(user_id, user_name):
        return dict()

    @staticmethod
    def enrich(extra: dict) -> dict:
        if "id" in extra:
            ActivityBuilder.warn_field("id", extra)
        else:
            extra["id"] = str(uuid())

        if "published" in extra:
            ActivityBuilder.warn_field("published", extra)
        else:
            extra["published"] = datetime.utcnow().strftime(
                ConfigKeys.DEFAULT_DATE_FORMAT
            )

        if "provider" in extra:
            ActivityBuilder.warn_field("provider", extra)
        else:
            extra["provider"] = {
                "id": environ.env.config.get(ConfigKeys.ENVIRONMENT, "testing")
            }

        return extra

    @staticmethod
    def warn_field(field: str, extra: dict) -> None:
        logger.warning(
            '"{}" field already exists in activity, not adding new: {}'.format(
                field, extra
            )
        )
