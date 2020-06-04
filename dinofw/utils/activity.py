import logging
from datetime import datetime
from uuid import uuid4 as uuid

from dinofw import environ
from dinofw.config import ConfigKeys

logger = logging.getLogger(__name__)


class ActivityBuilder:
    @staticmethod
    def activity_for_login(user_id: str, user_name: str, threads: list):
        return threads

    @staticmethod
    def activity_for_message(user_id, user_name):
        return dict()

    @staticmethod
    def enrich(extra: dict) -> dict:
        if 'id' in extra:
            ActivityBuilder.warn_field('id', extra)
        else:
            extra['id'] = str(uuid())

        if 'published' in extra:
            ActivityBuilder.warn_field('published', extra)
        else:
            extra['published'] = datetime.utcnow().strftime(ConfigKeys.DEFAULT_DATE_FORMAT)

        if 'provider' in extra:
            ActivityBuilder.warn_field('provider', extra)
        else:
            extra['provider'] = {
                'id': environ.env.config.get(ConfigKeys.ENVIRONMENT, 'testing')
            }

        return extra

    @staticmethod
    def warn_field(field: str, extra: dict) -> None:
        logger.warning('"{}" field already exists in activity, not adding new: {}'.format(field, extra))
