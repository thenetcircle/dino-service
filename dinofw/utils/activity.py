import logging
from datetime import datetime
from uuid import uuid4 as uuid

from dinofw.utils.config import ConfigKeys

logger = logging.getLogger(__name__)


class ActivityBuilder(object):
    @staticmethod
    def enrich(env, extra: dict) -> dict:
        if "id" in extra:
            ActivityBuilder.warn_field("id", extra)
        else:
            extra["id"] = str(uuid())

        if "published" in extra:
            ActivityBuilder.warn_field("published", extra)
        else:
            extra["published"] = datetime.utcnow().strftime(ConfigKeys.DEFAULT_DATE_FORMAT)

        if "provider" in extra:
            ActivityBuilder.warn_field("provider", extra)
        else:
            extra["provider"] = {
                "id": env.config.get(ConfigKeys.ENVIRONMENT, default="testing")
            }

        return extra

    @staticmethod
    def warn_field(field: str, extra: dict) -> None:
        logger.warning(f"'{field}' field already exists in activity, not adding new: {extra}")
