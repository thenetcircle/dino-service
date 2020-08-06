import logging
from abc import ABC, abstractmethod
from base64 import b64decode
from base64 import b64encode
from typing import List

from dinofw.db.storage.schemas import MessageBase
from dinofw.utils.exceptions import UserExistsException

logger = logging.getLogger(__name__)


def b64d(s: str) -> str:
    if s is None:
        return ""

    s = s.strip()
    if len(s) == 0:
        return ""

    try:
        return str(b64decode(bytes(s, "utf-8")), "utf-8")
    except Exception as e:
        logger.error(f"could not b64decode because: {str(e)}, value was: {str(s)}")
    return ""


def b64e(s: str) -> str:
    if s is None:
        return ""

    s = s.strip()
    if len(s) == 0:
        return ""

    try:
        return str(b64encode(bytes(s, "utf-8")), "utf-8")
    except Exception as e:
        logger.error(f"could not b64encode because: {str(e)}, value was: {str(s)}")
    return ""


class IPublisher(ABC):
    @abstractmethod
    def message(
        self, group_id: str, user_id: int, message: MessageBase, user_ids: List[int]
    ) -> None:
        """pass"""
