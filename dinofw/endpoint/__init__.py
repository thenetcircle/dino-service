from abc import ABC
from abc import abstractmethod
from typing import List

from dinofw.db.rdbms.schemas import GroupBase
from dinofw.db.storage.schemas import MessageBase
from dinofw.utils.config import EventTypes
from dinofw.utils.convert import to_int


class IPublishHandler(ABC):
    @abstractmethod
    def delete_attachments(
        self,
        group_id: str,
        attachments: List[MessageBase],
        user_ids: List[int],
        now: float
    ) -> None:
        """
        publish a list of attachments that has been deleted
        """


class IServerPublishHandler(IPublishHandler, ABC):
    pass


class IClientPublishHandler(IPublishHandler, ABC):
    @abstractmethod
    def message(self, message: MessageBase, user_ids: List[int], group: GroupBase) -> None:
        """pass"""

    @abstractmethod
    def edit(self, message: MessageBase, user_ids: List[int]) -> None:
        """pass"""

    @abstractmethod
    def attachment(self, attachment: MessageBase, user_ids: List[int], group: GroupBase) -> None:
        """pass"""

    @abstractmethod
    def group_change(self, group_base: GroupBase, user_ids: List[int]) -> None:
        """pass"""

    @abstractmethod
    def join(self, group_id: str, user_ids: List[int], joiner_ids: List[int], now: float) -> None:
        """pass"""

    @abstractmethod
    def leave(self, group_id: str, user_ids: List[int], leaver_id: int, now: float) -> None:
        """pass"""

    @staticmethod
    def event_for_delete_attachments(
        group_id: str,
        attachments: List[MessageBase],
        now: float
    ) -> dict:
        data = IClientPublishHandler.create_simple_event(
            event_type=EventTypes.DELETE_ATTACHMENT,
            group_id=group_id,
            now=now,
        )

        data["message_ids"] = [att.message_id for att in attachments]
        data["file_ids"] = [att.file_id for att in attachments]

        return data

    @staticmethod
    def create_simple_event(
            event_type: str,
            group_id: str,
            now: float,
            user_id: int = None,
            user_ids: List[int] = None
    ) -> dict:
        data = {
            "event_type": event_type,
            "created_at": to_int(now),
            "group_id": group_id
        }

        if user_id is not None:
            data["user_id"] = str(user_id)
        if user_ids is not None:
            data["user_ids"] = [str(uid) for uid in user_ids]

        return data


class IClientPublisher(ABC):
    @abstractmethod
    def send(self, user_id: int, fields: dict) -> None:
        """
        publish a bunch of fields to the configured stream
        """


class IServerPublisher(ABC):
    @abstractmethod
    def send(self, message: dict) -> None:
        """pass"""
