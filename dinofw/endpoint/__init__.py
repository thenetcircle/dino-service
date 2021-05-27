from abc import ABC
from abc import abstractmethod
from typing import List

from dinofw.db.rdbms.schemas import GroupBase
from dinofw.db.storage.schemas import MessageBase
from dinofw.rest.queries import AbstractQuery


class EventTypes:
    JOIN = "join"
    LEAVE = "leave"
    GROUP = "group"
    READ = "read"
    EDIT = "edit"
    ATTACHMENT = "attachment"
    MESSAGE = "message"
    ACTION_LOG = "action_log"
    DELETE_ATTACHMENT = "delete_attachment"
    DELETE_MESSAGE = "delete_message"


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
    def message(self, message: MessageBase, user_ids: List[int]) -> None:
        """pass"""

    @abstractmethod
    def edit(self, message: MessageBase, user_ids: List[int]) -> None:
        """pass"""

    @abstractmethod
    def attachment(self, attachment: MessageBase, user_ids: List[int]) -> None:
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
            "created_at": now,
            "group_id": group_id
        }

        if user_id is not None:
            data["user_id"] = user_id
        if user_ids is not None:
            data["user_ids"] = user_ids

        return data

    @staticmethod
    def read_to_event(group_id: str, user_id: int, now: float):
        return {
            "event_type": EventTypes.READ,
            "group_id": group_id,
            "user_id": user_id,
            "read_at": now,
        }

    @staticmethod
    def message_base_to_event(message: MessageBase, event_type: EventTypes = EventTypes.MESSAGE):
        return {
            "event_type": event_type,
            "group_id": message.group_id,
            "sender_id": message.user_id,
            "file_id": message.file_id,
            "message_id": message.message_id,
            "message_payload": message.message_payload,
            "message_type": message.message_type,
            "updated_at": AbstractQuery.to_ts(message.updated_at, allow_none=True) or "",
            "created_at": AbstractQuery.to_ts(message.created_at),
        }

    @staticmethod
    def group_base_to_event(group: GroupBase, user_ids: List[int]) -> dict:
        return {
            "event_type": EventTypes.GROUP,
            "group_id": group.group_id,
            "name": group.name,
            "description": group.description,
            "created_at": AbstractQuery.to_ts(group.created_at),
            "updated_at": AbstractQuery.to_ts(group.updated_at, allow_none=True) or None,
            "last_message_time": AbstractQuery.to_ts(group.last_message_time, allow_none=True) or None,
            "last_message_overview": group.last_message_overview,
            "last_message_type": group.last_message_type,
            "last_message_user_id": group.last_message_user_id,
            "status": group.status,
            "group_type": group.group_type,
            "owner_id": group.owner_id,
            "meta": group.meta,
            "user_ids": user_ids,
        }


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
