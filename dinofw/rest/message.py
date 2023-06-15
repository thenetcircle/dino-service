from typing import List

from sqlalchemy.orm import Session

from dinofw.rest.base import BaseResource
from dinofw.rest.models import Message
from dinofw.rest.queries import AttachmentQuery
from dinofw.rest.queries import CreateAttachmentQuery
from dinofw.rest.queries import EditMessageQuery
from dinofw.rest.queries import MessageInfoQuery
from dinofw.rest.queries import MessageQuery
from dinofw.rest.queries import SendMessageQuery
from dinofw.utils import users_to_group_id
from dinofw.utils import utcnow_ts
from dinofw.utils.config import EventTypes, MessageTypes
from dinofw.utils.convert import message_base_to_message
from dinofw.utils.exceptions import NoSuchUserException
from dinofw.utils.exceptions import GroupIsFrozenException
from dinofw.utils.exceptions import QueryValidationError


class MessageResource(BaseResource):
    async def send_message_to_group(
        self, group_id: str, user_id: int, query: SendMessageQuery, db: Session
    ) -> Message:
        if self.env.db.is_group_frozen(group_id, db):
            raise GroupIsFrozenException(group_id)

        message = self.env.storage.store_message(group_id, user_id, query)

        self._user_sends_a_message(
            group_id,
            user_id=user_id,
            message=message,
            db=db,
            # don't increase unread if this is an unprocessed attachment, when the
            # attachment has been processed, the unread count will be increased
            should_increase_unread=query.message_type != MessageTypes.IMAGE,
            event_type=EventTypes.MESSAGE,
            mentions=query.mention_user_ids
        )

        return message_base_to_message(message)

    async def send_message_to_user(
        self, user_id: int, query: SendMessageQuery, db: Session
    ) -> Message:
        if query.receiver_id < 1:
            raise NoSuchUserException(query.receiver_id)

        group_id = users_to_group_id(user_id, query.receiver_id)
        group_is_frozen = self.env.db.is_group_frozen(group_id, db)

        # can be None if the group doesn't exist yet
        if group_is_frozen is not None and group_is_frozen:
            raise GroupIsFrozenException(group_id)

        group_id = self._get_or_create_group_for_1v1(user_id, query.receiver_id, db)
        return await self.send_message_to_group(group_id, user_id, query, db)

    async def messages_in_group(
        self, group_id: str, query: MessageQuery
    ) -> List[Message]:
        raw_messages = self.env.storage.get_messages_in_group(group_id, query)
        messages = list()

        for message_base in raw_messages:
            message = message_base_to_message(message_base)
            messages.append(message)

        return messages

    async def messages_for_user(
        self, group_id: str, user_id: int, query: MessageQuery, db: Session
    ) -> List[Message]:
        user_stats = self.env.db.get_user_stats_in_group(group_id, user_id, db)

        if user_stats.hide:
            return list()

        raw_messages = self.env.storage.get_messages_in_group_for_user(
            group_id, user_stats, query
        )
        messages = list()

        for message_base in raw_messages:
            message = message_base_to_message(message_base)
            messages.append(message)

        return messages

    async def get_attachment_info(self, group_id: str, query: AttachmentQuery, db: Session) -> Message:
        group = self.env.db.get_group_from_id(group_id, db)

        message_base = self.env.storage.get_attachment_from_file_id(
            group_id,
            group.created_at,
            query
        )

        return message_base_to_message(message_base)

    async def get_message_info(self, user_id: int, message_id: str, query: MessageInfoQuery) -> Message:
        message_base = self.env.storage.get_message_with_id(
            group_id=query.group_id,
            user_id=user_id,
            message_id=message_id,
            created_at=query.created_at
        )

        return message_base_to_message(message_base)

    async def create_attachment(
        self, user_id: int, message_id: str, query: CreateAttachmentQuery, db: Session
    ) -> Message:
        if query.group_id is None and query.receiver_id is None:
            raise QueryValidationError("both group_id and receiver_id is empty")
        elif query.group_id is not None and query.receiver_id is not None:
            raise QueryValidationError("can't use both group_id AND receiver_id, choose one")

        group_id = query.group_id

        if group_id is None:
            group_id = self._get_or_create_group_for_1v1(
                user_id, query.receiver_id, db
            )

        attachment = self.env.storage.store_attachment(
            group_id, user_id, message_id, query
        )

        update_last_message = True
        update_last_message_time = True

        if query.action_log is not None:
            update_last_message = query.action_log.update_last_message
            update_last_message_time = query.action_log.update_last_message_time

        self._user_sends_a_message(
            group_id,
            user_id=user_id,
            message=attachment,
            db=db,
            # when the attachment is first created (not yet processed), the unread
            # count is NOT increased, so increase it now when processing has finished
            should_increase_unread=True,
            event_type=EventTypes.ATTACHMENT,
            update_last_message=update_last_message,
            update_last_message_time=update_last_message_time
        )

        return message_base_to_message(attachment)

    def delete_message(
        self, group_id: str, user_id: int, message_id: str, db: Session
    ) -> None:
        group = self.env.db.get_group_from_id(group_id, db)

        self.env.storage.delete_message(
            group_id, user_id, message_id, group.created_at
        )

        # TODO: how to tell apps a message was deleted? <-- update: create action log on deletions
        #  now the /notification/send api should be used for this?
        #  self.env.publisher.delete_message(group_id, message_id)

    def delete_attachment(self, group_id: str, query: AttachmentQuery, db: Session) -> None:
        group = self.env.db.get_group_from_id(group_id, db)

        attachment = self.env.storage.delete_attachment(
            group_id, group.created_at, query
        )

        now = utcnow_ts()
        user_ids = self.env.db.get_user_ids_and_join_time_in_group(group_id, db).keys()

        self.env.cache.remove_attachment_count_in_group_for_users(group_id, user_ids)
        self.create_action_log(query.action_log, db, group_id=group_id)
        self.env.server_publisher.delete_attachments(group_id, [attachment], user_ids, now)

    async def edit(self, user_id: int, message_id: str, query: EditMessageQuery, db: Session) -> Message:
        if query.group_id is None and query.receiver_id is None:
            raise QueryValidationError("both group_id and receiver_id is empty")
        elif query.group_id is not None and query.receiver_id is not None:
            raise QueryValidationError("can't use both group_id AND receiver_id, choose one")

        group_id = query.group_id
        if group_id is None or not len(group_id.strip()):
            group_id = users_to_group_id(user_id, query.receiver_id)

        self.env.storage.edit_message(group_id, user_id, message_id, query)
        action_log = self.create_action_log(query.action_log, db, group_id=group_id)

        """
        if query.action_log is not None:
            # we don't want to increase the unread count, but we want to notify users of the change
            update_unread_count = query.action_log.update_unread_count
            update_last_message = query.action_log.update_last_message

            self._user_sends_a_message(
                group_id,
                user_id=user_id,
                message=action_log,  # we want to update last_message_overview to be the payload of the action log
                db=db,
                should_increase_unread=update_unread_count,
                event_type=EventTypes.EDIT,
                update_last_message=update_last_message
            )
        """

        return action_log
