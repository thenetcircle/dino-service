from abc import ABC
from typing import List
from typing import Union, Optional

import arrow
from loguru import logger
from sqlalchemy.orm import Session

from dinofw.db.rdbms.schemas import GroupBase
from dinofw.db.rdbms.schemas import UserGroupStatsBase
from dinofw.db.storage.schemas import MessageBase
from dinofw.rest.models import Message
from dinofw.rest.queries import ActionLogQuery
from dinofw.rest.queries import SendMessageQuery
from dinofw.utils import need_to_update_stats_in_group
from dinofw.utils import users_to_group_id
from dinofw.utils import utcnow_dt
from dinofw.utils import utcnow_ts
from dinofw.utils.config import EventTypes
from dinofw.utils.convert import message_base_to_message
from dinofw.utils.exceptions import NoSuchGroupException, UserStatsOrGroupAlreadyCreated


class BaseResource(ABC):
    def __init__(self, env):
        self.env = env

        # used when no `hide_before` is specified in a query
        beginning_of_1995 = 789_000_000
        self.long_ago = arrow.Arrow.utcfromtimestamp(beginning_of_1995).datetime

    async def _user_opens_conversation(self, group_id: str, user_id: int, user_stats: UserGroupStatsBase, db):
        """
        update database and cache with everything related to opening a conversation (if needed)
        """
        # TODO: maybe use unread count here instead? since migration doesn't have MS on last_message_time
        last_message_time = await self.env.db.get_last_message_time_in_group(group_id, db)

        # if a user opens a conversation a second time and nothing has changed, we don't need to update
        if not need_to_update_stats_in_group(user_stats, last_message_time):
            return

        now_ts = utcnow_ts()
        now_dt = utcnow_dt(now_ts)

        # something changed, so update and set last_updated_time to sync to apps
        await self.env.db.update_last_read_and_highlight_in_group_for_user(
            group_id, user_id, now_dt, db
        )

        if user_stats.unread_count > 0 or user_stats.bookmark:
            await self.env.cache.set_unread_in_group(group_id, user_id, 0)
            await self.env.cache.remove_unread_group(user_id, group_id)
            await self.env.cache.reset_total_unread_message_count(user_id)

        # no point updating if already newer than last message (also skips
        # broadcasting unnecessary read-receipts)
        # TODO: double check this; won't it cause read-receipts to not be sent when reading a new message?
        if last_message_time > user_stats.last_read:
            user_ids = await self.env.db.get_user_ids_and_join_time_in_group(group_id, db)

            del user_ids[user_id]
            self.env.client_publisher.read(
                group_id, user_id, user_ids, now_dt, bookmark=user_stats.bookmark
            )

    async def create_action_log(
        self,
            query: ActionLogQuery,
            db: Session,
            user_id: int = None,
            group_id: str = None,
    ) -> Message:
        # creating an action log is optional for the caller
        if query is None:
            return None  # noqa

        if query.user_id is not None:
            user_id = query.user_id

        if user_id is None and group_id is None:
            raise ValueError("either receiver_id or group_id is required in CreateActionLogQuery")

        if user_id is None:
            raise ValueError("no user_id in api path(?) and no user_id on ActionLogQuery; need one of them")

        if query.group_id is not None and len(query.group_id.strip()):
            group_id = query.group_id
        elif query.receiver_id is not None and query.receiver_id > 0:
            group_id = await self._get_or_create_group_for_1v1(user_id, query.receiver_id, db)

        log = await self.env.storage.create_action_log(user_id, group_id, query)
        await self._user_sends_a_message(
            group_id,
            user_id=user_id,
            message=log,
            db=db,
            should_increase_unread=query.update_unread_count,
            update_last_message=query.update_last_message,
            update_last_message_time=query.update_last_message_time,
            unhide_group=query.unhide_group,
            update_group_updated_at=query.update_group_updated_at,
            event_type=EventTypes.ACTION_LOG,
            context=None
        )

        return message_base_to_message(log)

    async def _user_sends_a_message(
            self,
            group_id: str,
            user_id: int,
            message: MessageBase,
            db,
            should_increase_unread: bool,
            event_type: str,
            update_last_message: bool = True,
            update_last_message_time: bool = True,
            unhide_group: bool = True,
            update_group_updated_at: bool = True,
            mentions: List[int] = None,
            context: Optional[str] = None,
    ) -> Optional[GroupBase]:
        """
        update database and cache with everything related to sending a message
        """
        user_ids = await self.env.db.get_user_ids_and_join_time_in_group(group_id, db)

        group_base = await self.env.db.update_group_new_message(
            message,
            db,
            sender_user_id=user_id,
            update_unread_count=should_increase_unread,
            update_last_message=update_last_message,
            update_last_message_time=update_last_message_time,
            update_group_updated_at=update_group_updated_at,
            unhide_group=unhide_group,
            mentions=mentions,
            context=context
        )

        # if all users left the group, this message is an action log, and there's nothing more to do
        if not len(user_ids):
            return None

        if user_id not in user_ids and unhide_group:
            # if the user deleted the group, this is an action log for the
            # deletion, and we only have to un-hide it for the other user(s)
            await self.env.cache.set_hide_group(group_id, False)
        else:
            # otherwise we update as normal
            await self.env.db.update_last_read_and_sent_in_group_for_user(
                group_id, user_id, message.created_at, db, unhide_group=unhide_group
            )

        if event_type == EventTypes.ATTACHMENT:
            await self.env.cache.increase_attachment_count_in_group_for_users(group_id, list(user_ids.keys()))
        elif event_type == EventTypes.DELETE_ATTACHMENT:
            # instead of decreasing, just remove it, since in case no count is cached, decreasing
            # a non-existing key will store -1, which is incorrect
            await self.env.cache.remove_attachment_count_in_group_for_users(group_id, list(user_ids.keys()))
            # TODO: decrease total unread count in redis? or remove it?

        return group_base

    async def _get_or_create_group_for_1v1(
        self, user_id: int, receiver_id: int, db: Session
    ) -> str:
        if user_id is None or receiver_id is None:
            raise ValueError(
                f"either receiver_id ({receiver_id}) or user_id ({user_id}) is None for get/create 1v1 group"
            )

        group_id = users_to_group_id(user_id, receiver_id)
        if await self.env.cache.get_group_exists(group_id):
            return group_id

        try:
            group = await self.env.db.get_group_for_1to1(user_id, receiver_id, db)
        except NoSuchGroupException:
            try:
                group = await self.env.db.create_group_for_1to1(user_id, receiver_id, db)
            except UserStatsOrGroupAlreadyCreated:
                # another api call created it after we checked but before we finished creating it,
                # so we should now be able to just fetch the one created by the other call
                group = await self.env.db.get_group_for_1to1(user_id, receiver_id, db)

        group_id = group.group_id
        await self.env.cache.set_group_exists(group_id, True)
        return group_id
