from typing import Dict, Union
from typing import List

from dinofw.db.rdbms.schemas import GroupBase
from dinofw.db.rdbms.schemas import UserGroupBase
from dinofw.db.rdbms.schemas import UserGroupStatsBase
from dinofw.db.storage.schemas import MessageBase
from dinofw.rest.models import Group
from dinofw.rest.models import GroupJoinTime
from dinofw.rest.models import GroupLastRead
from dinofw.rest.models import Message
from dinofw.rest.models import UserGroup
from dinofw.rest.models import UserGroupStats
from dinofw.utils import to_ts
from dinofw.utils.config import EventTypes


def to_int(time_float):
    """
    frontend js sometimes don't handle milliseconds, so multiply by 1k in these cases
    """
    if not time_float:
        return 0
    return int(time_float * 1000)


def message_base_to_message(message: MessageBase) -> Message:
    message_dict = message.dict()

    message_dict["updated_at"] = to_ts(message_dict["updated_at"], allow_none=True)
    message_dict["created_at"] = to_ts(message_dict["created_at"], allow_none=True)

    return Message(**message_dict)


def to_last_read(user_id: int, last_read: float) -> GroupLastRead:
    return GroupLastRead(user_id=user_id, last_read=last_read)


def to_user_group_stats(user_stats: UserGroupStatsBase) -> UserGroupStats:
    delete_before = to_ts(user_stats.delete_before)
    last_updated_time = to_ts(user_stats.last_updated_time)
    last_sent = to_ts(user_stats.last_sent, allow_none=True)
    last_read = to_ts(user_stats.last_read, allow_none=True)
    first_sent = to_ts(user_stats.first_sent, allow_none=True)
    join_time = to_ts(user_stats.join_time, allow_none=True)
    highlight_time = to_ts(user_stats.highlight_time, allow_none=True)

    # try using the counter column on the stats table instead of actually counting
    """
    unread_amount = self.env.storage.count_messages_in_group_since(
        group_id, user_stats.last_read
    )
    """

    return UserGroupStats(
        user_id=user_stats.user_id,
        group_id=user_stats.group_id,
        unread=user_stats.unread_count,
        join_time=join_time,
        receiver_unread=-1,  # TODO: should be count for other user here as well?
        last_read_time=last_read,
        last_sent_time=last_sent,
        delete_before=delete_before,
        first_sent=first_sent,
        rating=user_stats.rating,
        highlight_time=highlight_time,
        hide=user_stats.hide,
        pin=user_stats.pin,
        deleted=user_stats.deleted,
        bookmark=user_stats.bookmark,
        last_updated_time=last_updated_time,
    )


def group_base_to_user_group(
    group_base: GroupBase,
    stats_base: UserGroupStatsBase,
    receiver_stats_base: UserGroupStatsBase,
    users: Dict[int, float],
    user_count: int,
    receiver_unread: int,
    unread: int,
) -> UserGroup:
    group = group_base_to_group(group_base, users, user_count)

    stats_dict = stats_base.__dict__
    stats_dict["unread"] = unread
    stats_dict["receiver_unread"] = receiver_unread
    stats_dict["receiver_highlight_time"] = to_ts(stats_base.receiver_highlight_time)

    if receiver_stats_base is not None:
        stats_dict["receiver_delete_before"] = to_ts(receiver_stats_base.delete_before)
        stats_dict["receiver_hide"] = receiver_stats_base.hide
        stats_dict["receiver_deleted"] = receiver_stats_base.deleted

    stats_dict["last_read_time"] = to_ts(stats_base.last_read)
    stats_dict["last_sent_time"] = to_ts(stats_base.last_sent)
    stats_dict["join_time"] = to_ts(stats_base.join_time)
    stats_dict["delete_before"] = to_ts(stats_base.delete_before)
    stats_dict["highlight_time"] = to_ts(stats_base.highlight_time, allow_none=True)
    stats_dict["first_sent"] = to_ts(stats_base.first_sent, allow_none=True)
    stats_dict["last_updated_time"] = to_ts(stats_base.last_updated_time)

    stats = UserGroupStats(**stats_dict)

    return UserGroup(group=group, stats=stats,)


def group_base_to_group(
    group: GroupBase,
    users: Dict[int, float],
    user_count: int,
    message_amount: int = -1
) -> Group:
    group_dict = group.dict()

    users = [
        GroupJoinTime(user_id=user_id, join_time=join_time,)
        for user_id, join_time in users.items()
    ]
    users.sort(key=lambda user: user.join_time, reverse=True)

    group_dict["updated_at"] = to_ts(group_dict["updated_at"], allow_none=True)
    group_dict["created_at"] = to_ts(group_dict["created_at"])
    group_dict["last_message_time"] = to_ts(group_dict["last_message_time"])
    group_dict["first_message_time"] = to_ts(group_dict["first_message_time"])
    group_dict["users"] = users
    group_dict["user_count"] = user_count
    group_dict["message_amount"] = message_amount

    return Group(**group_dict)


def to_user_group(user_groups: List[UserGroupBase]):
    groups: List[UserGroup] = list()

    for user_group in user_groups:
        groups.append(
            group_base_to_user_group(
                group_base=user_group.group,
                stats_base=user_group.user_stats,
                receiver_stats_base=user_group.receiver_user_stats,
                unread=user_group.unread,
                receiver_unread=user_group.receiver_unread,
                user_count=user_group.user_count,
                users=user_group.user_join_times,
            )
        )

    return groups


def stats_to_event_dict(user_stats):
    stats_dict = user_stats.dict()

    stats_dict["last_read"] = int(to_ts(stats_dict["last_read"]) * 1000)
    stats_dict["last_sent"] = int(to_ts(stats_dict["last_sent"]) * 1000)
    stats_dict["delete_before"] = int(to_ts(stats_dict["delete_before"]) * 1000)
    stats_dict["join_time"] = int(to_ts(stats_dict["join_time"]) * 1000)

    if stats_dict["highlight_time"]:
        stats_dict["highlight_time"] = int(to_ts(stats_dict["highlight_time"]) * 1000)

    if stats_dict["last_updated_time"]:
        stats_dict["last_updated_time"] = int(to_ts(stats_dict["last_updated_time"]) * 1000)

    if stats_dict["first_sent"]:
        stats_dict["first_sent"] = int(to_ts(stats_dict["first_sent"]) * 1000)

    if stats_dict["receiver_highlight_time"]:
        stats_dict["receiver_highlight_time"] = \
            int(to_ts(stats_dict["receiver_highlight_time"]) * 1000)

    # not needed in the mqtt event
    del stats_dict["user_id"]
    del stats_dict["group_id"]

    return stats_dict


def group_base_to_event(group: GroupBase, user_ids: List[int] = None) -> dict:
    group_dict = {
        "event_type": EventTypes.GROUP,
        "group_id": group.group_id,
        "name": group.name,
        "description": group.description,
        "updated_at": to_int(to_ts(group.updated_at, allow_none=True)),
        "created_at": to_int(to_ts(group.created_at)),
        "last_message_time": to_int(to_ts(group.last_message_time, allow_none=True)),
        "last_message_overview": group.last_message_overview,
        "last_message_type": group.last_message_type,
        "last_message_user_id": str(group.last_message_user_id),
        "status": group.status,
        "group_type": group.group_type,
        "owner_id": str(group.owner_id),
        "meta": group.meta
    }

    if user_ids is not None:
        group_dict["user_ids"] = [str(uid) for uid in user_ids]

    return group_dict


def read_to_event(group_id: str, user_id: int, now: float):
    return {
        "event_type": EventTypes.READ,
        "group_id": group_id,
        "user_id": str(user_id),
        "read_at": to_int(now),
    }


def message_base_to_event(
        message: Union[MessageBase, Message],
        event_type: EventTypes = EventTypes.MESSAGE,
        group: GroupBase = None
):
    event = {
        "event_type": event_type,
        "group_id": message.group_id,
        "sender_id": str(message.user_id),
        "message_id": message.message_id,
        "message_payload": message.message_payload,
        "message_type": message.message_type,
        "updated_at": to_int(to_ts(message.updated_at, allow_none=True)),
        "created_at": to_int(to_ts(message.created_at)),
    }

    # if the 'message' variable is Message instead of MessageBase, there's no file_id available; (e.g. for /edit)
    if hasattr(message, "file_id"):
        event["file_id"] = message.file_id

    if group is not None:
        group_dict = group_base_to_event(group)
        del group_dict["event_type"]

        event["group"] = group_dict

    return event
