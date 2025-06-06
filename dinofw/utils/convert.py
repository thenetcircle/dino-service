from datetime import datetime as dt
from typing import Dict, Tuple, Optional
from typing import List
from typing import Union

from dinofw.db.rdbms.schemas import GroupBase, DeletedStatsBase
from dinofw.db.rdbms.schemas import UserGroupBase
from dinofw.db.rdbms.schemas import UserGroupStatsBase
from dinofw.db.storage.schemas import MessageBase
from dinofw.rest.models import Group, LastReads, LastRead, DeletedStats, UnDeletedGroup
from dinofw.rest.models import GroupJoinTime
from dinofw.rest.models import Message
from dinofw.rest.models import UserGroup
from dinofw.rest.models import UserGroupStats
from dinofw.utils import to_ts
from dinofw.utils.config import EventTypes, GroupStatus


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


def to_user_group_stats(user_stats: UserGroupStatsBase) -> UserGroupStats:
    delete_before = to_ts(user_stats.delete_before)
    last_updated_time = to_ts(user_stats.last_updated_time)
    last_sent = to_ts(user_stats.last_sent, allow_none=True)
    last_read = to_ts(user_stats.last_read, allow_none=True)
    first_sent = to_ts(user_stats.first_sent, allow_none=True)
    join_time = to_ts(user_stats.join_time, allow_none=True)
    highlight_time = to_ts(user_stats.highlight_time, allow_none=True)

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
        mentions=user_stats.mentions,
        notifications=user_stats.notifications,
        deleted=user_stats.deleted,
        bookmark=user_stats.bookmark,
        last_updated_time=last_updated_time,
        kicked=user_stats.kicked
    )


def deleted_group_base_to_user_group(deleted_group: DeletedStatsBase) -> UserGroup:
    join_time = to_ts(deleted_group.join_time, allow_none=False)
    delete_time = to_ts(deleted_group.delete_time, allow_none=False)

    group = Group(
        group_id=deleted_group.group_id,
        users=list(),
        user_count=0,
        name="deleted group",
        status=GroupStatus.DELETED,
        status_changed_at=delete_time,
        group_type=deleted_group.group_type,
        created_at=join_time,
        updated_at=delete_time,
        first_message_time=join_time,
        last_message_time=delete_time
    )

    stats = UserGroupStats(
        user_id=deleted_group.user_id,
        group_id=deleted_group.group_id,
        unread=-1,  # not applicable for deleted groups
        receiver_unread=-1,  # not applicable for deleted groups
        delete_before=delete_time,
        join_time=join_time,
        last_updated_time=delete_time,
        notifications=False,
        mentions=-1,
        kicked=False
    )

    return UserGroup(group=group, stats=stats)

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
        stats_dict["receiver_last_read_time"] = to_ts(receiver_stats_base.last_read)

    stats_dict["last_read_time"] = to_ts(stats_base.last_read)
    stats_dict["last_sent_time"] = to_ts(stats_base.last_sent)
    stats_dict["join_time"] = to_ts(stats_base.join_time)
    stats_dict["delete_before"] = to_ts(stats_base.delete_before)
    stats_dict["highlight_time"] = to_ts(stats_base.highlight_time, allow_none=True)
    stats_dict["first_sent"] = to_ts(stats_base.first_sent, allow_none=True)
    stats_dict["last_updated_time"] = to_ts(stats_base.last_updated_time)

    stats = UserGroupStats(**stats_dict)

    return UserGroup(group=group, stats=stats)


def group_base_to_group(
    group: GroupBase,
    users: Dict[int, float],
    user_count: int,
    message_amount: int = -1,
    attachment_amount: int = -1
) -> Group:
    group_dict = group.dict()

    users = [
        GroupJoinTime(user_id=user_id, join_time=join_time,)
        for user_id, join_time in users.items()
    ]
    users.sort(key=lambda user: user.join_time, reverse=True)

    group_dict["status_changed_at"] = to_ts(group_dict["status_changed_at"], allow_none=True)
    group_dict["updated_at"] = to_ts(group_dict["updated_at"], allow_none=True)
    group_dict["created_at"] = to_ts(group_dict["created_at"])
    group_dict["last_message_time"] = to_ts(group_dict["last_message_time"])
    group_dict["first_message_time"] = to_ts(group_dict["first_message_time"])
    group_dict["users"] = users
    group_dict["user_count"] = user_count
    group_dict["message_amount"] = message_amount
    group_dict["attachment_amount"] = attachment_amount

    return Group(**group_dict)


def to_last_reads(group_id: str, last_reads: Dict[int, float]) -> LastReads:
    return LastReads(
        group_id=group_id,
        last_read_times=[
            LastRead(user_id=user_id, last_read_time=last_read_time)
            for user_id, last_read_time in last_reads.items()
        ]
    )


def to_undeleted_stats(undeleted_groups: List[Tuple[str, int, dt]]) -> List[UnDeletedGroup]:
    groups: List[UnDeletedGroup] = list()

    for group_id, group_type, join_time in undeleted_groups:
        join_time = to_ts(join_time, allow_none=True)

        group = UnDeletedGroup(
            group_id=group_id,
            group_type=group_type,
            join_time=join_time,
        )
        groups.append(group)

    return groups


def to_deleted_stats(deleted_stats: List[DeletedStatsBase]) -> List[DeletedStats]:
    stats: List[DeletedStats] = list()

    for deleted_stat in deleted_stats:
        join_time = to_ts(deleted_stat.join_time, allow_none=True)
        delete_time = to_ts(deleted_stat.delete_time, allow_none=True)

        stats.append(DeletedStats(
            group_id=deleted_stat.group_id,
            user_id=deleted_stat.user_id,
            group_type=deleted_stat.group_type,
            join_time=join_time,
            delete_time=delete_time
        ))

    return stats


def to_user_group(user_groups: List[UserGroupBase], deleted_groups: Optional[List[DeletedStatsBase]] = None):
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

    if deleted_groups is not None:
        for deleted_group in deleted_groups:
            groups.append(deleted_group_base_to_user_group(deleted_group))

    return groups


def stats_to_event_dict(user_stats: UserGroupStatsBase):
    stats_dict = user_stats.dict()

    stats_dict["last_read"] = to_int(to_ts(stats_dict["last_read"]))
    stats_dict["last_sent"] = to_int(to_ts(stats_dict["last_sent"]))
    stats_dict["delete_before"] = to_int(to_ts(stats_dict["delete_before"]))
    stats_dict["join_time"] = to_int(to_ts(stats_dict["join_time"]))

    if stats_dict["highlight_time"]:
        stats_dict["highlight_time"] = to_int(to_ts(stats_dict["highlight_time"]))

    if stats_dict["last_updated_time"]:
        stats_dict["last_updated_time"] = to_int(to_ts(stats_dict["last_updated_time"]))

    if stats_dict["first_sent"]:
        stats_dict["first_sent"] = to_int(to_ts(stats_dict["first_sent"]))

    if stats_dict["receiver_highlight_time"]:
        stats_dict["receiver_highlight_time"] = \
            to_int(to_ts(stats_dict["receiver_highlight_time"]))

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


def read_to_event(group_id: str, user_id: int, now: dt, bookmark: bool):
    # bookmark: false, means read   => peer_status: 3
    # bookmark: true,  means unread => peer_status: 4
    peer_status = 3
    if bookmark:
        peer_status = 4

    return {
        "event_type": EventTypes.READ,
        "group_id": group_id,
        "user_id": str(user_id),
        "peer_last_read": to_int(to_ts(now)),
        "peer_status": peer_status
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
