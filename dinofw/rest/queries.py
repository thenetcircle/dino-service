from datetime import datetime as dt
from typing import List
from typing import Optional

import arrow
from pydantic import BaseModel

from dinofw.utils import utcnow_dt
from dinofw.utils import utcnow_ts


class AbstractQuery(BaseModel):
    @staticmethod
    def to_dt(s, allow_none: bool = False, default: dt = None) -> Optional[dt]:
        if s is None and default is not None:
            return default

        if s is None and allow_none:
            return None

        if s is None:
            s = utcnow_dt()
        else:
            # millis not micros
            s = arrow.get(round(float(s), 3)).datetime

        return s

    @staticmethod
    def to_ts(ds, allow_none: bool = False) -> Optional[float]:
        if ds is None and allow_none:
            return None

        if ds is None:
            return utcnow_ts()

        # millis not micros
        return round(arrow.get(ds).float_timestamp, 3)


# TODO: need anything else? or caller can send anything
class BroadcastQuery(BaseModel):
    user_ids: List[int]
    context: dict


class ActionLogQuery(BaseModel):
    payload: str

    # not all action log creations should update last_message_time and unread count
    update_unread_count: Optional[bool] = False

    # in some cases the 'last message preview' on the group should not be updated
    update_last_message: Optional[bool] = True

    # in some cases the api route doesn't include the user id, e.g.
    # join/kick/etc., but should be recorded on the action log
    user_id: Optional[int]

    # POST /actions api specifies group_id/receiver_id on the query for generic action logs
    group_id: Optional[str]
    receiver_id: Optional[int]


class CreateActionLogQuery(AbstractQuery):
    action_log: Optional[ActionLogQuery]


class PaginationQuery(AbstractQuery):
    until: Optional[float]
    since: Optional[float]
    per_page: int


class AdminQuery(AbstractQuery):
    admin_id: Optional[int]


class OneToOneQuery(AbstractQuery):
    receiver_id: Optional[int]
    notification: Optional[dict]
    broadcast: Optional[bool] = False


class MessageQuery(PaginationQuery, AdminQuery):
    pass


class SearchQuery(PaginationQuery):
    # TODO: not used
    keyword: Optional[str]
    group_type: Optional[int]
    status: Optional[int]


class SendMessageQuery(OneToOneQuery):
    message_payload: Optional[str]
    message_type: int
    context: Optional[str]


class CreateGroupQuery(AbstractQuery):
    users: List[int]

    group_name: str
    group_type: int
    description: Optional[str]
    meta: Optional[int]


class UserStatsQuery(AbstractQuery):
    hidden: Optional[bool] = False
    count_unread: Optional[bool] = True
    only_unread: Optional[bool] = True


class GroupInfoQuery(AbstractQuery):
    count_messages: Optional[bool] = False


class ReceiverStatsQuery(AbstractQuery):
    receiver_stats: Optional[bool] = False


class GroupQuery(PaginationQuery, UserStatsQuery, ReceiverStatsQuery):
    receiver_ids: Optional[List[int]]


class GroupUpdatesQuery(GroupQuery):
    pass


class JoinGroupQuery(CreateActionLogQuery):
    users: List[int]


class AttachmentQuery(CreateActionLogQuery):
    file_id: str


class UpdateGroupQuery(CreateActionLogQuery):
    status: Optional[int]
    owner: Optional[int]
    group_name: Optional[str]
    description: Optional[str]


class MessageInfoQuery(AbstractQuery):
    group_id: str
    created_at: float  # needed to avoid large table scans


class CreateAttachmentQuery(AttachmentQuery, OneToOneQuery):
    message_payload: str
    created_at: float
    group_id: Optional[str]


class UpdateUserGroupStats(CreateActionLogQuery):
    last_read_time: Optional[float]
    delete_before: Optional[float]
    highlight_time: Optional[float]
    highlight_limit: Optional[int]
    hide: Optional[bool]
    bookmark: Optional[bool]
    pin: Optional[bool]
    rating: Optional[int]


class EditMessageQuery(OneToOneQuery):
    action_log: Optional[ActionLogQuery]

    created_at: float
    group_id: Optional[str]

    # fields that can be updated
    context: Optional[str]
