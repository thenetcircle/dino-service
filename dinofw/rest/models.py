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


class PaginationQuery(AbstractQuery):
    until: Optional[float]
    per_page: int


class AdminQuery(AbstractQuery):
    admin_id: Optional[int]


class OneToOneQuery(AbstractQuery):
    receiver_id: Optional[int]


class MessageQuery(PaginationQuery, AdminQuery):
    pass


class CreateActionLogQuery(AbstractQuery):
    payload: Optional[str]
    group_id: Optional[str]
    receiver_id: Optional[int]


class SearchQuery(PaginationQuery):
    keyword: Optional[str]
    group_type: Optional[int]
    status: Optional[int]


class SendMessageQuery(OneToOneQuery):
    message_payload: Optional[str]
    message_type: int


class CreateGroupQuery(AbstractQuery):
    users: List[int]

    group_name: str
    group_type: int
    description: Optional[str]
    meta: Optional[int]
    context: Optional[str]


class UserStatsQuery(AbstractQuery):
    hidden: Optional[bool]
    count_unread: Optional[bool] = True
    only_unread: Optional[bool] = True


class GroupInfoQuery(AbstractQuery):
    count_messages: Optional[bool] = False


class GroupQuery(PaginationQuery, UserStatsQuery):
    pass


class GroupUpdatesQuery(GroupQuery):
    since: Optional[float]


class JoinGroupQuery(AbstractQuery):
    users: List[int]


class UpdateGroupQuery(AbstractQuery):
    status: Optional[int]
    owner: Optional[int]
    group_name: Optional[str]
    description: Optional[str]


class EditMessageQuery(AdminQuery):
    message_payload: Optional[str]
    message_type: Optional[int]
    status: Optional[int]
    created_at: float


class AttachmentQuery(AbstractQuery):
    file_id: str


class CreateAttachmentQuery(AttachmentQuery, OneToOneQuery):
    message_payload: str
    created_at: float
    group_id: Optional[str]


class UpdateUserGroupStats(AbstractQuery):
    last_read_time: Optional[float]
    delete_before: Optional[float]
    highlight_time: Optional[float]
    hide: Optional[bool]
    bookmark: Optional[bool]
    pin: Optional[bool]
    rating: Optional[int]


class GroupJoinTime(BaseModel):
    user_id: int
    join_time: float


class GroupLastRead(BaseModel):
    user_id: int
    last_read: float


class GroupUsers(BaseModel):
    group_id: str
    owner_id: int
    user_count: int
    users: List[GroupJoinTime]


class UserStats(BaseModel):
    user_id: int
    last_update_time: Optional[float]
    last_sent_time: Optional[float]
    last_sent_group_id: Optional[str]

    # total number of unread messages in all 1v1/groups
    unread_amount: int

    # number of 1v1/groups the user has joined
    group_amount: int
    one_to_one_amount: int

    # number of 1v1/groups with at least one unread message
    unread_groups_amount: int


class UserGroupStats(BaseModel):
    group_id: str
    user_id: int
    unread: int
    receiver_unread: int
    delete_before: float
    join_time: float
    last_read_time: Optional[float]
    last_sent_time: Optional[float]
    highlight_time: Optional[float]
    last_updated_time: float
    first_sent: Optional[float]

    hide: Optional[bool]
    pin: Optional[bool]
    bookmark: Optional[bool]
    rating: Optional[int]

    receiver_highlight_time: Optional[float]
    receiver_delete_before: Optional[float]
    receiver_hide: Optional[bool]


class Message(BaseModel):
    group_id: str
    created_at: float
    user_id: int
    message_id: str
    message_payload: Optional[str]

    message_type: int
    file_id: Optional[str]
    updated_at: Optional[float]
    removed_at: Optional[float]
    removed_by_user: Optional[int]


class Group(BaseModel):
    group_id: str
    users: List[GroupJoinTime]
    user_count: int
    name: str
    description: Optional[str]
    status: Optional[int]
    group_type: int
    created_at: float
    updated_at: Optional[float]
    owner_id: int
    meta: Optional[int]
    first_message_time: float  # TODO: this is probably not needed for the rest api, just internal to track deletions
    last_message_time: float
    last_message_overview: Optional[str]
    last_message_user_id: Optional[int]
    last_message_type: Optional[int]
    last_message_id: Optional[str]
    user_count: int
    message_amount: Optional[int] = -1


class OneToOneStats(BaseModel):
    group: Group
    stats: List[UserGroupStats]


class UserGroup(BaseModel):
    group: Group
    stats: UserGroupStats


class Histories(BaseModel):
    messages: List[Message]
    last_reads: List[GroupLastRead]
