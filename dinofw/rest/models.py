from datetime import datetime as dt
from typing import Optional, List

import arrow
from pydantic import BaseModel


class AbstractQuery(BaseModel):
    @staticmethod
    def to_dt(s, allow_none: bool = False, default: dt = None) -> Optional[dt]:
        if s is None and default is not None:
            return default

        if s is None and allow_none:
            return None

        if s is None:
            s = arrow.utcnow().datetime
        else:
            s = arrow.get(float(s)).datetime

        return s

    @staticmethod
    def to_ts(ds, allow_none: bool = False) -> Optional[float]:
        if ds is None and allow_none:
            return None

        if ds is None:
            return arrow.utcnow().float_timestamp

        return arrow.get(ds).float_timestamp


class PaginationQuery(AbstractQuery):
    until: Optional[float]
    per_page: int


class AdminQuery(AbstractQuery):
    admin_id: Optional[int]


class OneToOneQuery(AbstractQuery):
    receiver_id: Optional[int]


class MessageQuery(PaginationQuery, AdminQuery):
    pass


class CreateActionLogQuery(AdminQuery):
    user_ids: List[int]
    action_type: int
    payload: Optional[str]


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
    weight: Optional[int]


class GroupQuery(PaginationQuery):
    count_unread: Optional[bool]
    only_unread: Optional[bool]
    hidden: Optional[bool]


class GroupUpdatesQuery(GroupQuery):
    since: Optional[float]


class UpdateGroupQuery(AbstractQuery):
    status: Optional[int]
    owner: Optional[int]
    name: Optional[str]
    weight: Optional[int]
    context: Optional[str]


class UpdateUserMessageQuery(AbstractQuery):
    status: int


class EditMessageQuery(AdminQuery):
    message_payload: Optional[str]
    message_type: Optional[int]
    status: Optional[int]
    created_at: float


class CreateAttachmentQuery(OneToOneQuery):
    file_id: str
    status: int
    message_payload: str
    created_at: float


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
    unread_amount: int
    group_amount: int
    one_to_one_amount: int
    owned_group_amount: int
    last_update_time: Optional[float]
    last_read_time: Optional[float]
    last_read_group_id: Optional[str]
    last_send_time: Optional[float]
    last_send_group_id: Optional[str]


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


class Message(BaseModel):
    group_id: str
    created_at: float
    user_id: int
    message_id: str
    message_payload: Optional[str]

    status: Optional[int]
    message_type: int
    updated_at: Optional[float]
    removed_at: Optional[float]
    removed_by_user: Optional[int]
    last_action_log_id: Optional[str]


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
    context: Optional[str]
    last_message_overview: Optional[str]
    last_message_user_id: Optional[int]
    last_message_time: float
    last_message_type: Optional[int]
    user_count: int


class OneToOneStats(BaseModel):
    group: Group
    stats: List[UserGroupStats]


class UserGroup(BaseModel):
    group: Group
    stats: UserGroupStats


class Histories(BaseModel):
    messages: List[Message]
    last_reads: List[GroupLastRead]
