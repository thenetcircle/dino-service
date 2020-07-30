from datetime import datetime as dt
from typing import Optional, List

import pytz
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
            s = dt.fromtimestamp(s).replace(tzinfo=pytz.UTC)

        return s

    @staticmethod
    def to_ts(ds, allow_none: bool = False) -> Optional[str]:
        if ds is None and allow_none:
            return None

        if ds is None:
            ds = arrow.utcnow().datetime

        return ds.strftime("%s.%f")


class PaginationQuery(AbstractQuery):
    until: Optional[float]
    per_page: int


class AdminQuery(AbstractQuery):
    admin_id: Optional[int]


class MessageQuery(PaginationQuery, AdminQuery):
    message_type: Optional[int]
    status: Optional[int]


class SearchQuery(PaginationQuery):
    keyword: Optional[str]
    group_type: Optional[int]
    status: Optional[int]


class SendMessageQuery(AbstractQuery):
    message_payload: str
    message_type: str


class CreateGroupQuery(AbstractQuery):
    group_name: str
    group_type: int
    users: List[int]
    description: Optional[str]
    group_meta: Optional[int]
    group_context: Optional[str]


class GroupQuery(PaginationQuery):
    ownership: Optional[int]
    weight: Optional[int]
    has_unread: Optional[int]


class AdminUpdateGroupQuery(AdminQuery):
    group_status: int


class UpdateGroupQuery(AbstractQuery):
    # TODO: update owner?
    group_name: Optional[str]
    group_weight: Optional[int]
    group_context: Optional[str]


class EditMessageQuery(AdminQuery):
    message_payload: Optional[str]
    message_type: Optional[int]
    status: Optional[int]


class Message(AbstractQuery):
    group_id: str
    created_at: float
    user_id: int
    message_id: str
    message_payload: str

    status: Optional[int]
    message_type: Optional[str]
    updated_at: Optional[float]
    removed_at: Optional[float]
    removed_by_user: Optional[int]
    last_action_log_id: Optional[str]


class GroupJoinTime(AbstractQuery):
    user_id: int
    join_time: float


class GroupUsers(AbstractQuery):
    group_id: str
    owner_id: int
    user_count: int
    users: List[GroupJoinTime]


class UserStats(AbstractQuery):
    user_id: int
    unread_amount: int
    group_amount: int
    owned_group_amount: int
    last_update_time: Optional[float]
    last_read_time: Optional[float]
    last_read_group_id: Optional[str]
    last_send_time: Optional[float]
    last_send_group_id: Optional[str]


class UserGroupStats(AbstractQuery):
    group_id: str
    user_id: int
    message_amount: int
    unread_amount: int
    last_read_time: float
    last_send_time: float
    delete_before: float
    highlight_time: Optional[float]

    hide: bool
    pin: bool
    bookmark: bool


class UpdateUserGroupStats(AbstractQuery):
    last_read_time: Optional[float]
    delete_before: Optional[float]
    hide: Optional[bool]
    bookmark: Optional[bool]
    pin: Optional[bool]


class ActionLog(AbstractQuery):
    action_id: str
    user_id: int
    group_id: str
    action_type: int
    created_at: float
    admin_id: Optional[int]
    message_id: Optional[str]


class Group(AbstractQuery):
    group_id: str
    users: List[GroupJoinTime]
    user_count: int
    last_read: float
    name: str
    description: Optional[str]
    status: Optional[int]
    group_type: int
    created_at: float
    updated_at: Optional[float]
    owner_id: int
    group_meta: Optional[int]
    group_context: Optional[str]
    last_message_overview: Optional[str]
    last_message_time: float


class Histories(AbstractQuery):
    messages: List[Message]
    action_logs: List[ActionLog]
