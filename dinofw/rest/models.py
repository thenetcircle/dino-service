from datetime import datetime as dt
from typing import Optional, List

import pytz
from pydantic import BaseModel


class AbstractQuery(BaseModel):
    @staticmethod
    def to_dt(s, allow_none: bool = False, default: dt = None):
        if s is None and default is not None:
            return default

        if s is None and allow_none:
            return None

        if s is None:
            s = dt.utcnow()
            s = s.replace(tzinfo=pytz.UTC)
        else:
            s = int(s)
            s = dt.utcfromtimestamp(s)

        return s

    @staticmethod
    def to_ts(ds, allow_none: bool = False):
        if ds is None and allow_none:
            return None

        if ds is None:
            ds = dt.utcnow()
            ds = ds.replace(tzinfo=pytz.UTC)

        return ds.strftime("%s")


class PaginationQuery(AbstractQuery):
    until: Optional[int]
    hide_before: Optional[int]
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
    group_type: str
    description: Optional[str]
    group_meta: Optional[int]  # TODO: int or str?
    group_context: Optional[str]


class GroupJoinQuery(AbstractQuery):
    joiner_id: int
    inviter_id: int
    invitation_context: str


class GroupJoinerQuery(PaginationQuery):
    status: int


class GroupQuery(PaginationQuery):
    ownership: Optional[int]
    weight: Optional[int]
    has_unread: Optional[int]


class JoinerUpdateQuery(AbstractQuery):
    status: int


class AdminUpdateGroupQuery(AdminQuery):
    group_status: int


class UpdateGroupQuery(AbstractQuery):
    # TODO: update owner?
    group_name: Optional[str]
    group_weight: Optional[int]
    group_context: Optional[str]


class EditMessageQuery(MessageQuery):
    message_payload: Optional[str]


class Message(AbstractQuery):
    group_id: str
    created_at: int
    user_id: int
    message_id: str
    message_payload: str

    status: Optional[int]
    message_type: Optional[int]
    updated_at: Optional[int]
    removed_at: Optional[int]
    removed_by_user: Optional[int]
    last_action_log_id: Optional[str]


class GroupUsers(AbstractQuery):
    # TODO: should sort user ids by join datetime
    group_id: str
    owner_id: int
    users: List[int]


class UserStats(AbstractQuery):
    user_id: int
    message_amount: int
    unread_amount: int
    group_amount: int
    owned_group_amount: int
    last_read_time: int
    last_read_group_id: int
    last_send_time: int
    last_send_group_id: int
    last_group_join_time: int
    last_group_join_sent_time: int


class UserGroupStats(AbstractQuery):
    message_amount: int
    unread_amount: int
    last_read_time: int
    last_send_time: int
    hide_before: int


class UpdateUserGroupStats(AbstractQuery):
    last_read_time: Optional[int]
    hide_before: Optional[int]


class ActionLog(AbstractQuery):
    action_id: str
    user_id: int
    group_id: str
    action_type: int
    created_at: int
    admin_id: Optional[int]
    message_id: Optional[str]


class Group(AbstractQuery):
    group_id: str
    users: List[int]
    last_read: int
    name: str
    description: Optional[str]
    status: Optional[int]
    group_type: int
    created_at: int
    updated_at: Optional[int]
    owner_id: int
    group_meta: Optional[int]
    group_context: Optional[str]
    last_message_overview: Optional[str]
    last_message_user_id: Optional[int]
    last_message_time: int


class Joiner(AbstractQuery):
    joiner_id: int
    group_id: str
    inviter_id: int
    created_at: int
    status: int
    invitation_context: str


class Histories(AbstractQuery):
    messages: Optional[List[Message]]
    action_logs: Optional[List[ActionLog]]
