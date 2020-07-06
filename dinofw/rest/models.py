from datetime import datetime as dt
from typing import Optional, List

import pytz
from pydantic import BaseModel


class PaginationQuery(BaseModel):
    since: Optional[int]
    per_page: int

    @staticmethod
    def to_dt(s):
        if s is None:
            s = dt.utcnow()
            s = s.replace(tzinfo=pytz.UTC)
        else:
            s = dt.strptime(str(s), "%s")

        return s


class AdminQuery(BaseModel):
    admin_id: Optional[int]


class MessageQuery(PaginationQuery, AdminQuery):
    message_type: Optional[int]
    status: Optional[int]


class HistoryQuery(MessageQuery):
    time_from: Optional[int]
    time_to: Optional[int]


class SearchQuery(PaginationQuery):
    keyword: Optional[str]
    group_type: Optional[int]
    status: Optional[int]


class SendMessageQuery(BaseModel):
    message_payload: str
    message_type: str


class CreateGroupQuery(BaseModel):
    group_name: str
    group_meta: int  # TODO: int or str?
    group_type: str
    group_context: str


class GroupJoinQuery(BaseModel):
    joiner_id: int
    inviter_id: int
    invitation_context: str


class GroupJoinerQuery(PaginationQuery):
    status: int


class GroupQuery(PaginationQuery):
    ownership: Optional[int]
    weight: Optional[int]
    has_unread: Optional[int]


class JoinerUpdateQuery(BaseModel):
    status: int


class AdminUpdateGroupQuery(AdminQuery):
    group_status: int


class UpdateGroupQuery(BaseModel):
    # TODO: update owner?
    group_name: str
    group_weight: int
    group_context: str


class EditMessageQuery(MessageQuery):
    read_at: int


class Message(BaseModel):
    message_id: str
    group_id: str
    user_id: int
    created_at: int
    status: int
    message_type: int
    read_at: int
    updated_at: int
    last_action_log_id: int
    removed_at: int
    removed_by_user: int
    message_payload: str


class GroupUsers(BaseModel):
    # TODO: should sort user ids by join datetime
    group_id: str
    owner_id: int
    users: List[int]


class UserStats(BaseModel):
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


class UserGroupStats(BaseModel):
    user_id: int
    group_id: str
    message_amount: int
    unread_amount: int
    last_read_time: int
    last_send_time: int
    hide_before: int


class ActionLog(BaseModel):
    action_id: str
    user_id: int
    group_id: str
    message_id: str
    action_type: int
    created_at: int
    admin_id: int


class Group(BaseModel):
    group_id: str
    users: List[int]
    last_read: int
    name: str
    description: str
    status: int
    group_type: int
    created_at: int
    updated_at: int
    owner_id: int
    group_meta: int
    group_context: str
    last_message_overview: str
    last_message_user_id: int
    last_message_time: int


class Joiner(BaseModel):
    joined_id: int
    group_id: str
    inviter_id: int
    created_at: int
    status: int
    invitation_context: str


class Histories(BaseModel):
    message: Optional[Message]
    action_log: Optional[ActionLog]
