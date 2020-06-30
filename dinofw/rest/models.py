from typing import Optional, List

from pydantic import BaseModel


class PaginationQuery(BaseModel):
    page: int
    per_page: int


class HistoryQuery(PaginationQuery):
    time_from: Optional[int] = None
    time_to: Optional[int] = None
    message_type: Optional[int] = None
    status: Optional[int] = None


class SearchQuery(PaginationQuery):
    keyword: Optional[str]
    group_type: Optional[int]
    status: Optional[int]


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
