from typing import Optional

from pydantic import BaseModel


class HistoryQuery(BaseModel):
    time_from: Optional[int] = None
    time_to: Optional[int] = None
    page: int
    per_page: int
    message_type: Optional[int] = None
    status: Optional[int] = None


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


class ActionLog(BaseModel):
    action_id: str
    user_id: int
    group_id: int
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


class Histories(BaseModel):
    message: Optional[Message]
    action_log: Optional[ActionLog]
