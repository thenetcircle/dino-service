from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class MessageBase(BaseModel):
    group_id: str
    created_at: datetime
    user_id: int
    message_id: str
    message_payload: str

    status: Optional[int]
    message_type: Optional[int]
    updated_at: Optional[datetime]
    removed_at: Optional[datetime]
    removed_by_user: Optional[int]
    last_action_log_id: Optional[str]


class JoinerBase(MessageBase):
    group_id: str
    created_at: datetime
    inviter_id: int
    joined_id: int
    status: int
    invitation_context: Optional[str]


class ActionLogBase(BaseModel):
    group_id: str
    created_at: datetime
    user_id: int
    action_id: str
    action_type: int

    admin_id: Optional[int]
    message_id: Optional[int]
