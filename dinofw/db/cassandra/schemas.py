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
    message_type: Optional[str]
    updated_at: Optional[datetime]
    removed_at: Optional[datetime]
    removed_by_user: Optional[int]
    last_action_log_id: Optional[str]


class ActionLogBase(BaseModel):
    group_id: str
    created_at: datetime
    user_id: int
    action_id: str
    action_type: int

    admin_id: Optional[int]
    message_id: Optional[int]
