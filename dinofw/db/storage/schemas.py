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


class ActionLogBase(BaseModel):
    group_id: str
    created_at: datetime
    user_id: int
    action_id: str
    action_type: int

    admin_id: Optional[int]


class AttachmentBase(BaseModel):
    group_id: str
    attachment_id: str
    message_id: str
    user_id: int
    is_resized: bool
    context: str
    filename: str

    created_at: datetime
    updated_at: Optional[datetime]
