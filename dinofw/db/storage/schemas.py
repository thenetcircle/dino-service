from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class MessageBase(BaseModel):
    group_id: str
    created_at: datetime
    user_id: int
    message_id: str
    message_payload: Optional[str]

    status: Optional[int]
    message_type: Optional[int]
    updated_at: Optional[datetime]


class ActionLogBase(BaseModel):
    group_id: str
    created_at: datetime
    user_id: int
    action_id: str
    action_type: int
    context: Optional[str]

    admin_id: Optional[int]


class AttachmentBase(BaseModel):
    group_id: str
    attachment_id: str
    message_id: str
    user_id: int
    context: str
    file_id: str
    status: int

    created_at: datetime
    updated_at: Optional[datetime]
