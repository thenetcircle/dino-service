from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class MessageBase(BaseModel):
    group_id: str
    created_at: datetime
    user_id: int
    message_id: str
    message_type: int
    status: Optional[int]
    file_id: Optional[str]
    message_payload: Optional[str]
    context: Optional[str]
    updated_at: Optional[datetime]
