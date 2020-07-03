from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class GroupBase(BaseModel):
    user_id: int
    group_id: str

    last_message_time: datetime
    created_at: datetime

    name: str
    description: Optional[str]
    last_message_overview: Optional[str]


class GroupCreate(GroupBase):
    pass


class Group(GroupBase):
    id: int

    class Config:
        orm_mode = True
