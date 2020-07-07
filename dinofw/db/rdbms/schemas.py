from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class GroupBase(BaseModel):
    group_id: str
    name: str
    description: Optional[str]
    created_at: datetime

    last_message_time: datetime
    last_message_overview: Optional[str]

    group_type: int
    owner_id: int

    updated_at: Optional[datetime]
    group_meta: Optional[int]
    group_context: Optional[str]


class LastReadBase(BaseModel):
    group_id: str
    user_id: int
    last_read: datetime


class GroupCreate(GroupBase):
    pass


class LastReadCreate(LastReadBase):
    pass


class Group(GroupBase):
    id: int

    class Config:
        orm_mode = True


class LastRead(LastReadBase):
    id: int

    class Config:
        orm_mode = True