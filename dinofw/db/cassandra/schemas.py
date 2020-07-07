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


"""
class JoinerModel(Model):
    __table_name__ = "joiners"

    group_id = UUID(
        required=True,
        primary_key=True,
        partition_key=True,
    )
    created_at = DateTime(
        required=True,
        primary_key=True,
        clustering_order="DESC",
    )
    inviter_id = Integer(
        required=True,
        primary_key=True,
    )
    joined_id = Integer(
        required=True,
        primary_key=True,
    )
    status = Integer(
        required=True
    )
    invitation_context = Text(
        required=True
    )


class ActionLogModel(Model):
    __table_name__ = "action_logs"

    group_id = UUID(
        required=True,
        primary_key=True,
        partition_key=True,
    )
    created_at = DateTime(
        required=True,
        primary_key=True,
        clustering_order="DESC",
    )
    user_id = Integer(
        required=True,
        primary_key=True,
    )
    action_id = UUID(
        required=True,
        default=uuid.uuid4
    )
    action_type = Integer(
        required=True
    )

    admin_id = Integer()
    message_id = UUID()
"""