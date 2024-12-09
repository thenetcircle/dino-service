import uuid

from cassandra.cqlengine.columns import DateTime
from cassandra.cqlengine.columns import Integer
from cassandra.cqlengine.columns import Text
from cassandra.cqlengine.columns import UUID

from dinofw.db.storage.aiocqlengine import AioModel


class MessageModel(AioModel):
    __table_name__ = "messages"

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
    message_id = UUID(
        required=True,
        default=uuid.uuid4
    )

    file_id = Text(
        required=False
    )
    message_payload = Text(
        required=False
    )

    # user for quotes, reactions, etc.
    context = Text(
        required=False
    )

    message_type = Integer(
        required=True
    )
    updated_at = DateTime()
    removed_at = DateTime()


class AttachmentModel(AioModel):
    # duplicate attachments from message table to this table for fast querying
    __table_name__ = "attachments"

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
    message_id = UUID(
        required=True,
        default=uuid.uuid4
    )
    file_id = Text(
        required=True
    )
    message_payload = Text(
        required=False
    )
    context = Text(
        required=False
    )
    message_type = Integer(
        required=True
    )
    updated_at = DateTime()
