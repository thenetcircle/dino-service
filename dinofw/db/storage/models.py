import uuid

from cassandra.cqlengine.columns import UUID
from cassandra.cqlengine.columns import DateTime
from cassandra.cqlengine.columns import Integer
from cassandra.cqlengine.columns import Text
from cassandra.cqlengine.models import Model


class MessageModel(Model):
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

    # not required; if deleted we clear the body first
    # TODO: maybe just remove the row?
    message_payload = Text()

    status = Integer()
    message_type = Text()
    updated_at = DateTime()


class ActionLogModel(Model):
    __table_name__ = "action_logs"

    # TODO: do we need a body here? or type is enough?

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
