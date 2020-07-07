import uuid

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model


class JoinerModel(Model):
    __table_name__ = "joiners"

    group_id = columns.UUID(
        required=True,
        primary_key=True,
        partition_key=True,
    )
    created_at = columns.DateTime(
        required=True,
        primary_key=True,
        clustering_order="DESC",
    )
    inviter_id = columns.Integer(
        required=True,
        primary_key=True,)
    joined_id = columns.Integer(
        required=True,
        primary_key=True,
    )
    status = columns.Integer(
        required=True
    )
    invitation_context = columns.Text(
        required=True
    )


class ActionLogModel(Model):
    __table_name__ = "action_logs"

    group_id = columns.UUID(
        required=True,
        primary_key=True,
        partition_key=True,
    )
    created_at = columns.DateTime(
        required=True,
        primary_key=True,
        clustering_order="DESC",
    )
    user_id = columns.Integer(
        required=True,
        primary_key=True,
    )
    action_id = columns.UUID(
        required=True,
        default=uuid.uuid4
    )
    action_type = columns.Integer(
        required=True
    )

    admin_id = columns.Integer()
    message_id = columns.UUID()


class MessageModel(Model):
    __table_name__ = "messages"

    group_id = columns.UUID(
        required=True,
        primary_key=True,
        partition_key=True,
    )
    created_at = columns.DateTime(
        required=True,
        primary_key=True,
        clustering_order="DESC",
    )
    user_id = columns.Integer(
        required=True,
        primary_key=True,
    )
    message_id = columns.UUID(
        required=True,
        default=uuid.uuid4
    )
    message_payload = columns.Text(
        required=True
    )

    status = columns.Integer()
    message_type = columns.Integer()
    updated_at = columns.DateTime()
    removed_by_user = columns.Integer()
    last_action_log_id = columns.UUID()
