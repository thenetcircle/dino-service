import uuid
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model


class MessageModel(Model):
    group_id = columns.UUID(
        required=True,
        primary_key=True,
        partition_key=True,
        index=True,
    )
    created_at = columns.DateTime(
        required=True,
        primary_key=True,
        clustering_order="DESC",
    )
    user_id = columns.Integer(
        required=True,
        primary_key=True,
        index=True,
    )
    message_id = columns.UUID(
        required=True,
        default=uuid.uuid4
    )
    message_payload = columns.Text(required=True)

    status = columns.Integer()
    message_type = columns.Integer()
    updated_at = columns.DateTime()
    removed_by_user = columns.Integer()
    last_action_log_id = columns.UUID()

    # read_at = columns.DateTime()  # TODO: read by whom?


class GroupModel(Model):
    user_id = columns.Integer(
        required=True,
        primary_key=True,
        partition_key=True,
        index=True,
    )
    last_message_time = columns.DateTime(
        required=True,
        primary_key=True,
        clustering_order="DESC",
    )
    group_id = columns.UUID(
        required=True,
        index=True,
        default=uuid.uuid4,
    )

    name = columns.Text(required=True)
    created_at = columns.DateTime(required=True)
    updated_at = columns.DateTime()

    description = columns.Text()
    status = columns.Integer()
    group_type = columns.Integer()
    group_meta = columns.Integer()
    group_context = columns.Text()

    last_message_overview = columns.Text()
    last_message_user_id = columns.Integer()
