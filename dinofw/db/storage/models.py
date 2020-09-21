import uuid

from cassandra.cqlengine.columns import UUID, Boolean
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
    # attachment placeholders doesn't have a body
    message_payload = Text(
        required=False
    )

    status = Integer()
    message_type = Integer()
    updated_at = DateTime()


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
    context = Text(
        required=False
    )

    admin_id = Integer()


class AttachmentModel(Model):
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
    attachment_id = UUID(
        required=True,
        default=uuid.uuid4,
    )
    message_id = UUID(
        required=True,
    )
    file_id = Text(
        required=True,
    )
    status = Integer(
        required=True
    )
    context = Text(
        required=True,
    )
    updated_at = DateTime(
        required=False,
    )

    # TODO: figure out if use a json body or all just fields, dont' need to filter on them, but need to update some
    """
    is_resized = Boolean(
        required=True,
        default=False,
    )
    const FIELD_EXT_NAME       = 'ext_name';
    const FIELD_FILE_ID        = 'file_id';
    const FIELD_ORIGIN_NAME    = 'origin_name';
    const FIELD_SIZE           = 'size';
    const FIELD_IP_ADDR        = 'ip_addr';
    const FIELD_PLAY_TIME      = 'play_time';
    const FIELD_SCREENSHOT     = 'screenshot';
    const FIELD_WIDTH          = 'width';
    const FIELD_HEIGHT         = 'height';
    const FIELD_STATUS         = 'status';
    const FIELD_UPDATED        = 'updated';
    
    id int(11) NOT NULL AUTO_INCREMENT,
    msg_id int(11) NOT NULL,
    sid int(11) NOT NULL,
    ip_addr varchar(35) CHARACTER SET utf8 NOT NULL,
    ip_addr_new varchar(64) NOT NULL,
    filename varchar(32) CHARACTER SET utf8 NOT NULL,
    filesize int(7) NOT NULL,
    origname varchar(50) CHARACTER SET utf8 NOT NULL,
    width int(4) DEFAULT '0',
    height int(4) DEFAULT '0',
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_on timestamp NULL DEFAULT NULL,
    is_resized tinyint(1) DEFAULT '1',
    new_msg_id varchar(23) CHARACTER SET utf8 DEFAULT NULL,
    """
