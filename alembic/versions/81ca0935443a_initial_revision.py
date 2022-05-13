"""initial revision

Revision ID: 81ca0935443a
Revises: 
Create Date: 2022-05-13 04:47:56.859606+00:00

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import engine_from_config
from sqlalchemy.engine import reflection


# revision identifiers, used by Alembic.
revision = "81ca0935443a"
down_revision = None
branch_labels = None
depends_on = None


def _has_table(table_name):
    config = op.get_context().config
    engine = engine_from_config(
        config.get_section(config.config_ini_section), prefix="sqlalchemy."
    )
    inspector = reflection.Inspector.from_engine(engine)
    tables = inspector.get_table_names()
    return table_name in tables


def upgrade():
    op.create_table(
        "groups",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("group_id", sa.String(36), nullable=False, index=True),
        sa.Column("name", sa.String(128), nullable=False),

        sa.Column("owner_id", sa.Integer),
        sa.Column("status", sa.Integer, server_default="0"),
        sa.Column("group_type", sa.Integer),
        sa.Column("created_at", sa.DateTime(timezone=True)),

        sa.Column("first_message_time", sa.DateTime(timezone=True), index=True, nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), index=True),

        sa.Column("last_message_time", sa.DateTime(timezone=True), index=True, nullable=False),
        sa.Column("last_message_user_id", sa.Integer, nullable=True),
        sa.Column("last_message_id", sa.String(36), nullable=True),
        sa.Column("last_message_type", sa.Integer, nullable=False, server_default="0"),
        sa.Column("last_message_overview", sa.String(1024), nullable=True),

        sa.Column("meta", sa.Integer, nullable=True),
        sa.Column("description", sa.String(512), nullable=True)
    )
    op.create_unique_constraint("ix_groups_group_id_unique", "groups", ["group_id"])

    op.create_table(
        "user_group_stats",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("group_id", sa.String(36), nullable=False, index=True),
        sa.Column("user_id", sa.Integer, nullable=False, index=True),

        sa.Column("last_read", sa.DateTime(timezone=True)),
        sa.Column("last_sent", sa.DateTime(timezone=True)),
        sa.Column("delete_before", sa.DateTime(timezone=True)),
        sa.Column("join_time", sa.DateTime(timezone=True)),
        sa.Column("first_sent", sa.DateTime(timezone=True)),

        sa.Column("unread_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("sent_message_count", sa.Integer, nullable=False, server_default="-1"),
        sa.Column("last_updated_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("highlight_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("receiver_highlight_time", sa.DateTime(timezone=True), nullable=False),

        sa.Column("pin", sa.Boolean, default=False, nullable=False, index=True, server_default="false"),
        sa.Column("hide", sa.Boolean, default=False, nullable=False, server_default="false"),
        sa.Column("deleted", sa.Boolean, default=False, nullable=False, server_default="false"),
        sa.Column("bookmark", sa.Boolean, nullable=False, default=False, server_default="false"),
        sa.Column("rating", sa.Integer, nullable=True),
    )
    op.create_unique_constraint("ugs_group_user_unique", "user_group_stats", ["group_id", "user_id"])


def downgrade():
    pass
