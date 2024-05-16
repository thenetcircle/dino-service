"""remove group deleted archived add status changed at

Revision ID: b1e7d8d64b84
Revises: dce5f1bcc06b
Create Date: 2024-05-16 01:28:52.636475+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b1e7d8d64b84'
down_revision = 'dce5f1bcc06b'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("UPDATE groups SET status = -2 WHERE archived = true")
    op.execute("UPDATE groups SET status = -3 WHERE deleted = true")
    op.execute("UPDATE groups SET status = 0 WHERE status IS NULL")

    op.drop_column(
        table_name="groups",
        column_name="archived_at"
    )
    op.drop_column(
        table_name="groups",
        column_name="archived"
    )
    op.drop_column(
        table_name="groups",
        column_name="deleted_at"
    )
    op.drop_column(
        table_name="groups",
        column_name="deleted"
    )

    op.add_column(
        table_name="groups",
        column=sa.Column("status_changed_at", sa.DateTime(timezone=True), nullable=True)
    )

    op.execute("UPDATE groups SET status_changed_at = NOW() WHERE status != 0")
    op.alter_column("groups", "status", server_default="0", existing_type=sa.Integer(), nullable=False)


def downgrade():
    op.add_column(
        table_name="groups",
        column=sa.Column("deleted", sa.Boolean(), nullable=False, server_default="false", default=False)
    )
    op.add_column(
        table_name="groups",
        column=sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column(
        table_name="groups",
        column=sa.Column("archived", sa.Boolean(), nullable=False, server_default="false", default=False)
    )
    op.add_column(
        table_name="groups",
        column=sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True)
    )

    op.execute("UPDATE groups SET archived = true, archived_at = status_changed_at WHERE status = -2")
    op.execute("UPDATE groups SET deleted = true, deleted_at = status_changed_at WHERE status = -3")
    op.execute("UPDATE groups SET status = NULL WHERE status = 0")

    op.drop_column(
        table_name="groups",
        column_name="status_changed_at"
    )
    op.alter_column("groups", "status", existing_type=sa.Integer(), nullable=True)
