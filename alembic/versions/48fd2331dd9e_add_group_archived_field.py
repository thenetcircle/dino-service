"""add group archived field

Revision ID: 48fd2331dd9e
Revises: 99c2e87a6c41
Create Date: 2024-02-26 04:41:02.489370+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '48fd2331dd9e'
down_revision = '99c2e87a6c41'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        table_name="groups",
        column=sa.Column("archived", sa.Boolean(), nullable=False, server_default="false", default=False)
    )
    op.add_column(
        table_name="groups",
        column=sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True)
    )


def downgrade():
    op.drop_column(
        table_name="groups",
        column_name="archived_at"
    )
    op.drop_column(
        table_name="groups",
        column_name="archived"
    )
