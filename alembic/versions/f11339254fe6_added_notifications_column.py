"""added notifications column

Revision ID: f11339254fe6
Revises: 04e83c5cd0a1
Create Date: 2022-12-20 07:50:12.422900+00:00

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import Column, Boolean

# revision identifiers, used by Alembic.
revision = 'f11339254fe6'
down_revision = '04e83c5cd0a1'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        table_name="user_group_stats",
        column=Column("notifications", Boolean(), nullable=False, server_default="true", default=True)
    )


def downgrade():
    op.drop_column(
        table_name="user_group_stats",
        column_name="notifications"
    )
