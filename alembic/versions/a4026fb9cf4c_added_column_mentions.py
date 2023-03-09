"""added column mentions

Revision ID: a4026fb9cf4c
Revises: 1b33f8ec9996
Create Date: 2023-03-09 02:34:03.839353+00:00

"""
from sqlalchemy import Column
from sqlalchemy import Integer

from alembic import op

# revision identifiers, used by Alembic.
revision = 'a4026fb9cf4c'
down_revision = '1b33f8ec9996'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        table_name="user_group_stats",
        column=Column("mentions", Integer(), nullable=False, server_default="0", default=0)
    )


def downgrade():
    op.drop_column(
        table_name="user_group_stats",
        column_name="mentions"
    )
