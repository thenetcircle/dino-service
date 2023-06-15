"""added kicked column

Revision ID: 25caf1d2067e
Revises: a4026fb9cf4c
Create Date: 2023-03-20 05:12:32.505097+00:00

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import Column, Boolean


# revision identifiers, used by Alembic.
revision = '25caf1d2067e'
down_revision = 'a4026fb9cf4c'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        table_name="user_group_stats",
        column=Column("kicked", Boolean(), nullable=False, server_default="false", default=False)
    )


def downgrade():
    op.drop_column(
        table_name="user_group_stats",
        column_name="kicked"
    )
