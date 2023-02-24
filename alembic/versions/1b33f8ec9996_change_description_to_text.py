"""change description to text

Revision ID: 1b33f8ec9996
Revises: f11339254fe6
Create Date: 2023-02-24 03:57:54.213255+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1b33f8ec9996'
down_revision = 'f11339254fe6'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        table_name="groups",
        column_name="description",
        nullable=True,
        type_=sa.Text()
    )


def downgrade():
    op.alter_column(
        table_name="groups",
        column_name="description",
        nullable=True,
        type_=sa.VARCHAR(length=256)
    )
