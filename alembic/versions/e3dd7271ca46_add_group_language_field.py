"""add group language field

Revision ID: e3dd7271ca46
Revises: 48fd2331dd9e
Create Date: 2024-02-27 02:47:17.986553+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e3dd7271ca46'
down_revision = '48fd2331dd9e'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        table_name="groups",
        column=sa.Column("language", sa.String(2), nullable=True)
    )


def downgrade():
    op.drop_column(
        table_name="groups",
        column_name="language"
    )
