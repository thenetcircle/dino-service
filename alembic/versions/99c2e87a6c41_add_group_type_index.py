"""add group type index

Revision ID: 99c2e87a6c41
Revises: b2b64b49599f
Create Date: 2024-02-26 04:40:47.080276+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '99c2e87a6c41'
down_revision = 'b2b64b49599f'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(
        "ix_groups_group_type",
        "groups",
        ["group_type"]
    )


def downgrade():
    op.drop_index(
        "ix_groups_group_type",
        table_name="groups"
    )
