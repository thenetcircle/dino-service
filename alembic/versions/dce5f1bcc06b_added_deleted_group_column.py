"""added deleted group column

Revision ID: dce5f1bcc06b
Revises: e3dd7271ca46
Create Date: 2024-04-19 07:47:04.596286+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'dce5f1bcc06b'
down_revision = 'e3dd7271ca46'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        table_name="groups",
        column=sa.Column("deleted", sa.Boolean(), nullable=False, server_default="false", default=False)
    )
    op.add_column(
        table_name="groups",
        column=sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True)
    )


def downgrade():
    op.drop_column(
        table_name="groups",
        column_name="deleted_at"
    )
    op.drop_column(
        table_name="groups",
        column_name="deleted"
    )
