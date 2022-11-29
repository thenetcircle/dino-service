"""change_overview_field_to_text

Revision ID: 04e83c5cd0a1
Revises: 81ca0935443a
Create Date: 2022-11-29 10:59:36.759682+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '04e83c5cd0a1'
down_revision = '81ca0935443a'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        table_name="groups",
        column_name="last_message_overview",
        nullable=True,
        type_=sa.Text()
    )


def downgrade():
    op.alter_column(
        table_name="groups",
        column_name="last_message_overview",
        nullable=True,
        type_=sa.VARCHAR(length=1024)
    )
