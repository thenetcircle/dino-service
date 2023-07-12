"""add table deleted_stats

Revision ID: b2b64b49599f
Revises: 25caf1d2067e
Create Date: 2023-07-11 23:39:05.049457+00:00

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import engine_from_config
from sqlalchemy.engine import reflection


# revision identifiers, used by Alembic.
revision = 'b2b64b49599f'
down_revision = '25caf1d2067e'
branch_labels = None
depends_on = None


def _has_table(table_name):
    config = op.get_context().config
    engine = engine_from_config(
        config.get_section(config.config_ini_section), prefix="sqlalchemy."
    )
    inspector = reflection.Inspector.from_engine(engine)
    tables = inspector.get_table_names()
    return table_name in tables


def upgrade():
    op.create_table(
        "deleted_stats",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("group_id", sa.String(36), nullable=False, index=True),
        sa.Column("user_id", sa.Integer, nullable=False, index=True),
        sa.Column("group_type", sa.Integer, nullable=False),

        sa.Column("join_time", sa.DateTime(timezone=True)),
        sa.Column("delete_time", sa.DateTime(timezone=True))
    )


def downgrade():
    pass
