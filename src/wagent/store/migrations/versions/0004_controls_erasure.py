"""Add durable limit, system-message, and erasure controls."""

import sqlalchemy as sa
from alembic import op

revision = "0004_controls_erasure"
down_revision = "0003_status_usage"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "conversations",
        sa.Column("erasing", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column("conversations", sa.Column("last_rate_notice_at", sa.DateTime(timezone=True)))
    op.add_column(
        "turns",
        sa.Column("agent_attempted", sa.Boolean(), nullable=False, server_default=sa.true()),
    )
    op.add_column(
        "outbound_messages",
        sa.Column("purpose", sa.String(), nullable=False, server_default="agent"),
    )
    op.add_column("outbound_messages", sa.Column("cap_reserved_at", sa.DateTime(timezone=True)))
    op.execute(
        sa.text(
            "UPDATE outbound_messages SET cap_reserved_at = COALESCE(sent_at, sending_at) "
            "WHERE status IN ('sending','unknown','sent','delivered','read') "
            "OR wa_message_id IS NOT NULL"
        )
    )


def downgrade() -> None:
    op.drop_column("outbound_messages", "cap_reserved_at")
    op.drop_column("outbound_messages", "purpose")
    op.drop_column("turns", "agent_attempted")
    op.drop_column("conversations", "last_rate_notice_at")
    op.drop_column("conversations", "erasing")
