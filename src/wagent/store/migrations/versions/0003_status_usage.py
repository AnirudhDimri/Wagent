"""Add status receipts, per-message pricing, drop audits, and send retry time."""

import sqlalchemy as sa
from alembic import op

revision = "0003_status_usage"
down_revision = "0002_worker_outbox"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("outbound_messages", sa.Column("next_attempt_at", sa.DateTime(timezone=True)))
    op.create_table(
        "usage",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "outbound_id",
            sa.Uuid(),
            sa.ForeignKey("outbound_messages.id", ondelete="CASCADE"),
            unique=True,
            nullable=False,
        ),
        sa.Column("billable", sa.Boolean()),
        sa.Column("pricing_model", sa.String()),
        sa.Column("category", sa.String()),
        sa.Column("type", sa.String()),
        sa.Column("pricing", sa.JSON(), nullable=False),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("pricing_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_usage_recorded_at", "usage", ["recorded_at"])
    op.create_table(
        "message_status_events",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("phone_number_id", sa.String(), nullable=False),
        sa.Column("wa_message_id", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("wa_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("tracker", sa.String()),
        sa.Column("pricing", sa.JSON()),
        sa.Column("error_code", sa.String()),
        sa.Column("applied", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_message_status_events_wa_message_id", "message_status_events", ["wa_message_id"]
    )
    op.create_index("ix_message_status_events_tracker", "message_status_events", ["tracker"])
    op.create_table(
        "audit_events",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "conversation_id",
            sa.Uuid(),
            sa.ForeignKey("conversations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("type", sa.String(), nullable=False),
        sa.Column("actor", sa.String(), nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("meta", sa.JSON(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("audit_events")
    op.drop_index("ix_message_status_events_tracker", table_name="message_status_events")
    op.drop_index("ix_message_status_events_wa_message_id", table_name="message_status_events")
    op.drop_table("message_status_events")
    op.drop_index("ix_usage_recorded_at", table_name="usage")
    op.drop_table("usage")
    op.drop_column("outbound_messages", "next_attempt_at")
