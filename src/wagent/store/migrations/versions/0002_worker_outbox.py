"""Add durable turns, outbox, and retry scheduling."""

import sqlalchemy as sa
from alembic import op

revision = "0002_worker_outbox"
down_revision = "0001_durable_inbox"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("inbound_messages", sa.Column("next_attempt_at", sa.DateTime(timezone=True)))
    op.create_table(
        "turns",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "conversation_id",
            sa.Uuid(),
            sa.ForeignKey("conversations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("inbound_ids", sa.JSON(), nullable=False),
        sa.Column("outcome", sa.String()),
        sa.Column("latency_ms", sa.Integer()),
        sa.CheckConstraint(
            "outcome IN ('replied','handoff','skipped','error')", name="ck_turn_outcome"
        ),
    )
    op.create_table(
        "outbound_messages",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "conversation_id",
            sa.Uuid(),
            sa.ForeignKey("conversations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("turn_id", sa.String(), sa.ForeignKey("turns.id", ondelete="CASCADE")),
        sa.Column("dedupe_key", sa.String(), unique=True, nullable=False),
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column("body", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(), nullable=False, server_default="queued"),
        sa.Column("wa_message_id", sa.String(), unique=True),
        sa.Column("error_code", sa.String()),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sending_at", sa.DateTime(timezone=True)),
        sa.Column("sent_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "kind IN ('text','template','interactive','media')", name="ck_outbound_kind"
        ),
        sa.CheckConstraint(
            "status IN ('queued','sending','sent','delivered','read','failed','unknown')",
            name="ck_outbound_status",
        ),
    )
    op.create_index("ix_outbound_queue", "outbound_messages", ["status", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_outbound_queue", table_name="outbound_messages")
    op.drop_table("outbound_messages")
    op.drop_table("turns")
    op.drop_column("inbound_messages", "next_attempt_at")
