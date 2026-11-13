"""Create conversations and inbound messages."""

import sqlalchemy as sa
from alembic import op

revision = "0001_durable_inbox"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "conversations",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("phone_number_id", sa.String(), nullable=False),
        sa.Column("wa_id", sa.String(), nullable=False),
        sa.Column("thread_id", sa.String(), nullable=False, unique=True),
        sa.Column("mode", sa.String(), nullable=False, server_default="bot"),
        sa.Column("last_inbound_at", sa.DateTime(timezone=True)),
        sa.Column("opted_out", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("locale", sa.String()),
        sa.Column("lease_owner", sa.String()),
        sa.Column("lease_until", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("phone_number_id", "wa_id", name="uq_conversation_sender"),
        sa.CheckConstraint("mode IN ('bot', 'human', 'paused')", name="ck_conversation_mode"),
    )
    op.create_table(
        "inbound_messages",
        sa.Column("wa_message_id", sa.String(), primary_key=True),
        sa.Column(
            "conversation_id",
            sa.Uuid(),
            sa.ForeignKey("conversations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("wa_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column("text", sa.String()),
        sa.Column("media_id", sa.String()),
        sa.Column("raw", sa.JSON()),
        sa.Column("status", sa.String(), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.String()),
        sa.Column("turn_id", sa.String()),
        sa.CheckConstraint(
            "kind IN ('text','audio','image','document','location','interactive','other')",
            name="ck_inbound_kind",
        ),
        sa.CheckConstraint(
            "status IN ('pending','processing','done','failed','skipped')", name="ck_inbound_status"
        ),
    )
    op.create_index(
        "ix_inbound_queue",
        "inbound_messages",
        ["conversation_id", "status", "wa_timestamp", "received_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_inbound_queue", table_name="inbound_messages")
    op.drop_table("inbound_messages")
    op.drop_table("conversations")
