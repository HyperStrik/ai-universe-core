-- =============================================================================
-- AI Universe Core — Public Chat History Migration (Supabase / PostgreSQL)
-- Safe to run on existing production databases.
--
-- Creates ONLY: conversations, messages (+ indexes)
-- Does NOT: ALTER, DROP, or modify the users table or any credit/token columns.
-- Idempotent: safe to re-run (IF NOT EXISTS guards).
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- conversations — one thread per public chat session
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS conversations (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_email  TEXT NOT NULL,
  title       TEXT NOT NULL DEFAULT 'New conversation',
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  -- Used by server.js for ownership checks and CASCADE cleanup (does not modify users)
  user_id     UUID REFERENCES users(id) ON DELETE CASCADE,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE conversations IS 'Public dashboard chat threads keyed by verified user email';
COMMENT ON COLUMN conversations.user_email IS 'Denormalized owner email from validated session (not an FK to users.email)';
COMMENT ON COLUMN conversations.user_id IS 'Optional link to users.id for ownership and ON DELETE CASCADE';

-- -----------------------------------------------------------------------------
-- messages — individual turns inside a conversation
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS messages (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  conversation_id  UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  role             TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'system')),
  content          TEXT NOT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE messages IS 'User/assistant transcript rows for public SSE chat history';
COMMENT ON COLUMN messages.role IS 'Message author: user | assistant | system';

-- -----------------------------------------------------------------------------
-- Indexes (lookup performance for sidebar + thread load)
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_conversations_user_email
  ON conversations (user_email);

CREATE INDEX IF NOT EXISTS idx_conversations_user_id
  ON conversations (user_id);

CREATE INDEX IF NOT EXISTS idx_conversations_updated_at
  ON conversations (updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_messages_conversation_id
  ON messages (conversation_id);

CREATE INDEX IF NOT EXISTS idx_messages_created_at
  ON messages (created_at);

COMMIT;
