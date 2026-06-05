-- Run once against DATABASE_URL (Supabase/Postgres)

CREATE TABLE IF NOT EXISTS users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email VARCHAR(255) UNIQUE NOT NULL,
  is_email_verified BOOLEAN NOT NULL DEFAULT FALSE,
  device_fingerprint VARCHAR(255),
  role VARCHAR(50) NOT NULL DEFAULT 'CLIENT',
  allowed_info_scope JSONB NOT NULL DEFAULT '["general"]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  credits_used_today INT NOT NULL DEFAULT 0,
  credits_reset_date DATE NOT NULL DEFAULT CURRENT_DATE,
  whatsapp_shares_today INT NOT NULL DEFAULT 0,
  whatsapp_share_day DATE,
  whatsapp_bonus_awarded_date DATE
);

CREATE TABLE IF NOT EXISTS whatsapp_shares (
  id SERIAL PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  share_batch_hash VARCHAR(128) UNIQUE NOT NULL,
  shared_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_users_device_fingerprint ON users(device_fingerprint);
CREATE INDEX IF NOT EXISTS idx_whatsapp_shares_user_id ON whatsapp_shares(user_id);
CREATE INDEX IF NOT EXISTS idx_whatsapp_shares_shared_at ON whatsapp_shares(shared_at);

-- Awards +1 bonus daily credit after 3 unique viral shares in a day
CREATE OR REPLACE FUNCTION increment_user_limit(p_email TEXT)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  UPDATE users
  SET whatsapp_bonus_awarded_date = CURRENT_DATE,
      whatsapp_shares_today = GREATEST(whatsapp_shares_today, 3),
      whatsapp_share_day = CURRENT_DATE
  WHERE email = p_email
    AND (
      whatsapp_bonus_awarded_date IS NULL
      OR whatsapp_bonus_awarded_date < CURRENT_DATE
    );
END;
$$;
