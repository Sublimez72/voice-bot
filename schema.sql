PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS voice_sessions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  channel_id INTEGER NOT NULL,
  joined_ts INTEGER NOT NULL,
  left_ts INTEGER
);

CREATE INDEX IF NOT EXISTS idx_voice_open ON voice_sessions(user_id) WHERE left_ts IS NULL;
CREATE INDEX IF NOT EXISTS idx_voice_join ON voice_sessions(joined_ts);
