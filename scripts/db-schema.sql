-- git-ai dashboard schema
-- Run with: psql "$DATABASE_URL" -f scripts/db-schema.sql

CREATE TABLE IF NOT EXISTS git_ai_prompts (
    id                TEXT PRIMARY KEY,
    tool              TEXT,
    model             TEXT,
    external_thread_id TEXT,
    human_author      TEXT,
    commit_sha        TEXT,
    workdir           TEXT,
    total_additions   INTEGER,
    total_deletions   INTEGER,
    accepted_lines    INTEGER,
    overridden_lines  INTEGER,
    accepted_rate     REAL,
    start_time        BIGINT,
    last_time         BIGINT,
    created_at        BIGINT,
    updated_at        BIGINT,
    messages          TEXT
);

CREATE INDEX IF NOT EXISTS idx_gap_tool         ON git_ai_prompts(tool);
CREATE INDEX IF NOT EXISTS idx_gap_human_author ON git_ai_prompts(human_author);
CREATE INDEX IF NOT EXISTS idx_gap_start_time   ON git_ai_prompts(start_time);
CREATE INDEX IF NOT EXISTS idx_gap_commit_sha   ON git_ai_prompts(commit_sha);

CREATE TABLE IF NOT EXISTS git_ai_commit_stats (
    commit_sha        TEXT PRIMARY KEY,
    commit_timestamp  BIGINT NOT NULL,
    commit_date       TEXT NOT NULL,
    author_name       TEXT,
    author_email      TEXT,
    subject           TEXT,
    insertions        INTEGER NOT NULL DEFAULT 0,
    deletions         INTEGER NOT NULL DEFAULT 0,
    ai_additions      INTEGER NOT NULL DEFAULT 0,
    human_additions   INTEGER NOT NULL DEFAULT 0,
    unknown_additions INTEGER NOT NULL DEFAULT 0,
    mixed_additions   INTEGER NOT NULL DEFAULT 0,
    has_note          BOOLEAN NOT NULL DEFAULT TRUE,
    tools_json        TEXT NOT NULL DEFAULT '{}',
    updated_at        BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gacs_commit_timestamp ON git_ai_commit_stats(commit_timestamp);
CREATE INDEX IF NOT EXISTS idx_gacs_author_name      ON git_ai_commit_stats(author_name);
CREATE INDEX IF NOT EXISTS idx_gacs_has_note         ON git_ai_commit_stats(has_note);
