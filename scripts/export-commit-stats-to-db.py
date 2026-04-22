#!/usr/bin/env python3
"""
export-commit-stats-to-db.py
============================
Collect commit-level git-ai attribution stats from refs/notes/ai and upsert them
into Postgres / Supabase.

Why this script exists:
- `git-ai prompts` exports prompt sessions, not commit-level attribution.
- A dashboard that needs accurate commit counts and human/AI split should use
  per-commit stats from `git-ai stats <sha> --json`.

Required environment variables:
  DATABASE_URL  Postgres connection string

Optional environment variables:
  SINCE_DAYS    Integer; only export commits newer than N days (default: all)
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path


def get_env(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        print(f"ERROR: environment variable {name} is required but not set.", file=sys.stderr)
        sys.exit(1)
    return val


def run_cmd(args: list[str]) -> str:
    proc = subprocess.run(args, capture_output=True, text=True)
    if proc.returncode != 0:
        quoted = " ".join(shlex.quote(a) for a in args)
        raise RuntimeError(f"Command failed: {quoted}\n{proc.stderr.strip()}")
    return proc.stdout


def list_noted_commits() -> list[str]:
    """Return commit SHAs that have refs/notes/ai notes attached."""
    out = run_cmd(["git", "notes", "--ref=ai", "list"]).strip()
    if not out:
        return []

    shas: list[str] = []
    seen: set[str] = set()
    for line in out.splitlines():
        parts = line.strip().split()
        if len(parts) < 2:
            continue
        commit_sha = parts[1]
        if commit_sha not in seen:
            seen.add(commit_sha)
            shas.append(commit_sha)
    return shas


def get_commit_meta(commit_sha: str) -> dict[str, object]:
    fmt = "%H%x1f%an%x1f%ae%x1f%at%x1f%s"
    out = run_cmd(["git", "show", "-s", f"--format={fmt}", commit_sha]).strip()
    parts = out.split("\x1f")
    if len(parts) < 5:
        raise RuntimeError(f"Unexpected git show output for commit {commit_sha}: {out!r}")

    ts = int(parts[3])
    date = time.strftime("%Y-%m-%d", time.localtime(ts))
    return {
        "commit_sha": parts[0],
        "author_name": parts[1],
        "author_email": parts[2],
        "commit_timestamp": ts,
        "commit_date": date,
        "subject": parts[4],
    }


def get_stats(commit_sha: str) -> dict[str, object]:
    out = run_cmd(["git-ai", "stats", commit_sha, "--json"]).strip()
    return json.loads(out) if out else {}


def flatten_tools(tool_model_breakdown: object) -> dict[str, int]:
    """
    Convert git-ai tool/model breakdown to a compact map suitable for dashboarding.
    Example key conversion: github-copilot::copilot/auto -> github-copilot/copilot/auto
    """
    totals: dict[str, int] = {}
    if not isinstance(tool_model_breakdown, dict):
        return totals

    for raw_key, raw_val in tool_model_breakdown.items():
        key = str(raw_key).replace("::", "/")
        lines = 0
        if isinstance(raw_val, dict):
            lines = int(raw_val.get("ai_additions", 0) or 0) + int(raw_val.get("mixed_additions", 0) or 0)
        elif isinstance(raw_val, (int, float)):
            lines = int(raw_val)

        if lines <= 0:
            continue
        totals[key] = totals.get(key, 0) + lines

    return totals


def collect_rows(since_days: int | None) -> list[tuple]:
    commits = list_noted_commits()
    if not commits:
        return []

    min_epoch = None
    if since_days is not None:
        min_epoch = int(time.time()) - since_days * 86400

    rows: list[tuple] = []
    skipped = 0
    for sha in commits:
        try:
            meta = get_commit_meta(sha)
        except Exception as exc:
            print(f"  WARN: skipping commit {sha}: {exc}", file=sys.stderr)
            skipped += 1
            continue

        commit_ts = int(meta["commit_timestamp"])
        if min_epoch is not None and commit_ts < min_epoch:
            continue

        try:
            stats = get_stats(sha)
        except Exception as exc:
            print(f"  WARN: skipping stats for {sha}: {exc}", file=sys.stderr)
            skipped += 1
            continue

        tools = flatten_tools(stats.get("tool_model_breakdown"))
        updated_at = int(time.time())

        rows.append(
            (
                meta["commit_sha"],
                commit_ts,
                meta["commit_date"],
                meta["author_name"],
                meta["author_email"],
                meta["subject"],
                int(stats.get("git_diff_added_lines", 0) or 0),
                int(stats.get("git_diff_deleted_lines", 0) or 0),
                int(stats.get("ai_additions", 0) or 0),
                int(stats.get("human_additions", 0) or 0),
                int(stats.get("unknown_additions", 0) or 0),
                int(stats.get("mixed_additions", 0) or 0),
                True,
                json.dumps(tools, separators=(",", ":")),
                updated_at,
            )
        )

    if skipped:
        print(f"  WARN: skipped {skipped} commit(s) due to read/stat errors", file=sys.stderr)
    return rows


DDL = """
CREATE TABLE IF NOT EXISTS git_ai_commit_stats (
    commit_sha       TEXT PRIMARY KEY,
    commit_timestamp BIGINT NOT NULL,
    commit_date      TEXT NOT NULL,
    author_name      TEXT,
    author_email     TEXT,
    subject          TEXT,
    insertions       INTEGER NOT NULL DEFAULT 0,
    deletions        INTEGER NOT NULL DEFAULT 0,
    ai_additions     INTEGER NOT NULL DEFAULT 0,
    human_additions  INTEGER NOT NULL DEFAULT 0,
    unknown_additions INTEGER NOT NULL DEFAULT 0,
    mixed_additions  INTEGER NOT NULL DEFAULT 0,
    has_note         BOOLEAN NOT NULL DEFAULT TRUE,
    tools_json       TEXT NOT NULL DEFAULT '{}',
    updated_at       BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gacs_commit_timestamp ON git_ai_commit_stats(commit_timestamp);
CREATE INDEX IF NOT EXISTS idx_gacs_author_name      ON git_ai_commit_stats(author_name);
CREATE INDEX IF NOT EXISTS idx_gacs_has_note         ON git_ai_commit_stats(has_note);
"""

UPSERT = """
INSERT INTO git_ai_commit_stats (
    commit_sha,
    commit_timestamp,
    commit_date,
    author_name,
    author_email,
    subject,
    insertions,
    deletions,
    ai_additions,
    human_additions,
    unknown_additions,
    mixed_additions,
    has_note,
    tools_json,
    updated_at
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (commit_sha) DO UPDATE SET
    commit_timestamp  = EXCLUDED.commit_timestamp,
    commit_date       = EXCLUDED.commit_date,
    author_name       = EXCLUDED.author_name,
    author_email      = EXCLUDED.author_email,
    subject           = EXCLUDED.subject,
    insertions        = EXCLUDED.insertions,
    deletions         = EXCLUDED.deletions,
    ai_additions      = EXCLUDED.ai_additions,
    human_additions   = EXCLUDED.human_additions,
    unknown_additions = EXCLUDED.unknown_additions,
    mixed_additions   = EXCLUDED.mixed_additions,
    has_note          = EXCLUDED.has_note,
    tools_json        = EXCLUDED.tools_json,
    updated_at        = EXCLUDED.updated_at;
"""


def export_rows(rows: list[tuple], database_url: str) -> None:
    import psycopg2  # type: ignore

    print("  Connecting to Postgres ...", file=sys.stderr)
    conn = psycopg2.connect(database_url)
    try:
        with conn:
            with conn.cursor() as cur:
                for stmt in DDL.strip().split(";"):
                    stmt = stmt.strip()
                    if stmt:
                        cur.execute(stmt)

                print(f"  Upserting {len(rows)} commit stat row(s) ...", file=sys.stderr)
                batch_size = 100
                done = 0
                for i in range(0, len(rows), batch_size):
                    chunk = rows[i : i + batch_size]
                    cur.executemany(UPSERT, chunk)
                    done += len(chunk)
                    print(f"    {done}/{len(rows)} ...", file=sys.stderr)

        print(f"  Done. {len(rows)} commit stat row(s) upserted.", file=sys.stderr)
    finally:
        conn.close()


def parse_since_days() -> int | None:
    raw = os.environ.get("SINCE_DAYS", "").strip()
    if not raw:
        return None
    try:
        days = int(raw)
    except ValueError:
        print(f"WARN: invalid SINCE_DAYS={raw!r}; exporting all noted commits.", file=sys.stderr)
        return None
    if days <= 0:
        return None
    return days


def main() -> None:
    database_url = get_env("DATABASE_URL")

    repo_root = Path.cwd()
    print("export-commit-stats-to-db", file=sys.stderr)
    print(f"  Repo: {repo_root}", file=sys.stderr)

    since_days = parse_since_days()
    if since_days is None:
        print("  Scope: all noted commits", file=sys.stderr)
    else:
        print(f"  Scope: commits in last {since_days} day(s)", file=sys.stderr)

    rows = collect_rows(since_days)
    print(f"  Collected {len(rows)} noted commit stat row(s)", file=sys.stderr)
    if not rows:
        print("  Nothing to export.", file=sys.stderr)
        return

    export_rows(rows, database_url)


if __name__ == "__main__":
    main()
