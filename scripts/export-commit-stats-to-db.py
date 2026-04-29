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
import re
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


def noted_commit_shas() -> set[str]:
    """Return the set of commit SHAs that have refs/notes/ai notes attached."""
    try:
        out = run_cmd(["git", "notes", "--ref=ai", "list"]).strip()
    except RuntimeError:
        return set()
    shas: set[str] = set()
    for line in out.splitlines():
        parts = line.strip().split()
        if len(parts) >= 2:
            shas.add(parts[1])
    return shas


_LOG_SEP = "\x1f"


def list_all_commits(
    since_days: int | None,
    base_commit: str | None = None,
) -> list[dict[str, object]]:
    """
    Return metadata for commits reachable from HEAD.

    Scoping (in priority order):
      1. base_commit (BASE_COMMIT env var) -- git log BASE_COMMIT..HEAD
         Use this to limit to your fork/project start point so upstream
         history from other contributors is excluded.
      2. since_days (SINCE_DAYS env var)  -- git log --since=N days ago
      3. Neither set                       -- all history (may be very large)

    Each entry contains: commit_sha, author_name, author_email,
    commit_timestamp, commit_date, subject, insertions, deletions.
    """
    fmt = _LOG_SEP.join(["%H", "%an", "%ae", "%at", "%s"])
    cmd = ["git", "log", f"--format=COMMIT:{fmt}", "--shortstat"]
    if base_commit:
        # Only commits after the given SHA (exclusive)
        cmd += [f"{base_commit}..HEAD"]
    elif since_days is not None and since_days > 0:
        cmd += [f"--since={since_days} days ago"]

    try:
        out = run_cmd(cmd).strip()
    except RuntimeError:
        return []

    commits: list[dict[str, object]] = []
    current: dict[str, object] | None = None
    for line in out.splitlines():
        if line.startswith("COMMIT:"):
            if current is not None:
                commits.append(current)
            parts = line[7:].split(_LOG_SEP)
            if len(parts) < 5:
                current = None
                continue
            ts = int(parts[3])
            current = {
                "commit_sha": parts[0],
                "author_name": parts[1],
                "author_email": parts[2],
                "commit_timestamp": ts,
                "commit_date": time.strftime("%Y-%m-%d", time.localtime(ts)),
                "subject": parts[4],
                "insertions": 0,
                "deletions": 0,
            }
        elif current is not None and " changed" in line:
            m = re.search(r"(\d+) insertion", line)
            if m:
                current["insertions"] = int(m.group(1))
            m = re.search(r"(\d+) deletion", line)
            if m:
                current["deletions"] = int(m.group(1))

    if current is not None:
        commits.append(current)
    return commits


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


def collect_rows(since_days: int | None, base_commit: str | None = None) -> list[tuple]:
    """
    Build upsert rows for ALL commits in scope, not just noted ones.

    - Commits with a refs/notes/ai note  → enriched with full git-ai attribution stats.
    - Commits without a note (e.g. other developers, GitHub web edits) → still
      inserted with real commit metadata (author, insertions, deletions) and
      has_note=False; all attribution fields are 0 so the dashboard counts them
      as entirely Unattributed, keeping all authors visible.
    """
    all_commits = list_all_commits(since_days, base_commit=base_commit)
    if not all_commits:
        print("  No commits found in the given time window.", file=sys.stderr)
        return []

    noted = noted_commit_shas()
    print(
        f"  Found {len(all_commits)} commit(s) total, {len(noted)} with git-ai notes.",
        file=sys.stderr,
    )

    rows: list[tuple] = []
    skipped = 0
    updated_at = int(time.time())

    for meta in all_commits:
        sha = str(meta["commit_sha"])
        commit_ts = int(meta["commit_timestamp"])  # type: ignore[arg-type]

        has_note = sha in noted

        if has_note:
            try:
                stats = get_stats(sha)
            except Exception as exc:
                print(f"  WARN: could not get stats for {sha}: {exc}", file=sys.stderr)
                stats = {}
                skipped += 1

            tools = flatten_tools(stats.get("tool_model_breakdown"))
            ai_add = int(stats.get("ai_additions", 0) or 0)
            human_add = int(stats.get("human_additions", 0) or 0)
            unknown_add = int(stats.get("unknown_additions", 0) or 0)
            mixed_add = int(stats.get("mixed_additions", 0) or 0)
            insertions = int(stats.get("git_diff_added_lines", 0) or 0) or int(meta["insertions"])  # type: ignore[arg-type]
            deletions = int(stats.get("git_diff_deleted_lines", 0) or 0) or int(meta["deletions"])  # type: ignore[arg-type]
        else:
            # No git-ai note: treat all lines as unattributed so the commit still
            # appears in the dashboard under the correct author.
            tools = {}
            ai_add = 0
            human_add = 0
            unknown_add = int(meta["insertions"])  # type: ignore[arg-type]
            mixed_add = 0
            insertions = int(meta["insertions"])  # type: ignore[arg-type]
            deletions = int(meta["deletions"])  # type: ignore[arg-type]

        rows.append(
            (
                sha,
                commit_ts,
                str(meta["commit_date"]),
                str(meta["author_name"]),
                str(meta["author_email"]),
                str(meta["subject"]),
                insertions,
                deletions,
                ai_add,
                human_add,
                unknown_add,
                mixed_add,
                has_note,
                json.dumps(tools, separators=(",", ":")),
                updated_at,
            )
        )

    if skipped:
        print(f"  WARN: {skipped} commit(s) had note-read errors; stored with partial/zero stats.", file=sys.stderr)
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
        print(f"WARN: invalid SINCE_DAYS={raw!r}; ignoring.", file=sys.stderr)
        return None
    if days <= 0:
        return None
    return days


def parse_base_commit() -> str | None:
    """Return the BASE_COMMIT SHA from env, or None if not set."""
    val = os.environ.get("BASE_COMMIT", "").strip()
    return val if val else None


def main() -> None:
    database_url = get_env("DATABASE_URL")

    repo_root = Path.cwd()
    print("export-commit-stats-to-db", file=sys.stderr)
    print(f"  Repo: {repo_root}", file=sys.stderr)

    base_commit = parse_base_commit()
    since_days = parse_since_days()

    if base_commit:
        print(f"  Scope: commits after {base_commit[:12]} (BASE_COMMIT)", file=sys.stderr)
    elif since_days is not None:
        print(f"  Scope: commits in last {since_days} day(s)", file=sys.stderr)
    else:
        print("  Scope: all commits (no BASE_COMMIT or SINCE_DAYS set)", file=sys.stderr)

    rows = collect_rows(since_days, base_commit=base_commit)
    print(f"  Collected {len(rows)} commit stat row(s) (noted + un-noted)", file=sys.stderr)
    if not rows:
        print("  Nothing to export.", file=sys.stderr)
        return

    export_rows(rows, database_url)


if __name__ == "__main__":
    main()
