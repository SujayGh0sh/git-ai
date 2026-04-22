#!/usr/bin/env bash
set -euo pipefail

# Reusable runtime for git-ai attribution collection in any CI system.
# Run this script from the target repository root.
#
# Required env:
#   DATABASE_URL
#
# Optional env:
#   SINCE_DAYS   (default: 30)
#   PROMPTS_DB   (default: prompts.db)
#   OUTPUT_HTML  (default: git-ai-dashboard.html)
#   PYTHON_BIN   (default: python3)

MODULE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "ERROR: DATABASE_URL is required." >&2
  exit 1
fi

SINCE_DAYS="${SINCE_DAYS:-30}"
PROMPTS_DB="${PROMPTS_DB:-prompts.db}"
OUTPUT_HTML="${OUTPUT_HTML:-git-ai-dashboard.html}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "ERROR: Python interpreter '$PYTHON_BIN' not found." >&2
  exit 1
fi

if ! command -v git-ai >/dev/null 2>&1; then
  echo "git-ai not found; installing..." >&2
  curl -sSL https://usegitai.com/install.sh | bash
  export PATH="$HOME/.git-ai/bin:$PATH"
fi

if ! command -v git-ai >/dev/null 2>&1; then
  echo "ERROR: git-ai is not available after installation." >&2
  exit 1
fi

echo "[git-ai-module] Fetching notes refs (if available)..." >&2
git fetch origin '+refs/notes/*:refs/notes/*' --force || true

echo "[git-ai-module] Installing Python dependency psycopg2-binary..." >&2
"$PYTHON_BIN" -m pip install --quiet psycopg2-binary

echo "[git-ai-module] Collecting prompts for all authors (since ${SINCE_DAYS} days)..." >&2
git-ai prompts --all-authors --since "$SINCE_DAYS"

if command -v sqlite3 >/dev/null 2>&1; then
  row_count="$(sqlite3 "$PROMPTS_DB" "SELECT count(*) FROM prompts;" 2>/dev/null || echo "unknown")"
  echo "[git-ai-module] prompts.db rows: ${row_count}" >&2
fi

echo "[git-ai-module] Exporting prompt sessions to Postgres..." >&2
DATABASE_URL="$DATABASE_URL" PROMPTS_DB="$PROMPTS_DB" \
  "$PYTHON_BIN" "$MODULE_ROOT/scripts/export-prompts-to-db.py"

echo "[git-ai-module] Exporting commit-level attribution to Postgres..." >&2
DATABASE_URL="$DATABASE_URL" SINCE_DAYS="$SINCE_DAYS" \
  "$PYTHON_BIN" "$MODULE_ROOT/scripts/export-commit-stats-to-db.py"

echo "[git-ai-module] Building dashboard from DB..." >&2
DATABASE_URL="$DATABASE_URL" \
  "$PYTHON_BIN" "$MODULE_ROOT/scripts/git-ai-dashboard.py" --db --output "$OUTPUT_HTML"

echo "[git-ai-module] Done. Dashboard written to: $OUTPUT_HTML" >&2
