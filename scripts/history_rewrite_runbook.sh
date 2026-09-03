#!/usr/bin/env bash
# ==============================================================================
# BLUE TIDE OBSERVATORY — HISTORY REWRITE RUNBOOK (Prompt W §08)
# ==============================================================================
# PURPOSE:
#   Purge historical 68MB monolithic bundle.json, cache-busted JSON shards,
#   and parquet blobs from git history under docs/data/ to reduce .git from
#   605MB down to ~10-25MB.
#
# CRITICAL SAFETY:
#   DO NOT RUN THIS SCRIPT INSIDE A SANDBOX OR WITHOUT A TESTED MIRROR BACKUP.
#   RUN NONE OF THIS UNTIL EXPLICITLY APPROVED AND EXECUTED ON HOST.
# ==============================================================================

set -euo pipefail

REPO_ROOT="$(pwd)"
BACKUP_DIR="../Supply-Demand-Flows-backup.git"

echo "=== [STAGE 1] PRE-FLIGHT SAFETY: MIRROR BACKUP ==="
if [ -d "$BACKUP_DIR" ]; then
    echo "ERROR: Backup directory $BACKUP_DIR already exists! Aborting."
    exit 1
fi

echo "Creating full mirror backup to $BACKUP_DIR..."
git clone --mirror "$REPO_ROOT" "$BACKUP_DIR"

echo "Verifying backup integrity..."
git -C "$BACKUP_DIR" fsck --full --strict
PRE_HEAD_SHA="$(git rev-parse HEAD)"
BACKUP_HEAD_SHA="$(git -C "$BACKUP_DIR" rev-parse HEAD)"

if [ "$PRE_HEAD_SHA" != "$BACKUP_HEAD_SHA" ]; then
    echo "ERROR: HEAD SHA mismatch! Repo: $PRE_HEAD_SHA, Backup: $BACKUP_HEAD_SHA"
    exit 1
fi
echo "Backup verified complete and identical at commit $PRE_HEAD_SHA."

echo ""
echo "=== [STAGE 2] WORKTREE TEARDOWN HAZARD MITIGATION ==="
# SPECIFIC HAZARD:
#   Repository contains multiple worktrees under .claude/worktrees/.
#   git filter-repo rewrites commit hashes. Any active worktree pointing to
#   pre-rewrite commit SHAs will resurrect pruned blobs if a branch is checked
#   out, merged, or pushed.
echo "Pruning registered worktrees before rewrite..."
git worktree list
# Remove any temporary worktrees under .claude/worktrees/
# git worktree remove --force .claude/worktrees/<name>
git worktree prune

echo ""
echo "=== [STAGE 3] FILTER-REPO INVOCATION ==="
# REQUIREMENTS:
#   - Use git filter-repo (not filter-branch, not BFG)
#   - Preserve docs/data/manifest.json
#   - Purge docs/data/bundle.json, docs/data/bundle.*.json, docs/data/src.*.json,
#     docs/data/index.*.json, and docs/data/*.parquet across all historical commits.

# Method: Filter using path globs with --invert-paths while keeping manifest.json
# git-filter-repo requires python3 -m pip install git-filter-repo
python -m git_filter_repo \
    --path-glob 'docs/data/bundle*' \
    --path-glob 'docs/data/src.*.json' \
    --path-glob 'docs/data/index.*.json' \
    --path-glob 'docs/data/*.parquet' \
    --invert-paths \
    --force

echo ""
echo "=== [STAGE 4] VERIFICATION & INTEGRITY CHECKS ==="
POST_HEAD_SHA="$(git rev-parse HEAD)"
echo "Pre-filter HEAD:  $PRE_HEAD_SHA"
echo "Post-filter HEAD: $POST_HEAD_SHA"

# 1. Verify docs/data/manifest.json survives at HEAD
if [ ! -f "docs/data/manifest.json" ]; then
    echo "CRITICAL FAILURE: docs/data/manifest.json was lost during rewrite!"
    exit 1
fi
echo "PASSED: docs/data/manifest.json is present."

# 2. Verify no dead-weight files remain under docs/data/
REMAINING_DATA_FILES="$(find docs/data -maxdepth 1 -type f | wc -l)"
echo "Files remaining in docs/data: $REMAINING_DATA_FILES (expecting exactly 1: manifest.json)"
if [ "$REMAINING_DATA_FILES" -ne 1 ]; then
    echo "WARNING: Unexpected files remain in docs/data:"
    find docs/data -maxdepth 1 -type f
fi

# 3. Verify HEAD working tree byte-identical except for purged data files
echo "Verifying working tree parity with pre-filter state..."
# Differences between backup HEAD and current HEAD must be exclusively the purged docs/data paths
git --git-dir="$BACKUP_DIR" diff --name-only "$PRE_HEAD_SHA" "$POST_HEAD_SHA" | while read -r line; do
    case "$line" in
        docs/data/*) ;; # Expected difference
        *) echo "UNEXPECTED FILE MODIFIED: $line"; exit 1 ;;
    esac
done
echo "PASSED: Tree at HEAD is byte-identical except for purged docs/data files."

# 4. Aggressive garbage collection
echo "Running git gc --prune=now --aggressive..."
git reflog expire --expire=now --all
git gc --prune=now --aggressive

echo ""
echo "=== [STAGE 5] SIZE & OBJECT METRICS ==="
git count-objects -v
# Expected .git size afterwards:
# Current .git: 605 MB
# Expected post-rewrite: 10 MB to 25 MB
# Reasoning:
#   Repository code, documentation, and small test fixtures account for ~12-18 MB.
#   All 506 historical heavy objects (68MB bundles and shard parquets) are expunged.
#   Packfiles compress the remaining commit history cleanly into <25 MB.

echo ""
echo "=== [STAGE 6] POST-REWRITE RECOVERY RUNBOOK FOR WORKTREES & CLONES ==="
cat << 'EOF'
FOR COLLABORATORS AND WORKTREES:
1. Do NOT run 'git pull'. Running 'git pull' will create a 3-way merge between
   pre-rewrite and post-rewrite histories and resurrect all 600MB of deleted blobs!
2. In active development clones:
   git fetch origin
   git checkout main
   git reset --hard origin/main
3. For existing worktrees in .claude/worktrees/:
   rm -rf .claude/worktrees/*
   git worktree prune
   Re-create worktrees from fresh rewritten main.
4. Remote push (requires force-push privileges):
   git push origin --force --all
   git push origin --force --tags
EOF
