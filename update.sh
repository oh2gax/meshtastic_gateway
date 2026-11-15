#!/usr/bin/env bash
# update.sh -- update Meshtastic Gateway from GitHub and restart the service.
#
# Usage:  ./update.sh            update if GitHub has a newer version
#         ./update.sh --force    update even if up to date / overwrite local edits
#         ./update.sh --deps     also upgrade Python packages (requirements.txt)
#
# What it does:
#   1. git fetch; stop here if already up to date
#   2. stop the systemd service
#   3. back up the database + local settings to ~/meshtastic_backups/<time>/
#   4. reset the code to origin/main (database, api_keys.py, local_config.py
#      and venv/ are never touched)
#   5. install Python packages if requirements.txt changed (or --deps)
#   6. syntax-check the new code; roll back automatically if it is broken
#   7. start the service and show the latest log lines
#
# The whole script lives inside main() so bash has read all of it before
# git replaces this file during the update.

main() {
  set -euo pipefail

  # SERVICE and STOPPED are global on purpose: the EXIT trap reads them.
  local APP_DIR BRANCH BACKUP_ROOT KEEP_BACKUPS
  APP_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
  SERVICE="meshtastic-gateway"
  BRANCH="main"
  BACKUP_ROOT="${HOME}/meshtastic_backups"
  KEEP_BACKUPS=5
  local DB_FILES=(meshtastic_messages.db meshtastic_messages.db-wal meshtastic_messages.db-shm)
  local LOCAL_FILES=(api_keys.py local_config.py)
  local PY="${APP_DIR}/venv/bin/python3"
  local MAIN_PY="gateway_web_stable.py"

  local FORCE=0 DEPS=0
  for a in "$@"; do
    case "$a" in
      --force) FORCE=1 ;;
      --deps)  DEPS=1 ;;
      -h|--help) sed -n '2,9p' "${BASH_SOURCE[0]}"; return 0 ;;
      *) echo "Unknown option: $a (try --help)"; return 2 ;;
    esac
  done

  say()  { printf '\n\033[1;32m==>\033[0m %s\n' "$*"; }
  warn() { printf '\033[1;33m!!\033[0m %s\n' "$*"; }
  die()  { printf '\033[1;31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }

  cd "$APP_DIR"
  [ -d .git ] || die "$APP_DIR is not a git checkout."
  command -v git >/dev/null || die "git is not installed (sudo apt install git)."
  [ -x "$PY" ] || die "No virtualenv at $APP_DIR/venv (see README: python3 -m venv venv)."
  # Ignore executable-bit differences (files committed from Windows have
  # none; we chmod +x update.sh locally, which must not count as an edit).
  git config core.fileMode false

  # ---- 1. anything new? ------------------------------------------------
  say "Checking GitHub for updates (branch $BRANCH)"
  git fetch --quiet origin "$BRANCH" || die "git fetch failed (network?)."
  local OLD NEW
  OLD="$(git rev-parse HEAD)"
  NEW="$(git rev-parse "origin/$BRANCH")"
  echo "   installed: $(git log -1 --date=short --format='%h %ad %s' "$OLD")"
  echo "   latest:    $(git log -1 --date=short --format='%h %ad %s' "$NEW")"
  if [ "$OLD" = "$NEW" ] && [ "$FORCE" -eq 0 ]; then
    echo "Already up to date."
    return 0
  fi
  if git merge-base --is-ancestor "$OLD" "$NEW" 2>/dev/null && [ "$OLD" != "$NEW" ]; then
    echo "   changes:"
    git --no-pager log --format='     %h %s' "$OLD..$NEW"
  fi

  # ---- local edits to tracked files? (database files don't count) -------
  local CHANGED
  CHANGED="$(git status --porcelain --untracked-files=no \
             | awk '{print $NF}' | grep -v -E '^meshtastic_messages\.db' || true)"
  if [ -n "$CHANGED" ] && [ "$FORCE" -eq 0 ]; then
    warn "These files were edited on this machine and would be overwritten:"
    echo "$CHANGED" | sed 's/^/     /'
    echo "   Put local settings in local_config.py instead (see local_config.py.example)."
    echo "   Run './update.sh --force' to update anyway (your edits are saved as a patch in the backup)."
    return 1
  fi

  # ---- 2. stop service ---------------------------------------------------
  local HAVE_SERVICE=0
  STOPPED=0
  if systemctl list-unit-files "${SERVICE}.service" --no-legend 2>/dev/null | grep -q "^${SERVICE}.service"; then
    HAVE_SERVICE=1
  fi
  if [ "$HAVE_SERVICE" -eq 1 ]; then
    say "Stopping $SERVICE"
    sudo systemctl stop "$SERVICE"
    STOPPED=1
    # If anything below fails, make sure the gateway comes back up.
    trap 'if [ "$STOPPED" -eq 1 ]; then echo; echo "Restarting $SERVICE after error..."; sudo systemctl start "$SERVICE" || true; fi' EXIT
  else
    warn "systemd service '$SERVICE' not installed -- not stopping/starting anything."
    if pgrep -f "python[0-9.]* .*${MAIN_PY}" >/dev/null; then
      die "$MAIN_PY is running outside systemd. Stop it first (pkill -f $MAIN_PY)."
    fi
  fi

  # ---- 3. backup -----------------------------------------------------------
  local BK
  BK="${BACKUP_ROOT}/$(date +%Y%m%d-%H%M%S)"
  say "Backing up to $BK"
  mkdir -p "$BK"
  if [ -f meshtastic_messages.db ]; then
    # Fold the WAL into the main file so the backup is a single consistent DB.
    "$PY" -c "import sqlite3; c=sqlite3.connect('meshtastic_messages.db'); c.execute('PRAGMA wal_checkpoint(TRUNCATE)'); c.close()" \
      || warn "WAL checkpoint failed (continuing; WAL files are backed up too)."
  fi
  local f
  for f in "${DB_FILES[@]}" "${LOCAL_FILES[@]}"; do
    [ -e "$f" ] && cp -p "$f" "$BK/"
  done
  git diff > "$BK/local_changes.patch" || true
  [ -s "$BK/local_changes.patch" ] || rm -f "$BK/local_changes.patch"
  echo "$OLD" > "$BK/previous_commit.txt"
  ls -l "$BK" | tail -n +2 | sed 's/^/     /'

  # ---- 4. update code -------------------------------------------------------
  say "Updating code to $(git rev-parse --short "$NEW")"
  # Stop tracking the database locally (older checkouts still had it in git).
  git rm --cached --quiet --ignore-unmatch "${DB_FILES[@]}" >/dev/null 2>&1 || true
  git reset --hard --quiet "$NEW"
  restore_db() {
    local x
    for x in "${DB_FILES[@]}"; do rm -f "$x"; done
    for x in "${DB_FILES[@]}" "${LOCAL_FILES[@]}"; do
      [ -e "$BK/$x" ] && cp -p "$BK/$x" "$x"
    done
    return 0
  }
  # Always put the live database back exactly as it was before the update.
  restore_db
  chmod +x update.sh 2>/dev/null || true

  # ---- 5. dependencies -----------------------------------------------------
  if [ "$DEPS" -eq 1 ] || ! git diff --quiet "$OLD" "$NEW" -- requirements.txt 2>/dev/null; then
    if [ -f requirements.txt ]; then
      say "Installing Python packages (requirements.txt)"
      "$PY" -m pip install --upgrade -r requirements.txt
    fi
  fi

  # ---- 6. sanity check, roll back if broken --------------------------------
  say "Checking the new code"
  if ! "$PY" -c "import ast,sys; ast.parse(open('$MAIN_PY', encoding='utf-8').read())"; then
    warn "Syntax error in the new $MAIN_PY -- rolling back to $(git rev-parse --short "$OLD")."
    git reset --hard --quiet "$OLD"
    restore_db
    die "Update aborted; previous version restored."
  fi
  echo "   OK"

  # ---- 7. start ---------------------------------------------------------------
  if [ "$HAVE_SERVICE" -eq 1 ]; then
    say "Starting $SERVICE"
    sudo systemctl start "$SERVICE"
    STOPPED=0
    trap - EXIT
    sleep 4
    if systemctl is-active --quiet "$SERVICE"; then
      echo "   $SERVICE is running."
    else
      warn "$SERVICE did not stay up. Recent log:"
    fi
    journalctl -u "$SERVICE" -n 12 --no-pager 2>/dev/null | sed 's/^/     /' || true
  fi

  # ---- prune old backups ----------------------------------------------------
  ls -1dt "${BACKUP_ROOT}"/*/ 2>/dev/null | tail -n +$((KEEP_BACKUPS + 1)) | xargs -r rm -rf

  say "Done: now at $(git log -1 --date=short --format='%h %ad %s')"
}

main "$@"
exit $?
