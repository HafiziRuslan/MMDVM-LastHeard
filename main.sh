#!/bin/bash
set -e

# --- Initial Setup ---
if [ "$EUID" -ne 0 ]; then
  echo "Please run as root" >&2
  exit 1
fi

# Ensure we are in the script directory
cd "$(dirname "$0")"

# --- Configuration and Constants ---
LOG_FILE="/var/log/mmdvmlhbot.log"
REQUIRED_SPACE_MB=100
RESTART_DELAY=5
MAX_DELAY=300
MAX_RETRIES=10
INTERNET_AVAILABLE=false
dir_own=$(stat -c '%U' .)

# --- Logging Setup ---
# Cleanup and create log directory before redirecting output
# rm -rf /var/tmp/mmdvmlhbot
rm -rf /var/log/mmdvmlhbot
mkdir -p /var/log/mmdvmlhbot
chown -hR "$dir_own:$dir_own" /var/log/mmdvmlhbot

# Save original stdout to fd 3 for display updates
exec 3>&1
# Redirect stdout and stderr to the log file
exec >> "$LOG_FILE" 2>&1

# --- Function Definitions ---

#
# Group 1: Core Utilities
#
log_msg() {
  local level=$1
  local calling_func=${FUNCNAME[1]:-main}
  local line_no=${BASH_LINENO[0]}
  shift
  local timestamp
  timestamp=$(date +'%Y-%m-%dT%H:%M:%S.%3N%:z')
  # Format from src/main.py: %(asctime)s | %(levelname)-8s | %(threadName)-12s | %(name)s.%(funcName)s:%(lineno)d | %(message)s
  printf "%s | %-8s | main.sh      | main.sh.%s:%s | %s\n" "$timestamp" "$level" "$calling_func" "$line_no" "$*"
}

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

#
# Group 2: Pre-flight Checks
#
check_internet() {
  INTERNET_AVAILABLE=false
  local hosts=("1.1.1.1" "8.8.8.8" "github.com" "pypi.org")
  for host in "${hosts[@]}"; do
    if timeout 5 ping -q -c 1 -W 1 "$host" >/dev/null 2>&1; then
      INTERNET_AVAILABLE=true
      return
    fi
  done
  log_msg WARN "Internet check failed. Could not reach any of: $(IFS=', '; echo "${hosts[*]}")"
}

check_disk_space() {
  local available_space_mb
  # Get the last line of df output to be robust against outputs with or without a header.
  available_space_mb=$(df -mP . | tail -n 1 | awk '{print $4}')

  # Validate that we received a numeric value before comparison.
  if ! [[ "$available_space_mb" =~ ^[0-9]+$ ]]; then
    log_msg WARN "⚠️ Could not determine available disk space."
    return 1
  fi

  if [ "$available_space_mb" -lt "$REQUIRED_SPACE_MB" ]; then
    log_msg WARN "⚠️ Insufficient disk space for update. Required: ${REQUIRED_SPACE_MB}MB, Available: ${available_space_mb}MB."
    return 1
  fi
  return 0
}

#
# Group 3: Setup and Maintenance
#
ensure_system_packages() {
  local packages=("$@")
  local missing_packages=()
  for pkg in "${packages[@]}"; do
    if ! dpkg -s "$pkg" >/dev/null 2>&1; then
      missing_packages+=("$pkg")
    fi
  done

  if [ ${#missing_packages[@]} -eq 0 ]; then
    log_msg INFO "✅ Packages are installed: ${packages[*]}."
  else
    log_msg WARN "❌ Missing packages: ${missing_packages[*]}. -> Installing missing packages"
    if [ "$INTERNET_AVAILABLE" = true ]; then
      apt-get update -q && apt-get install -y -q "${missing_packages[@]}"
    else
      log_msg ERROR "Cannot install missing packages without internet connection."
      exit 1
    fi
  fi
}

ensure_uv_installed() {
  if command_exists uv; then
    log_msg INFO "✅ uv is installed."
  else
    if [ "$INTERNET_AVAILABLE" = true ]; then
      log_msg WARN "❌ uv is NOT installed. -> Installing uv"
      curl -LsSf https://astral.sh/uv/install.sh | sh
    else
      log_msg ERROR "❌ uv is NOT installed and cannot be installed without internet."
      exit 1
    fi
  fi
}

update_repository() {
  if ! check_disk_space; then
    log_msg WARN "Skipping repository update due to insufficient disk space."
    return
  fi

  log_msg INFO "Checking for updates..."
  local fetch_success=false
  for i in {1..3}; do
    if sudo -u "$dir_own" git fetch -q; then
      fetch_success=true
      break
    fi
    log_msg WARN "Git fetch failed (attempt $i/3). Retrying in 5 seconds..."
    sleep 5
  done

  if [ "$fetch_success" = false ]; then
    log_msg WARN "⚠️ Failed to fetch updates after multiple attempts. Skipping update check."
    return
  fi

  local LOCAL
  LOCAL=$(sudo -u "$dir_own" git rev-parse HEAD)
  local REMOTE
  REMOTE=$(sudo -u "$dir_own" git rev-parse @{u})

    if [ "$LOCAL" != "$REMOTE" ]; then
      log_msg INFO "Updating MMDVM_LastHeard repository"
      local UPDATE_SUCCESS=false
      if sudo -u "$dir_own" timeout 60 git pull --autostash -q; then
        UPDATE_SUCCESS=true
      else
        log_msg WARN "Git pull failed. Attempting to resolve conflicts by resetting to remote..."
        if sudo -u "$dir_own" git reset --hard @{u}; then
          UPDATE_SUCCESS=true
          log_msg INFO "Reset to remote successful."
        fi
      fi

      if [ "$UPDATE_SUCCESS" = true ] && [ "$(sudo -u "$dir_own" git rev-parse HEAD)" = "$REMOTE" ]; then
        if ! sudo -u "$dir_own" git diff --quiet "$LOCAL" HEAD -- pyproject.toml; then
          log_msg INFO "Application updated. Forcing environment recreation."
          sudo -u "$dir_own" uv venv --clear
        fi

        log_msg INFO "Verifying repository integrity..."
        if sudo -u "$dir_own" git fsck --full >/dev/null 2>&1; then
          log_msg INFO "Update applied and verified. Restarting script..."
          exec "$0" "$@"
        else
          log_msg ERROR "Repository integrity check failed! Skipping restart."
        fi
      else
        log_msg WARN "Update failed or HEAD does not match remote. Skipping restart."
      fi
    else
      log_msg INFO "Repository is up to date."
    fi
}

sync_dependencies() {
  local action=$1
  if [ "$INTERNET_AVAILABLE" = true ]; then
    log_msg INFO "$action MMDVM_LastHeard dependencies"
    sudo -u "$dir_own" uv tool run pyclean . -d -q
    sudo -u "$dir_own" uv sync -q
  elif [ "$action" = "Installing" ]; then
    log_msg WARN "Internet unavailable. Skipping dependency installation."
  fi
}

manage_venv() {
  if [ -d ".venv" ]; then
    if ! sudo -u "$dir_own" ./.venv/bin/python3 -c "import sys" >/dev/null 2>&1; then
      log_msg WARN "⚠️ Virtual environment appears corrupted. Removing it..."
      sudo -u "$dir_own" uv venv --clear
    fi
  fi

  if [ ! -d ".venv" ]; then
    log_msg INFO "MMDVM_LastHeard environment not found, creating one."
    sudo -u "$dir_own" uv venv
    log_msg INFO "Activating MMDVM_LastHeard environment"
    sync_dependencies "Installing"
  else
    log_msg INFO "MMDVM_LastHeard environment exists. -> Activating MMDVM_LastHeard environment"
    sync_dependencies "Updating"
  fi
}

#
# Group 4: Application Execution
#
run_application() {
  log_msg INFO "Running MMDVM_LastHeard"
  local retry_count=0
  local restart_delay=$RESTART_DELAY

  while true; do
    if [ ! -f .env ]; then
      log_msg ERROR "❌ .env file not found! Cannot start MMDVM_LastHeard. Exiting."
      exit 1
    fi

    local START_TIME
    START_TIME=$(date +%s)
    set +e
    sudo -u "$dir_own" uv run -s ./src/main.py
    local exit_code=$?
    set -e
    local END_TIME
    END_TIME=$(date +%s)

    if [ $((END_TIME - START_TIME)) -gt 60 ]; then
      restart_delay=$RESTART_DELAY
      retry_count=0
    fi

    # Restart on any error code except 0 (success), 130 (SIGINT), 143 (SIGTERM)
    if [ "$exit_code" -ne 0 ] && [ "$exit_code" -ne 130 ] && [ "$exit_code" -ne 143 ]; then
      retry_count=$((retry_count + 1))
      if [ "$retry_count" -gt "$MAX_RETRIES" ]; then
        log_msg ERROR "Maximum retries ($MAX_RETRIES) reached. Exiting."
        exit 1
      fi

      log_msg ERROR "MMDVM_LastHeard exited with code $exit_code. Retry $retry_count/$MAX_RETRIES. Re-run in ${restart_delay} seconds."
      sleep "$restart_delay"

      restart_delay=$((restart_delay * 2))
      if [ "$restart_delay" -gt "$MAX_DELAY" ]; then
        restart_delay=$MAX_DELAY
      fi
    elif [ "$exit_code" -eq 0 ]; then
      log_msg INFO "MMDVM_LastHeard exited normally. Stopping."
      break
    else
      log_msg ERROR "MMDVM_LastHeard exited with unrecoverable code $exit_code. Stopping."
      exit "$exit_code"
    fi
  done
}

# --- Main Execution ---
main() {
  check_internet
  if [ "$INTERNET_AVAILABLE" = false ]; then
    log_msg WARN "⚠️ No internet connection detected. Skipping updates and online operations."
  fi

  ensure_system_packages gcc git python3-dev curl
  ensure_uv_installed

  if [ "$INTERNET_AVAILABLE" = true ]; then
    update_repository
  fi

  manage_venv

  if [ ! -f .env ]; then
    log_msg ERROR "❌ .env file not found! Please copy .env.sample to .env and configure it."
    exit 1
  fi

  run_application
}

main "$@"
