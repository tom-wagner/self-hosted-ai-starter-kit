#!/usr/bin/env bash
set -euo pipefail

log() { printf '[*] %s\n' "$*"; }

kill_procs() {
  log "Killing Docker processes..."
  pkill -9 dockerd 2>/dev/null || true
  pkill -9 containerd 2>/dev/null || true
  pkill -9 runc 2>/dev/null || true
  pkill -9 Docker 2>/dev/null || true
}

wipe_common() {
  log "Removing user Docker config..."
  rm -rf "$HOME/.docker"
}

wipe_macos() {
  log "Stopping Docker Desktop..."
  if command -v osascript >/dev/null; then
    osascript -e 'quit app "Docker"' || true
  fi
  kill_procs
  log "Removing Docker Desktop state..."
  rm -rf "$HOME/Library/Containers/com.docker.docker"
  rm -rf "$HOME/Library/Group Containers/group.com.docker"
}

wipe_linux() {
  log "Stopping docker services..."
  sudo systemctl stop docker docker.socket containerd 2>/dev/null || true
  kill_procs
  log "Removing engine state (requires sudo)..."
  sudo rm -rf /var/lib/docker
  sudo rm -rf /var/lib/containerd
}

main() {
  log "This will DELETE all Docker data. Ctrl+C now to abort."
  sleep 2

  wipe_common

  case "$(uname -s)" in
    Darwin) wipe_macos ;;
    Linux)  wipe_linux ;;
    *) log "Unsupported OS: $(uname -s)"; exit 1 ;;
  esac

  log "Done. Reboot or restart Docker, then reinstall if needed."
}

main "$@"
