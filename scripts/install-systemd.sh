#!/usr/bin/env bash
# Install + enable the sigmashake-tokens user systemd unit.
# Usage: bash scripts/install-systemd.sh [--start]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
UNIT_SRC="${PROJECT_DIR}/systemd/sigmashake-tokens.service"
SLICE_SRC="${PROJECT_DIR}/systemd/sigmashake-tokens.slice"
UNIT_DIR="${HOME}/.config/systemd/user"
UNIT_DST="${UNIT_DIR}/sigmashake-tokens.service"
SLICE_DST="${UNIT_DIR}/sigmashake-tokens.slice"
SERVICE_UNIT="sigmashake-tokens.service"
SLICE_UNIT="sigmashake-tokens.slice"
SERVICE_MEMORY_MAX="128M"
SLICE_MEMORY_MAX="512M"

resolve_native_bun() {
  local candidate resolved magic
  local -a candidates=()

  if [[ -n "${BUN_INSTALL:-}" ]]; then
    candidates+=("${BUN_INSTALL}/bin/bun")
  fi
  candidates+=("${HOME}/.bun/bin/bun")
  while IFS= read -r candidate; do
    candidates+=("${candidate}")
  done < <(type -aP bun 2>/dev/null || true)

  for candidate in "${candidates[@]}"; do
    [[ -x "${candidate}" && ! -d "${candidate}" ]] || continue
    resolved="$(readlink -f -- "${candidate}" 2>/dev/null || printf '%s' "${candidate}")"
    # A ~/.local/bin/bun wrapper can be a shell script that moves its child
    # into another cgroup. A systemd service must exec the native Bun ELF
    # directly so this unit's CPUQuota/MemoryMax remain authoritative.
    magic="$(od -An -N4 -tx1 "${resolved}" 2>/dev/null | tr -d '[:space:]')"
    if [[ "${magic}" == "7f454c46" ]]; then
      printf '%s\n' "${resolved}"
      return 0
    fi
  done

  return 1
}

BUN_BIN="$(resolve_native_bun || true)"
ENV_BIN="$(command -v env 2>/dev/null || true)"

if [[ -z "${BUN_BIN}" ]]; then
  echo "error: native Bun ELF not found (shell wrappers cannot own this systemd unit)" >&2
  exit 1
fi
if [[ -z "${ENV_BIN}" || "${ENV_BIN}" != /* || ! -x "${ENV_BIN}" ]]; then
  echo "error: an absolute executable env binary is required to launch native Bun safely" >&2
  exit 1
fi

systemd_quote() {
  local value="$1" char escaped=""
  local i
  local LC_ALL=C

  # Unit values are line-oriented. Refuse control characters instead of
  # allowing a path to add a directive or split ExecStart into another line.
  if [[ "${value}" =~ [[:cntrl:]] ]]; then
    echo "error: paths containing control characters cannot be rendered into a systemd unit" >&2
    return 1
  fi

  # systemd parses C-style quotes and %-specifiers in these fields. ExecStart
  # uses the ':' prefix below to suppress $ environment expansion, so quote
  # every value and only double percent specifiers here.
  for ((i = 0; i < ${#value}; i++)); do
    char="${value:i:1}"
    case "${char}" in
      \\) escaped+='\\' ;;
      '"') escaped+='\"' ;;
      '%') escaped+='%%' ;;
      '$') escaped+='$' ;;
      *) escaped+="${char}" ;;
    esac
  done
  printf '"%s"' "${escaped}"
}

systemd_working_directory() {
  local value="$1" char escaped=""
  local i
  local LC_ALL=C

  # WorkingDirectory= consumes one path value, not an ExecStart-style argv
  # token. Quoting the whole value leaves the quote as part of the path on
  # systemd 255 (and makes an otherwise absolute path fail to load). Emit an
  # unquoted absolute value and C-escape characters that could be parsed as
  # syntax. Percent must remain doubled because systemd expands specifiers in
  # WorkingDirectory= after parsing.
  if [[ "${value}" =~ [[:cntrl:]] ]]; then
    echo "error: paths containing control characters cannot be rendered into a systemd unit" >&2
    return 1
  fi
  if [[ "${value}" != /* ]]; then
    echo "error: WorkingDirectory must be an absolute path" >&2
    return 1
  fi

  for ((i = 0; i < ${#value}; i++)); do
    char="${value:i:1}"
    case "${char}" in
      ' ') escaped+='\x20' ;;
      \\) escaped+='\x5c' ;;
      '"') escaped+='\x22' ;;
      "'") escaped+='\x27' ;;
      '%') escaped+='%%' ;;
      *) escaped+="${char}" ;;
    esac
  done
  printf '%s' "${escaped}"
}

render_unit() {
  local line
  local workdir_count=0
  local exec_count=0

  while IFS= read -r line || [[ -n "${line}" ]]; do
    case "${line}" in
      'WorkingDirectory=__WORKDIR__')
        printf 'WorkingDirectory=%s\n' "${WORKDIR_UNIT}"
        ((workdir_count += 1))
        ;;
      'ExecStart=:__ENV_BIN__ -- __BUN_BIN__ __WORKDIR__/src/server.ts')
        printf 'ExecStart=:%s -- %s %s\n' "${ENV_UNIT}" "${BUN_UNIT}" "${SERVER_UNIT}"
        ((exec_count += 1))
        ;;
      *) printf '%s\n' "${line}" ;;
    esac
  done < "${UNIT_SRC}"

  if ((workdir_count != 1 || exec_count != 1)); then
    echo "error: service template must contain exactly one WorkingDirectory and ExecStart placeholder" >&2
    return 1
  fi
}

render_slice() {
  local line
  if [[ -f "${SLICE_SRC}" ]]; then
    while IFS= read -r line || [[ -n "${line}" ]]; do
      printf '%s\n' "${line}"
    done < "${SLICE_SRC}"
    return 0
  fi

  # Keep the installer safe when invoked from a minimal distribution fixture
  # that predates the standalone template. The fallback is deliberately the
  # same restrictive envelope, never an uncapped implicit slice.
  printf '%s\n' \
    '[Unit]' \
    'Description=SigmaShake Tokens aggregate resource envelope' \
    '' \
    '[Slice]' \
    'MemoryMax=512M' \
    'CPUQuota=50%' \
    '' \
    '[Install]' \
    'WantedBy=default.target'
}

WORKDIR_UNIT="$(systemd_working_directory "${PROJECT_DIR}")"
BUN_UNIT="$(systemd_quote "${BUN_BIN}")"
ENV_UNIT="$(systemd_quote "${ENV_BIN}")"
SERVER_UNIT="$(systemd_quote "${PROJECT_DIR}/src/server.ts")"

mkdir -p "${UNIT_DIR}"
UNIT_TMP="$(mktemp "${UNIT_DIR}/.sigmashake-tokens.service.XXXXXX")"
SLICE_TMP=""
cleanup() {
  rm -f -- "${UNIT_TMP}"
  if [[ -n "${SLICE_TMP}" ]]; then rm -f -- "${SLICE_TMP}"; fi
}
trap cleanup EXIT
SLICE_TMP="$(mktemp "${UNIT_DIR}/.sigmashake-tokens.slice.XXXXXX")"

render_unit > "${UNIT_TMP}"
render_slice > "${SLICE_TMP}"
chmod 644 "${UNIT_TMP}"
chmod 644 "${SLICE_TMP}"
mv -f -- "${SLICE_TMP}" "${SLICE_DST}"
mv -f -- "${UNIT_TMP}" "${UNIT_DST}"
trap - EXIT

systemctl --user daemon-reload

# Runtime resource properties created by `systemctl set-property --runtime`
# live below the user manager's control directory and override a newly rendered
# unit. Never lower them under a live parent or orphan worker. Once the exact
# cgroup is empty, overwrite both properties through systemctl so this stays
# path-independent across systemd versions and user-manager runtime dirs.
SERVICE_STATE=""
SERVICE_STATE_STATUS=0
if SERVICE_STATE="$(systemctl --user is-active "${SERVICE_UNIT}")"; then
  SERVICE_STATE_STATUS=0
else
  SERVICE_STATE_STATUS=$?
fi
if [[ "${SERVICE_STATE_STATUS}" != "3" || "${SERVICE_STATE}" != "inactive" ]]; then
  echo "error: ${SERVICE_UNIT} must be proven inactive before installing resource caps (state=${SERVICE_STATE:-unknown}, status=${SERVICE_STATE_STATUS})" >&2
  exit 1
fi
ACTIVE_WORKERS="$(
  systemctl --user list-units \
    --type=service \
    --state=active \
    --plain \
    --no-legend \
    'sigmashake-tokens-ingest-*.service' \
    'sigmashake-tokens-usage-*.service'
)"
if [[ -n "${ACTIVE_WORKERS//[[:space:]]/}" ]]; then
  echo "error: Tokens transient workers remain active; resource caps were not changed" >&2
  exit 1
fi
SLICE_TASKS="$(systemctl --user show "${SLICE_UNIT}" --property=TasksCurrent --value)"
if [[ ! "${SLICE_TASKS}" =~ ^[0-9]+$ || "${SLICE_TASKS}" != "0" ]]; then
  echo "error: ${SLICE_UNIT} is not empty (TasksCurrent=${SLICE_TASKS:-unknown})" >&2
  exit 1
fi

systemctl --user set-property --runtime "${SERVICE_UNIT}" "MemoryMax=${SERVICE_MEMORY_MAX}"
systemctl --user set-property --runtime "${SLICE_UNIT}" "MemoryMax=${SLICE_MEMORY_MAX}"
systemctl --user daemon-reload
systemctl --user enable "${SLICE_UNIT}" "${SERVICE_UNIT}"

if [[ "${1:-}" == "--start" ]]; then
  systemctl --user start "${SERVICE_UNIT}"
  sleep 1
  systemctl --user --no-pager status "${SERVICE_UNIT}" | head -20
fi

echo
echo "installed: ${UNIT_DST}"
echo "installed: ${SLICE_DST}"
echo "to start:   systemctl --user start sigmashake-tokens.service"
echo "to logs:    journalctl --user -u sigmashake-tokens.service -f"
echo "dashboard:  http://127.0.0.1:7913"
