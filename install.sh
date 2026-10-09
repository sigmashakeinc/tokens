#!/bin/sh
# install.sh - install the `tokens` binary on Linux or macOS.
#
#   curl -fsSL https://raw.githubusercontent.com/sigmashakeinc/tokens/main/install.sh | sh
#   curl -fsSL https://raw.githubusercontent.com/sigmashakeinc/tokens/main/install.sh | sh -s -- --version 1.2.3
#
# The script finds your operating system and CPU, downloads the matching
# release archive from GitHub, checks its SHA-256 against checksums.txt from
# the same release, and installs the binary. If the check fails or the
# checksum entry is missing, nothing is installed.
#
# Options:
#   --version X.Y.Z      Install this release (default: the latest release).
#   --install-dir DIR    Install into DIR (default: $HOME/.local/bin).
#   --from-file FILE     Install from a local archive. Needs a checksums.txt in
#                        the same directory, or --checksums FILE. Used by CI.
#   --checksums FILE     checksums.txt to verify --from-file against.
#   -h, --help           Show this help.
#
# Environment (the options above take priority):
#   VERSION              Same as --version.
#   INSTALL_DIR          Same as --install-dir.
#   TOKENS_REPO          GitHub repository (default: sigmashakeinc/tokens).
#   TOKENS_RELEASE_BASE  Base URL of the releases (default:
#                        https://github.com/$TOKENS_REPO/releases). For tests.

set -eu

REPO="${TOKENS_REPO:-sigmashakeinc/tokens}"
BASE="${TOKENS_RELEASE_BASE:-https://github.com/$REPO/releases}"
VERSION="${VERSION:-}"
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"
FROM_FILE=""
CHECKSUMS_FILE=""
WORK=""

say() { printf '%s\n' "$*"; }
die() {
  printf 'install.sh: error: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'USAGE'
Usage: install.sh [options]

Installs the tokens binary from a GitHub release. The SHA-256 of the archive
is checked against checksums.txt; any failure stops the install.

Options:
  --version X.Y.Z      Install this release (default: the latest release).
  --install-dir DIR    Install into DIR (default: $HOME/.local/bin).
  --from-file FILE     Install from a local archive. Needs a checksums.txt in
                       the same directory, or --checksums FILE.
  --checksums FILE     checksums.txt to verify --from-file against.
  -h, --help           Show this help.

Environment: VERSION, INSTALL_DIR, TOKENS_REPO, TOKENS_RELEASE_BASE.
USAGE
}

# Remove only the files this script created, then the empty directories.
cleanup() {
  if [ -n "$WORK" ] && [ -d "$WORK" ]; then
    for f in "$WORK"/* "$WORK"/unpack/*; do
      if [ -f "$f" ]; then rm -f "$f"; fi
    done
    rmdir "$WORK/unpack" 2>/dev/null || true
    rmdir "$WORK" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM HUP

while [ $# -gt 0 ]; do
  case "$1" in
    --version)
      [ $# -ge 2 ] || die "--version needs a value"
      VERSION="$2"
      shift 2
      ;;
    --install-dir)
      [ $# -ge 2 ] || die "--install-dir needs a value"
      INSTALL_DIR="$2"
      shift 2
      ;;
    --from-file)
      [ $# -ge 2 ] || die "--from-file needs a value"
      FROM_FILE="$2"
      shift 2
      ;;
    --checksums)
      [ $# -ge 2 ] || die "--checksums needs a value"
      CHECKSUMS_FILE="$2"
      shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      die "unknown option: $1 (try --help)"
      ;;
  esac
done

VERSION="${VERSION#v}"

# Operating system and CPU.
case "$(uname -s)" in
  Linux) OS=linux ;;
  Darwin) OS=darwin ;;
  *) die "unsupported operating system: $(uname -s). On Windows, use install.ps1." ;;
esac
case "$(uname -m)" in
  x86_64 | amd64) ARCH=x64 ;;
  aarch64 | arm64) ARCH=arm64 ;;
  *) die "unsupported CPU: $(uname -m)" ;;
esac

# Tools.
download() {
  # download URL DEST
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL --retry 3 -o "$2" "$1" || die "download failed: $1"
  elif command -v wget >/dev/null 2>&1; then
    wget -q -O "$2" "$1" || die "download failed: $1"
  else
    die "curl or wget is required"
  fi
}

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  elif command -v openssl >/dev/null 2>&1; then
    openssl dgst -sha256 "$1" | awk '{print $NF}'
  else
    die "sha256sum, shasum, or openssl is required to verify the download"
  fi
}

command -v tar >/dev/null 2>&1 || die "tar is required"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/tokens-install.XXXXXX")" || die "cannot create a temporary directory"

if [ -n "$FROM_FILE" ]; then
  [ -f "$FROM_FILE" ] || die "file not found: $FROM_FILE"
  ARCHIVE_NAME="$(basename "$FROM_FILE")"
  ARCHIVE="$FROM_FILE"
  if [ -z "$CHECKSUMS_FILE" ]; then
    CHECKSUMS_FILE="$(dirname "$FROM_FILE")/checksums.txt"
  fi
  [ -f "$CHECKSUMS_FILE" ] || die "no checksums file at $CHECKSUMS_FILE (use --checksums)"
  CHECKSUMS="$CHECKSUMS_FILE"
else
  if [ -n "$VERSION" ]; then
    RELEASE_URL="$BASE/download/v$VERSION"
  else
    RELEASE_URL="$BASE/latest/download"
  fi
  CHECKSUMS="$WORK/checksums.txt"
  say "Downloading checksums from $RELEASE_URL"
  download "$RELEASE_URL/checksums.txt" "$CHECKSUMS"
  # Pick this platform's archive from the checksum list. The list is the
  # trusted index: it names the archive and its hash.
  ARCHIVE_NAME="$(awk -v suffix="_${OS}_${ARCH}.tar.gz" '
    $2 ~ /^tokens_/ && substr($2, length($2) - length(suffix) + 1) == suffix { print $2; exit }
  ' "$CHECKSUMS")"
  [ -n "$ARCHIVE_NAME" ] || die "this release has no archive for ${OS}/${ARCH}"
  ARCHIVE="$WORK/$ARCHIVE_NAME"
  say "Downloading $ARCHIVE_NAME"
  download "$RELEASE_URL/$ARCHIVE_NAME" "$ARCHIVE"
fi

# Verify. Fail closed: a missing entry, a malformed entry, or a mismatch stops the install.
EXPECTED="$(awk -v name="$ARCHIVE_NAME" '$2 == name { print $1; exit }' "$CHECKSUMS")"
[ -n "$EXPECTED" ] || die "checksums.txt has no entry for $ARCHIVE_NAME"
case "$EXPECTED" in
  *[!0-9a-fA-F]*) die "checksums.txt entry for $ARCHIVE_NAME is not a SHA-256 hash" ;;
esac
[ "${#EXPECTED}" -eq 64 ] || die "checksums.txt entry for $ARCHIVE_NAME is not a SHA-256 hash"
ACTUAL="$(sha256_of "$ARCHIVE")"
EXPECTED_LOWER="$(printf '%s' "$EXPECTED" | tr 'A-F' 'a-f')"
ACTUAL_LOWER="$(printf '%s' "$ACTUAL" | tr 'A-F' 'a-f')"
if [ "$EXPECTED_LOWER" != "$ACTUAL_LOWER" ]; then
  die "SHA-256 mismatch for $ARCHIVE_NAME (expected $EXPECTED_LOWER, got $ACTUAL_LOWER). Nothing was installed."
fi
say "SHA-256 verified: $ACTUAL_LOWER"

# Unpack and install.
mkdir -p "$WORK/unpack"
tar -xzf "$ARCHIVE" -C "$WORK/unpack" || die "cannot unpack $ARCHIVE_NAME"
[ -f "$WORK/unpack/tokens" ] || die "the archive has no tokens binary"

mkdir -p "$INSTALL_DIR" || die "cannot create $INSTALL_DIR"
# Copy beside the target, then rename, so a running tokens process is not overwritten in place.
cp "$WORK/unpack/tokens" "$INSTALL_DIR/.tokens.new.$$" || die "cannot write to $INSTALL_DIR"
chmod 755 "$INSTALL_DIR/.tokens.new.$$"
mv -f "$INSTALL_DIR/.tokens.new.$$" "$INSTALL_DIR/tokens" || die "cannot install to $INSTALL_DIR/tokens"

# Apple Silicon refuses to run an unsigned binary. Sign it ad hoc if it is not signed.
if [ "$OS" = "darwin" ] && command -v codesign >/dev/null 2>&1; then
  if ! codesign --verify "$INSTALL_DIR/tokens" >/dev/null 2>&1; then
    codesign --force --sign - "$INSTALL_DIR/tokens" >/dev/null 2>&1 || true
  fi
fi

say "Installed: $INSTALL_DIR/tokens"
"$INSTALL_DIR/tokens" --version || die "the installed binary does not run"

case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *)
    say ""
    say "$INSTALL_DIR is not on your PATH. Add this line to your shell profile:"
    say "  export PATH=\"$INSTALL_DIR:\$PATH\""
    ;;
esac
say ""
say "Run the service:  tokens serve"
say "Dashboard:        http://127.0.0.1:7913"
