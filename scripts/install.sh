#!/bin/sh
# Install the published native binary. No compiler or package manager required.
set -eu
version=0.14.0
plugin=false
case "${1-}" in
  --plugin) plugin=true ;;
  '') ;;
  *) printf '%s\n' 'Usage: sh scripts/install.sh [--plugin]' >&2; exit 2 ;;
esac
case "$(uname -s)/$(uname -m)" in
  Darwin/arm64) target=aarch64-apple-darwin ;;
  Darwin/x86_64) target=x86_64-apple-darwin ;;
  Linux/aarch64|Linux/arm64) target=aarch64-unknown-linux-gnu ;;
  Linux/x86_64) target=x86_64-unknown-linux-gnu ;;
  *) printf '%s\n' 'Supported: macOS and Linux, ARM64 or x86-64.' >&2; exit 1 ;;
esac
case "$target" in
  *linux*)
    libc=$(getconf GNU_LIBC_VERSION 2>/dev/null || true)
    printf '%s\n' "$libc" | awk '{split($2,v,"."); exit !($1=="glibc" && (v[1]>2 || (v[1]==2 && v[2]>=35)))}' || {
      printf '%s\n' 'Linux binaries require glibc 2.35 or newer.' >&2; exit 1;
    } ;;
esac
archive="agent-mail-v${version}-${target}.tar.gz"
base="https://github.com/youssef-tharwat/agent-mail/releases/download/v${version}"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
curl --fail --location --proto '=https' --tlsv1.2 --silent --show-error "$base/$archive" -o "$tmp/$archive"
curl --fail --location --proto '=https' --tlsv1.2 --silent --show-error "$base/$archive.sha256" -o "$tmp/$archive.sha256"
(
  cd "$tmp"
  if command -v sha256sum >/dev/null 2>&1; then sha256sum -c "$archive.sha256";
  else shasum -a 256 -c "$archive.sha256"; fi
  tar -xzf "$archive" agent-mail
)
actual=$("$tmp/agent-mail" --version)
test "$actual" = "agent-mail $version" || { printf '%s\n' 'Unexpected binary version' >&2; exit 1; }
dest="${AGENT_MAIL_INSTALL_DIR:-$HOME/.local/bin}"
mkdir -p "$dest"
install -m 755 "$tmp/agent-mail" "$dest/.agent-mail-install-$$"
mv -f "$dest/.agent-mail-install-$$" "$dest/agent-mail"
if "$plugin"; then
  repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
  mkdir -p "$repo/target/release"
  install -m 755 "$tmp/agent-mail" "$repo/target/release/agent-mail"
fi
printf 'Installed %s to %s/agent-mail\n' "$actual" "$dest"
case ":$PATH:" in *":$dest:"*) ;; *) printf 'Add %s to your PATH before configuring runtime hooks.\n' "$dest" ;; esac
