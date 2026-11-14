#!/bin/sh
set -eu

# Provision only this application's named volume. Never print credentials.
entry=${RPCHUB_GATEWAY_HTPASSWD:?Missing gateway password-hash entry}
case "$entry" in
  *'
'*) echo 'The gateway password-hash entry must be one line.' >&2; exit 1 ;;
esac
if ! printf '%s\n' "$entry" | grep -Eq '^[A-Za-z0-9_-]{1,64}:\$6\$[A-Za-z0-9./]{1,16}\$[A-Za-z0-9./]{86}$'; then
  echo 'Invalid gateway username or SHA-512 password hash.' >&2
  exit 1
fi

# The pinned gateway image runs its workers as UID/GID 101. Reclaim the
# directory on each run, then replace the file atomically without DAC bypass.
chown 0:0 /auth
chmod 0700 /auth
pending=$(mktemp /auth/.users.XXXXXX)
trap 'rm -f "$pending"' EXIT HUP INT TERM
printf '%s\n' "$entry" > "$pending"
chmod 0644 "$pending"
chown 101:101 "$pending"
mv -f "$pending" /auth/users.htpasswd
test "$(stat -c '%u:%g:%a' /auth/users.htpasswd)" = '101:101:644'
chown 101:101 /auth
test "$(stat -c '%u:%g:%a' /auth)" = '101:101:700'
trap - EXIT HUP INT TERM
