#!/usr/bin/env bash
set -euo pipefail

binary=${1:?worker binary required}
: "${MACOS_CERTIFICATE_P12:?set MACOS_CERTIFICATE_P12}"
: "${MACOS_CERTIFICATE_PASSWORD:?set MACOS_CERTIFICATE_PASSWORD}"
: "${MACOS_NOTARY_KEY_P8:?set MACOS_NOTARY_KEY_P8}"
: "${MACOS_NOTARY_KEY_ID:?set MACOS_NOTARY_KEY_ID}"
: "${MACOS_NOTARY_ISSUER:?set MACOS_NOTARY_ISSUER}"

umask 077
work_dir=$(mktemp -d)
keychain="$work_dir/worker.keychain-db"
cleanup() {
    security delete-keychain "$keychain" >/dev/null 2>&1 || true
    rm -rf "$work_dir"
}
trap cleanup EXIT
password=$(openssl rand -hex 24)

printf '%s' "$MACOS_CERTIFICATE_P12" | base64 -D > "$work_dir/cert.p12"
printf '%s' "$MACOS_NOTARY_KEY_P8" | base64 -D > "$work_dir/notary.p8"
security create-keychain -p "$password" "$keychain"
security unlock-keychain -p "$password" "$keychain"
security import "$work_dir/cert.p12" -k "$keychain" -P "$MACOS_CERTIFICATE_PASSWORD" -T /usr/bin/codesign
security set-key-partition-list -S apple-tool:,apple:,codesign: -s -k "$password" "$keychain" >/dev/null
identity=$(security find-identity -v -p codesigning "$keychain" |
    awk '/Developer ID Application:/ { print $2; exit }')
test -n "$identity"

codesign --force --options runtime --timestamp --keychain "$keychain" --sign "$identity" "$binary"
codesign --verify --strict "$binary"
ditto -c -k --keepParent "$binary" "$work_dir/prime-worker.zip"
xcrun notarytool submit "$work_dir/prime-worker.zip" +    --key "$work_dir/notary.p8" --key-id "$MACOS_NOTARY_KEY_ID" --issuer "$MACOS_NOTARY_ISSUER" +    --wait --timeout 30m --output-format json > "$work_dir/result.json"
jq -e '.status == "Accepted"' "$work_dir/result.json" >/dev/null
