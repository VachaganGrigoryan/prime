#!/usr/bin/env bash
set -euo pipefail
[[ $(id -u) == 0 ]] || { echo 'Run this script as root on Atlas.' >&2; exit 1; }
[[ $(hostname -s) == atlas ]] || { echo 'Atlas host required.' >&2; exit 1; }
[[ $(findmnt -n -M /NAS -o FSTYPE) == btrfs ]] || { echo 'The Btrfs NAS must be mounted.' >&2; exit 1; }

archive=/NAS/dev/prime-finder
mountpoint="$archive/storage"
image="$archive/archive.img"
identity=prime-finder-atlas-v1
bytes=100000000000
unit=$(systemd-escape --path --suffix=mount "$mountpoint")
install -d /NAS/dev
if [[ ! -e "$archive" ]]; then
    btrfs subvolume create "$archive"
fi
btrfs subvolume show "$archive" >/dev/null
install -d -m 0700 "$mountpoint"
if [[ ! -e "$image" ]]; then
    available=$(df -B1 --output=avail /NAS | tail -n1 | tr -d ' ')
    (( available >= bytes )) || { echo 'NAS has less than 100 GB available.' >&2; exit 1; }
    (umask 077; set -o noclobber; : > "$image")
    truncate -s "$bytes" "$image"
    mkfs.ext4 -q -m 0 -L prime-finder "$image"
fi
[[ -f "$image" && ! -L "$image" && $(stat -c %s "$image") == "$bytes" ]] || {
    echo 'Archive image type or size mismatch.' >&2; exit 1;
}
if [[ $(blkid -s TYPE -o value "$image") != ext4 || $(blkid -s LABEL -o value "$image") != prime-finder ]]; then
    echo 'Archive image filesystem mismatch.' >&2
    exit 1
fi

cat > "/etc/systemd/system/$unit" <<UNIT
[Unit]
Description=Prime Finder archive on Atlas NAS
RequiresMountsFor=/NAS
ConditionPathIsMountPoint=/NAS

[Mount]
What=$image
Where=$mountpoint
Type=ext4
Options=loop,nodev,nosuid,noexec

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now "$unit"
[[ $(findmnt -n -M "$mountpoint" -o FSTYPE) == ext4 ]]
device=$(findmnt -n -M "$mountpoint" -o SOURCE)
[[ $(losetup --noheadings --output BACK-FILE "$device" | xargs) == "$image" ]]
if [[ -e "$mountpoint/.prime-archive-id" ]]; then
    [[ $(cat "$mountpoint/.prime-archive-id") == "$identity" ]]
else
    printf '%s\n' "$identity" > "$mountpoint/.prime-archive-id"
fi
chmod 0644 "$mountpoint/.prime-archive-id"
chown 10001:10001 "$mountpoint"
chmod 0700 "$mountpoint"
echo 'Prime Finder archive mounted.'
