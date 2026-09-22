#!/usr/bin/env bash
# v1 tried apt and got "Unable to locate package" -- the guest has only an IPv6
# ULA and no route off incusbr0, so nothing was installed and the AFTER state
# was the BEFORE state. Test the claim directly instead: boot the image a
# tenant who wanted a normal kernel would install. Debian's "generic" variant
# ships the stock kernel; "genericcloud" ships the cut-down one.
set -uo pipefail
W=/tmp/f39-kernel-probe2; mkdir -p $W
VM=kprobe2
IMG=$W/debian-13-generic-amd64.qcow2
URL=https://cloud.debian.org/images/cloud/trixie/latest/debian-13-generic-amd64.qcow2
[ -f $IMG ] || curl -fsSL -o $IMG "$URL" || { echo "DOWNLOAD FAILED"; exit 1; }
echo "image sha256: $(sha256sum $IMG | cut -d' ' -f1)"
printf 'architecture: x86_64\ncreation_date: %s\nproperties:\n  description: Debian 13 generic (stock kernel)\n  os: debian\n  release: trixie\n' "$(date +%s)" > $W/metadata.yaml
( cd $W && tar -cJf metadata.tar.xz metadata.yaml )
incus image delete f39-generic </dev/null >/dev/null 2>&1
incus image import $W/metadata.tar.xz $IMG --alias f39-generic </dev/null || { echo "IMPORT FAILED"; exit 1; }
KEY=$W/buyer; [ -f $KEY ] || ssh-keygen -t ed25519 -N '' -f $KEY -q
incus delete -f $VM </dev/null >/dev/null 2>&1
incus init f39-generic $VM --vm </dev/null >/dev/null
incus config set $VM user.lnrent.tier=1 </dev/null
incus config set $VM security.guestapi=false </dev/null
incus config set $VM cloud-init.user-data - </dev/null <<YAML
#cloud-config
users:
  - name: buyer
    sudo: ALL=(ALL) NOPASSWD:ALL
    shell: /bin/bash
    ssh_authorized_keys:
      - $(cat $KEY.pub)
YAML
incus config device add $VM cloud-init disk source=cloud-init:config </dev/null >/dev/null
incus start $VM </dev/null
for i in $(seq 1 60); do IP=$(incus network list-leases incusbr0 -f csv 2>/dev/null | awk -F, -v v=$VM '$1==v{print $3}' | head -1); [ -n "$IP" ] && break; sleep 5; done
echo "ip=$IP"
G(){ timeout 60 ssh -n -i $KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o BatchMode=yes -o ConnectTimeout=5 buyer@$IP "$@" 2>&1; }
for i in $(seq 1 40); do [ "$(G hostname)" = "$VM" ] && break; sleep 5; done
echo "HOSTCHECK: $(G hostname)"
echo "HOSTSIDE attachment: $(grep -o 'mount_tag = "[a-z]*"' /run/incus/$VM/qemu.conf 2>/dev/null | tr '\n' ' ')"
echo "GUEST kernel: $(G 'uname -r')"
echo "GUEST 9p in /proc/filesystems: $(G 'grep -c 9p /proc/filesystems || true')"
echo "GUEST modprobe: $(G 'sudo modprobe 9p 9pnet_virtio 2>&1; grep -c 9p /proc/filesystems || true')"
echo "GUEST MOUNT: $(G 'sudo mkdir -p /mnt/a; sudo mount -t 9p -o trans=virtio,version=9p2000.L,ro agent /mnt/a 2>&1 && echo MOUNTED || echo FAILED')"
echo "GUEST CONTENTS: $(G 'ls /mnt/a 2>/dev/null | head -6 | tr "\n" " "')"
echo "GUEST agent binary readable: $(G 'ls -la /mnt/a/incus-agent 2>/dev/null | head -1')"
echo "INCUS EXEC: $(incus exec $VM -- true </dev/null 2>&1 | head -1)"
incus delete -f $VM </dev/null >/dev/null 2>&1
