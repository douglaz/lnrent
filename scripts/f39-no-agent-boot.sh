#!/usr/bin/env bash
# F39 acceptance run: a stock curated cloud image boots from a NoCloud seed,
# applies the Buyer key then the Seed fragment, serves a Readiness probe on its
# own address, and yields its SSH host keys over the network -- with NO
# hypervisor guest agent present.
#
# Run under: nix shell nixpkgs#qemu nixpkgs#cloud-utils nixpkgs#openssh nixpkgs#curl
#
# Guards, because the first version of this script passed two checks by reading
# an unrelated qemu already listening on the port it assumed was free:
#   * ports are chosen free at runtime, never hardcoded;
#   * qemu's own stderr is checked for a failed forwarding rule;
#   * the qemu child must still be alive;
#   * the machine answering must identify itself as THIS run's guest.
set -uo pipefail

# Artifacts (a ~330 MB image, an overlay, logs) live in a work dir, never in the repo.
HERE="${F39_WORKDIR:-${TMPDIR:-/tmp}/f39-no-agent-boot}"
mkdir -p "$HERE"
IMG_URL="https://cloud.debian.org/images/cloud/trixie/latest/debian-13-genericcloud-amd64.qcow2"
IMG_SHA="5754395abffb1d384d50f6d0945d46d1beb7be42a7e786e4fc4a6f27270ab16f"
BASE="$HERE/debian-13-genericcloud-amd64.qcow2"
if [ ! -f "$BASE" ]; then
  echo "fetching $(basename "$BASE") into $HERE ..." >&2
  curl -fsSL -o "$BASE" "$IMG_URL" || { echo "image download failed" >&2; exit 1; }
fi
GOT_SHA="$(sha256sum "$BASE" | cut -d' ' -f1)"
if [ "$GOT_SHA" != "$IMG_SHA" ]; then
  echo "NOTE: image sha256 $GOT_SHA differs from the recorded $IMG_SHA" >&2
  echo "      Debian republishes 'latest'; the run is still valid, record the new digest." >&2
fi
OVL="$HERE/overlay.qcow2"
SEED="$HERE/seed.img"
KEY="$HERE/buyer_ed25519"
LOG="$HERE/serial.log"
QERR="$HERE/qemu.stderr"
RESULTS="$HERE/results.txt"
HOSTNAME_TAG="lnrent-f39"
PASS=0; FAIL=0
: > "$RESULTS"

say() { printf '%s\n' "$*" | tee -a "$RESULTS"; }
check() { if [ "$2" = "$3" ]; then PASS=$((PASS+1)); say "PASS  $1  (= $3)";
          else FAIL=$((FAIL+1)); say "FAIL  $1  (expected $2, got $3)"; fi }
die()  { FAIL=$((FAIL+1)); say "ABORT $*"; [ -n "${VMPID:-}" ] && kill "$VMPID" 2>/dev/null; exit 1; }

freeport() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
SSH_PORT="$(freeport)"; PROBE_PORT="$(freeport)"

SSHOPT="-i $KEY -p $SSH_PORT -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o GlobalKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=5 \
        -o BatchMode=yes -o PasswordAuthentication=no -o KbdInteractiveAuthentication=no \
        -o NumberOfPasswordPrompts=0"
# Every guest command goes through this: stdin closed and a hard wall-clock cap, so a
# guest that accepts TCP and then stalls can never hang the run (it hung for 900s once).
rssh() { timeout 20 ssh -n $SSHOPT buyer@127.0.0.1 "$@" 2>/dev/null; }

say "=== F39 no-agent boot run  $(date -u +%FT%TZ)"
say "image:  $(basename "$BASE")"
say "sha256: $(sha256sum "$BASE" | cut -d' ' -f1)"
say "qemu:   $(qemu-system-x86_64 --version | head -1)"
say "ports:  ssh=$SSH_PORT probe=$PROBE_PORT (chosen free at runtime)"

# ---- Buyer key (the Order's supplied public key, VMH-31) -------------------
rm -f "$KEY" "$KEY.pub"; ssh-keygen -t ed25519 -N '' -f "$KEY" -q
BUYER_PUB="$(cat "$KEY.pub")"

# ---- NoCloud seed: Buyer key FIRST, then the Recipe's Seed fragment --------
cat > "$HERE/user-data" <<EOF
#cloud-config
users:
  - name: buyer
    sudo: ALL=(ALL) NOPASSWD:ALL
    shell: /bin/bash
    ssh_authorized_keys:
      - $BUYER_PUB
# ---- Seed fragment (Recipe-supplied, carries no secrets, ADR-0026) --------
write_files:
  - path: /etc/lnrent-workload.conf
    permissions: '0644'
    content: |
      workload=probe-demo
      seeded_by=lnrent-seed-fragment
  - path: /etc/systemd/system/lnrent-probe.service
    permissions: '0644'
    content: |
      [Unit]
      Description=lnrent readiness probe target
      After=network.target
      [Service]
      ExecStart=/usr/bin/python3 -m http.server $PROBE_PORT --directory /etc
      Restart=always
      [Install]
      WantedBy=multi-user.target
runcmd:
  - [ systemctl, daemon-reload ]
  - [ systemctl, enable, --now, lnrent-probe.service ]
EOF
cat > "$HERE/meta-data" <<EOF
instance-id: lnrent-f39-0001
local-hostname: $HOSTNAME_TAG
EOF
cloud-localds "$SEED" "$HERE/user-data" "$HERE/meta-data" || die "seed build failed"

rm -f "$OVL"; qemu-img create -q -f qcow2 -b "$BASE" -F qcow2 "$OVL" 8G
: > "$LOG"; : > "$QERR"

boot_vm() {
  qemu-system-x86_64 \
    -machine q35,accel=kvm -cpu host -m 1024 -smp 2 \
    -drive file="$OVL",if=virtio,format=qcow2 \
    -drive file="$SEED",if=virtio,format=raw \
    -netdev user,id=n0,hostfwd=tcp::$SSH_PORT-:22,hostfwd=tcp::$PROBE_PORT-:$PROBE_PORT \
    -device virtio-net-pci,netdev=n0 \
    -nographic -serial file:"$LOG" -monitor none -display none >/dev/null 2>>"$QERR" &
  echo $!
}
VMPID="$(boot_vm)"
sleep 3
grep -q 'Could not set up host forwarding' "$QERR" && die "qemu could not bind the forwarding ports"
kill -0 "$VMPID" 2>/dev/null || die "qemu exited immediately: $(tail -2 "$QERR")"
say "qemu pid $VMPID -- no -device virtio-serial, no guest-agent chardev, no agent share"

# ---- wait until OUR guest answers: identity, not just a live port ----------
wait_mine() {
  for _ in $(seq 1 90); do
    kill -0 "$VMPID" 2>/dev/null || { say "      qemu died while waiting"; return 1; }
    if [ "$(rssh 'hostname' 2>/dev/null)" = "$HOSTNAME_TAG" ]; then return 0; fi
    sleep 2
  done; return 1
}
if ! wait_mine; then
  say "--- diagnostics (guest never identified)"
  say "serial tail: $(tail -5 "$LOG" | tr '\n' '|')"
  say "keyscan: $(ssh-keyscan -p $SSH_PORT -T 3 127.0.0.1 2>&1 | head -2 | tr '\n' '|')"
  timeout 20 ssh -vn $SSHOPT buyer@127.0.0.1 true 2>&1 | grep -iE 'Authenticat|Offering|denied|refused|banner|Connection' | head -8 | while read -r l; do say "ssh: $l"; done
  die "no guest identifying as $HOSTNAME_TAG after 180s"
fi
say "identity: guest reports hostname $HOSTNAME_TAG -- this run's VM, not a stray listener"

# ---- (d) host keys over an SSH key exchange, before trusting anything ------
KEYS="$(ssh-keyscan -p $SSH_PORT -T 5 127.0.0.1 2>/dev/null | awk '{print $2}' | sort -u | tr '\n' ' ')"
check "host-keys-readable-over-network" "yes" "$([ -n "$KEYS" ] && echo yes || echo no)"
say "      algorithms: $KEYS"

# ---- (c) Readiness probe on the unit's own address, no agent --------------
check "readiness-probe-http" "200" \
  "$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://127.0.0.1:$PROBE_PORT/lnrent-workload.conf)"

# ---- (f) Buyer key logs in; the Seed fragment took effect ------------------
check "buyer-key-login" "buyer" "$(rssh 'id -un' 2>/dev/null)"
check "seed-fragment-applied" "1" \
  "$(rssh 'grep -c seeded_by=lnrent-seed-fragment /etc/lnrent-workload.conf' 2>/dev/null)"
say "      cloud-init: $(rssh 'cloud-init status' 2>/dev/null | head -1)"
say "      datasource: $(rssh 'sudo cloud-init query platform' 2>/dev/null)"

# ---- (b) prove NO guest agent is loaded -----------------------------------
check "no-virtio-serial-agent-channel" "0" \
  "$(rssh 'ls /dev/virtio-ports 2>/dev/null | wc -l' 2>/dev/null)"
# Match on the executable name only. An earlier version used `pgrep -f`, whose
# pattern matched the shell that was running the pgrep -- it reported a guest
# agent that did not exist.
AGENT_RE='^(qemu-ga|qemu-guest-agent|incus-agent|lxd-agent)$'
check "no-guest-agent-process" "0" \
  "$(rssh "ps -eo comm= | grep -cE '$AGENT_RE' || true" 2>/dev/null)"
check "no-guest-agent-binary" "0" \
  "$(rssh 'command -v qemu-ga incus-agent lxd-agent 2>/dev/null | wc -l' 2>/dev/null)"

# ---- negative control: the checks can fail --------------------------------
say "--- negative control"
rssh 'sudo mkdir -p /dev/virtio-ports && sudo touch /dev/virtio-ports/org.fake.agent.0' >/dev/null 2>&1
NEG="$(rssh 'ls /dev/virtio-ports 2>/dev/null | wc -l' 2>/dev/null)"
if [ "$NEG" = "0" ]; then FAIL=$((FAIL+1)); say "FAIL  negative-control: planted agent channel was NOT detected";
else PASS=$((PASS+1)); say "PASS  negative-control: planted agent channel detected (= $NEG)"; fi
rssh 'sudo rm -rf /dev/virtio-ports' >/dev/null 2>&1
# a process whose executable really is named qemu-ga must be seen
rssh 'sudo cp /bin/sleep /usr/local/bin/qemu-ga && sudo sh -c "nohup /usr/local/bin/qemu-ga 60 >/dev/null 2>&1 &"' >/dev/null 2>&1
sleep 2
NEG2="$(rssh "ps -eo comm= | grep -cE '$AGENT_RE' || true" 2>/dev/null)"
if [ "${NEG2:-0}" -ge 1 ]; then PASS=$((PASS+1)); say "PASS  negative-control: planted qemu-ga process detected (= $NEG2)";
else FAIL=$((FAIL+1)); say "FAIL  negative-control: planted qemu-ga process NOT detected"; fi
rssh 'sudo pkill -x qemu-ga; sudo rm -f /usr/local/bin/qemu-ga' >/dev/null 2>&1

# ---- (e) reboot and repeat the observable facts ---------------------------
say "--- reboot"
rssh 'sudo systemctl reboot' >/dev/null 2>&1
sleep 8
if wait_mine; then
  check "readiness-probe-after-reboot" "200" \
    "$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://127.0.0.1:$PROBE_PORT/lnrent-workload.conf)"
  check "host-keys-stable-across-reboot" "$KEYS" \
    "$(ssh-keyscan -p $SSH_PORT -T 5 127.0.0.1 2>/dev/null | awk '{print $2}' | sort -u | tr '\n' ' ')"
  check "no-agent-channel-after-reboot" "0" \
    "$(rssh 'ls /dev/virtio-ports 2>/dev/null | wc -l' 2>/dev/null)"
else
  FAIL=$((FAIL+1)); say "FAIL reboot: guest did not come back"
fi

kill "$VMPID" 2>/dev/null; wait "$VMPID" 2>/dev/null
say "=== $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
