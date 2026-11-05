#!/usr/bin/env bash
#
# End-to-end check against a running Heimdall cluster.
#
# Brings nothing up and tears nothing down - start the cluster first:
#   docker compose up -d --build
#   scripts/smoke-test.sh
#
# It uploads a random file, waits for replication, verifies the download is byte-identical,
# checks a handful of byte ranges against the source, then kills the node that reads are being
# routed to and confirms the next holder serves the same bytes.

set -euo pipefail

GATEWAY="${GATEWAY:-http://localhost:${HEIMDALL_GATEWAY_PORT:-8090}}"
API_KEY="${API_KEY:-dev-heimdall-key}"
SIZE_MB="${SIZE_MB:-24}"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

pass=0
fail=0

ok()   { printf '  \033[32mPASS\033[0m  %s\n' "$1"; pass=$((pass + 1)); }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; fail=$((fail + 1)); }
step() { printf '\n\033[1m%s\033[0m\n' "$1"; }

check() { if [ "$2" = "$3" ]; then ok "$1 ($2)"; else bad "$1: expected '$3', got '$2'"; fi; }

header() { grep -i "^$2:" "$1" | tr -d '\r' | cut -d' ' -f2- | tail -1; }
status() { head -1 "$1" | tr -d '\r' | cut -d' ' -f2; }

# ---------------------------------------------------------------------------------------------

step "Cluster"
curl -fsS "$GATEWAY/api/cluster" > "$WORK/cluster.json"
nodes=$(python3 -c "import json;print(json.load(open('$WORK/cluster.json'))['nodeCount'])")
tokens=$(python3 -c "import json;print(json.load(open('$WORK/cluster.json'))['tokenCount'])")
echo "  $nodes nodes, $tokens virtual nodes on the ring"

# Wait rather than assert: a node restarted by a previous run may still be coming up, and the
# point of this check is "the cluster is ready", not "it was ready the instant we asked".
healthy=0
for _ in $(seq 1 60); do
  healthy=$(curl -sS "$GATEWAY/api/cluster/health" | python3 -c "import sys,json;print(json.load(sys.stdin)['healthyNodes'])")
  [ "$healthy" = "$nodes" ] && break
  sleep 1
done
check "all nodes healthy" "$healthy" "$nodes"

step "Upload (${SIZE_MB} MiB, streamed)"
head -c "$((SIZE_MB * 1024 * 1024))" /dev/urandom > "$WORK/source.bin"
source_hash=$(sha256sum "$WORK/source.bin" | cut -d' ' -f1)

curl -fsS -X POST "$GATEWAY/api/videos?fileName=smoke-test.mp4" \
  -H "X-Heimdall-Key: $API_KEY" \
  -H "Content-Type: video/mp4" \
  --data-binary "@$WORK/source.bin" > "$WORK/upload.json"

video_id=$(python3 -c "import json;print(json.load(open('$WORK/upload.json'))['videoId'])")
primary=$(python3 -c "import json;print(json.load(open('$WORK/upload.json'))['primaryNodeId'])")
replicas=$(python3 -c "import json;print(' '.join(json.load(open('$WORK/upload.json'))['replicaNodeIds']))")
upload_hash=$(python3 -c "import json;print(json.load(open('$WORK/upload.json'))['contentHash'])")

echo "  id=$video_id  primary=$primary  replicas=$replicas"
check "ingest hash equals the source SHA-256" "$upload_hash" "$source_hash"

step "Replication"
# Replication is asynchronous, so poll rather than assuming it has already landed.
for _ in $(seq 1 30); do
  held=0
  for node in $primary $replicas; do
    port="810${node#node-}"
    if curl -fsS -o /dev/null "http://localhost:$port/internal/videos/$video_id/manifest" 2>/dev/null; then
      held=$((held + 1))
    fi
  done
  [ "$held" -eq 3 ] && break
  sleep 1
done
check "all three holders have a copy" "$held" "3"

step "Download"
curl -fsS -D "$WORK/h.txt" "$GATEWAY/api/videos/$video_id" -o "$WORK/download.bin"
served_by=$(header "$WORK/h.txt" "X-Heimdall-Served-By")

check "status" "$(status "$WORK/h.txt")" "200"
check "Accept-Ranges advertised" "$(header "$WORK/h.txt" "Accept-Ranges")" "bytes"
check "read was load balanced off the primary" "$([ "$served_by" != "$primary" ] && echo yes || echo no)" "yes"

if cmp -s "$WORK/source.bin" "$WORK/download.bin"; then
  ok "download is byte-identical to the upload"
else
  bad "download differs from the upload"
fi

step "Range requests"
total=$((SIZE_MB * 1024 * 1024))
range_case() {
  local spec="$1" offset="$2" length="$3"
  curl -fsS -D "$WORK/rh.txt" -H "Range: bytes=$spec" "$GATEWAY/api/videos/$video_id" -o "$WORK/r.bin"
  dd if="$WORK/source.bin" of="$WORK/expected.bin" bs=1 skip="$offset" count="$length" status=none

  if [ "$(status "$WORK/rh.txt")" = "206" ] && cmp -s "$WORK/r.bin" "$WORK/expected.bin"; then
    ok "bytes=$spec -> 206, $length bytes, matches source"
  else
    bad "bytes=$spec -> $(status "$WORK/rh.txt"), $(stat -c%s "$WORK/r.bin") bytes"
  fi
}

range_case "0-1023" 0 1024
range_case "1000-1999" 1000 1000
range_case "$((total - 1024))-" "$((total - 1024))" 1024
range_case "-2048" "$((total - 2048))" 2048
# Straddles the 8 MiB chunk boundary: the slice has to be stitched from two stored chunks.
range_case "8388607-8388609" 8388607 3

curl -s -D "$WORK/oob.txt" -o /dev/null -H "Range: bytes=999999999-" "$GATEWAY/api/videos/$video_id"
check "out-of-range request" "$(status "$WORK/oob.txt")" "416"
check "416 advertises the real length" "$(header "$WORK/oob.txt" "Content-Range")" "bytes */$total"

step "Failover"
if command -v docker >/dev/null 2>&1; then
  victim="$served_by"
  echo "  stopping $victim, the node that just served the read"
  docker compose stop "storage-node-${victim#node-}" >/dev/null 2>&1

  curl -fsS -D "$WORK/fo.txt" -H "Range: bytes=1000-1999" "$GATEWAY/api/videos/$video_id" -o "$WORK/fo.bin"
  dd if="$WORK/source.bin" of="$WORK/expected.bin" bs=1 skip=1000 count=1000 status=none

  check "ranged read still succeeds" "$(status "$WORK/fo.txt")" "206"
  if cmp -s "$WORK/fo.bin" "$WORK/expected.bin"; then
    ok "bytes are correct after failover (served by $(header "$WORK/fo.txt" "X-Heimdall-Served-By"))"
  else
    bad "bytes are wrong after failover"
  fi

  echo "  restarting $victim"
  docker compose start "storage-node-${victim#node-}" >/dev/null 2>&1

  # Wait for it to come back before moving on: with a holder still unreachable the gateway
  # correctly answers 503 rather than 404, which would make the delete check below ambiguous.
  for _ in $(seq 1 30); do
    [ "$(curl -sS "$GATEWAY/api/cluster/health" | python3 -c 'import sys,json;print(json.load(sys.stdin)["healthyNodes"])')" = "$nodes" ] && break
    sleep 1
  done
else
  echo "  skipped: docker not on PATH"
fi

step "Mid-stream resume"
if command -v docker >/dev/null 2>&1; then
  # The hard case: kill the node *while it is sending the body*. The status line and
  # Content-Length are already on the wire, so the only correct behaviour is to fetch the unsent
  # remainder from another holder and finish the same response.
  rate=$(( SIZE_MB * 1024 / 8 ))        # finish in roughly 8 seconds
  ( curl -s --limit-rate "${rate}k" -D "$WORK/mh.txt" "$GATEWAY/api/videos/$video_id" -o "$WORK/mid.bin"; \
    echo $? > "$WORK/mid.exit" ) &
  pumping=$!

  sleep 3
  mid_victim=$(grep -i '^X-Heimdall-Served-By:' "$WORK/mh.txt" 2>/dev/null | tr -d '\r' | cut -d' ' -f2)

  if [ -n "$mid_victim" ]; then
    echo "  SIGKILLing $mid_victim mid-body"
    docker kill "heimdall-storage-node-${mid_victim#node-}" >/dev/null 2>&1
    wait "$pumping"

    check "curl reported success" "$(cat "$WORK/mid.exit")" "0"
    check "full length delivered" "$(stat -c%s "$WORK/mid.bin")" "$total"

    if cmp -s "$WORK/source.bin" "$WORK/mid.bin"; then
      ok "response is byte-identical after the serving node was killed mid-body"
    else
      bad "response is corrupt after mid-body failover"
    fi

    docker start "heimdall-storage-node-${mid_victim#node-}" >/dev/null 2>&1
    for _ in $(seq 1 30); do
      [ "$(curl -sS "$GATEWAY/api/cluster/health" | python3 -c 'import sys,json;print(json.load(sys.stdin)["healthyNodes"])')" = "$nodes" ] && break
      sleep 1
    done
  else
    wait "$pumping" || true
    echo "  skipped: the transfer finished before it could be interrupted"
  fi
else
  echo "  skipped: docker not on PATH"
fi

step "Authentication"
check "write without a key"  "$(curl -s -o /dev/null -w '%{http_code}' -X POST "$GATEWAY/api/videos" --data-binary 'x')" "401"
check "write with a bad key" "$(curl -s -o /dev/null -w '%{http_code}' -X POST "$GATEWAY/api/videos" -H "X-Heimdall-Key: wrong" --data-binary 'x')" "401"
check "reads need no key"    "$(curl -s -o /dev/null -w '%{http_code}' -I "$GATEWAY/api/videos/$video_id")" "200"

step "Cleanup"
curl -fsS -X DELETE "$GATEWAY/api/videos/$video_id" -H "X-Heimdall-Key: $API_KEY" > /dev/null
check "read after delete" "$(curl -s -o /dev/null -w '%{http_code}' "$GATEWAY/api/videos/$video_id")" "404"

printf '\n\033[1m%d passed, %d failed\033[0m\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
