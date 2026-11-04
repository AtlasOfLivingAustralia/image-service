#!/usr/bin/env bash
# Inject network faults between image-service and the local S3 (via toxiproxy's HTTP API).
# Usage: s3-local/toxic.sh <command> [args]
#
#   blackhole [stream]        accept connections but never deliver data (half-open / NAT-drop style)
#   latency <ms> [jitter]     add latency to downstream (S3 -> app) and upstream (app -> S3)
#   slow <KB/s>               bandwidth limit both directions
#   reset [ms]                RST connections after <ms> (default 0)
#   slowclose <ms>            delay socket close
#   cut <bytes>               close connection after <bytes> sent upstream (partial upload)
#   down | up                 disable/enable the proxy (drops all connections, refuses new ones)
#   clear                     remove all toxics and re-enable the proxy
#   list                      show proxy + toxics
#
# stream defaults to "upstream" (app -> S3, i.e. the upload body direction) where relevant.
set -euo pipefail

API="${TOXIPROXY_API:-http://localhost:8474}"
PROXY="${TOXIPROXY_PROXY:-s3}"

toxic() { # name type stream attributes-json
    curl -fsS -X POST "$API/proxies/$PROXY/toxics" -H 'Content-Type: application/json' \
        -d "{\"name\":\"$1\",\"type\":\"$2\",\"stream\":\"$3\",\"toxicity\":1.0,\"attributes\":$4}" >/dev/null
    echo "added $1 ($2, $3) $4"
}

cmd="${1:-list}"; shift || true
case "$cmd" in
    blackhole) toxic "blackhole_${1:-upstream}" timeout "${1:-upstream}" '{"timeout":0}' ;;
    latency)   toxic latency_down latency downstream "{\"latency\":${1:?ms},\"jitter\":${2:-0}}"
               toxic latency_up latency upstream "{\"latency\":${1},\"jitter\":${2:-0}}" ;;
    slow)      toxic bandwidth_up bandwidth upstream "{\"rate\":${1:?KB/s}}"
               toxic bandwidth_down bandwidth downstream "{\"rate\":${1}}" ;;
    reset)     toxic reset_peer reset_peer upstream "{\"timeout\":${1:-0}}" ;;
    slowclose) toxic slow_close slow_close downstream "{\"delay\":${1:?ms}}" ;;
    cut)       toxic limit_data limit_data upstream "{\"bytes\":${1:?bytes}}" ;;
    down)      curl -fsS -X POST "$API/proxies/$PROXY" -H 'Content-Type: application/json' -d '{"enabled":false}' >/dev/null; echo "proxy disabled" ;;
    up)        curl -fsS -X POST "$API/proxies/$PROXY" -H 'Content-Type: application/json' -d '{"enabled":true}' >/dev/null; echo "proxy enabled" ;;
    clear)     curl -fsS -X POST "$API/reset" >/dev/null; echo "toxics cleared, proxy enabled" ;;
    list)      curl -fsS "$API/proxies/$PROXY"; echo ;;
    *)         sed -n '2,17p' "$0"; exit 1 ;;
esac
