#!/bin/bash
# Single-host vearch cluster: 3 masters + 3 PSes + 2 routers.
#
# Usage:
#   ./scripts/cluster.sh start    # start everything in correct order
#   ./scripts/cluster.sh stop     # kill all vearch processes started here
#   ./scripts/cluster.sh restart  # stop then start
#   ./scripts/cluster.sh status   # show running roles + master cluster view
#   ./scripts/cluster.sh wipe     # stop + delete data/ logs/ (clean slate)
#
# Layout summary (ports):
#   m1 api=18817  m2 api=18827  m3 api=18837  (raft etcd quorum)
#   ps1 rpc=8081  ps2 rpc=8082  ps3 rpc=8083
#   r1  http=19001  r2 http=19002

set -u

# Resolve repo root (this script lives in scripts/).
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." &> /dev/null && pwd)"
cd "$ROOT"

VEARCH_BIN="${VEARCH_BIN:-./build/bin/vearch}"
CONF_DIR="${CONF_DIR:-config}"
PID_DIR="${PID_DIR:-.cluster_pids}"

MASTERS=(m1 m2 m3)
PSES=(1 2 3)
ROUTERS=(1 2)

# Master api port for status query (any of them works).
# Match m1's api_port in config/master_m1.toml.
ANY_MASTER_API=28817

mkdir -p "$PID_DIR"

start_one() {
    local role="$1"          # master|ps|router
    local instance="$2"      # m1 / 1 / 2 ...
    local conf="$3"
    local extra="${4:-}"     # e.g. "-master m1" for master
    local logfile="logs/${role}${instance}/startup.log"
    local pidfile="$PID_DIR/${role}${instance}.pid"
    mkdir -p "$(dirname "$logfile")"

    if [[ -f "$pidfile" ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        echo "  [skip] $role $instance already running pid=$(cat "$pidfile")"
        return 0
    fi

    nohup $VEARCH_BIN -conf "$conf" $extra "$role" \
        > "$logfile" 2>&1 &
    local pid=$!
    echo $pid > "$pidfile"
    echo "  [start] $role $instance pid=$pid conf=$conf"
}

start() {
    if [[ ! -x "$VEARCH_BIN" ]]; then
        echo "ERROR: vearch binary not found at $VEARCH_BIN"
        echo "  set VEARCH_BIN env or build it first"
        exit 1
    fi

    # Pre-create data directories.
    for m in "${MASTERS[@]}"; do mkdir -p "data/master_$m" "logs/master_$m"; done
    for p in "${PSES[@]}";    do mkdir -p "data/ps$p"      "logs/ps$p"; done
    for r in "${ROUTERS[@]}"; do mkdir -p "data/router$r"  "logs/router$r"; done

    echo "[1/3] starting 3 masters..."
    for m in "${MASTERS[@]}"; do
        start_one master "$m" "$CONF_DIR/master_${m}.toml" "-master $m"
    done
    echo "  waiting 8s for raft quorum..."
    sleep 8

    echo "[2/3] starting 3 PSes..."
    for p in "${PSES[@]}"; do
        start_one ps "$p" "$CONF_DIR/ps${p}.toml"
    done
    echo "  waiting 5s for PS registration..."
    sleep 5

    echo "[3/3] starting 2 routers..."
    for r in "${ROUTERS[@]}"; do
        start_one router "$r" "$CONF_DIR/router${r}.toml"
    done
    sleep 3

    echo
    status
}

stop() {
    echo "stopping vearch processes..."
    # Kill by recorded PIDs first (clean).
    for f in "$PID_DIR"/*.pid; do
        [[ -f "$f" ]] || continue
        local pid; pid=$(cat "$f")
        if kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null && echo "  TERM $pid ($(basename "$f" .pid))"
        fi
        rm -f "$f"
    done
    sleep 2
    # Sweep stragglers (anything still using our config dir).
    pkill -9 -f "vearch -conf $CONF_DIR/" 2>/dev/null || true
    sleep 1
    if pgrep -fa "vearch -conf $CONF_DIR/" >/dev/null; then
        echo "WARNING: some processes still alive:"
        pgrep -fa "vearch -conf $CONF_DIR/"
    else
        echo "all stopped."
    fi
}

status() {
    echo "=== process table ==="
    pgrep -fa "vearch -conf $CONF_DIR/" || echo "(no vearch process)"
    echo
    echo "=== listening ports ==="
    if command -v ss >/dev/null 2>&1; then
        ss -tlnp 2>/dev/null | grep vearch || echo "(no listeners)"
    else
        netstat -tlnp 2>/dev/null | grep vearch || echo "(install ss or netstat)"
    fi
    echo
    echo "=== master cluster view (via m1 api $ANY_MASTER_API) ==="
    if command -v curl >/dev/null 2>&1; then
        # Anonymous OK if skip_auth=true; else use root: with empty pwd.
        local resp
        resp=$(curl -s -m 3 -u root:secret "http://127.0.0.1:${ANY_MASTER_API}/servers" 2>/dev/null)
        if [[ -n "$resp" ]]; then
            echo "$resp" | (command -v jq >/dev/null 2>&1 && jq '.' || cat)
        else
            echo "(master not reachable on port $ANY_MASTER_API)"
        fi
    fi
}

restart() {
    stop
    sleep 2
    start
}

wipe() {
    stop
    echo "wiping data/ and logs/ ..."
    rm -rf data logs "$PID_DIR"
    echo "done. next 'start' will create a fresh cluster."
}

case "${1:-}" in
    start)   start ;;
    stop)    stop ;;
    restart) restart ;;
    status)  status ;;
    wipe)    wipe ;;
    *)
        echo "usage: $0 {start|stop|restart|status|wipe}"
        echo
        echo "  start    bring up 3 masters + 3 ps + 2 routers in order"
        echo "  stop     terminate all vearch processes started here"
        echo "  restart  stop then start"
        echo "  status   list running processes / listening ports / master view"
        echo "  wipe     stop + remove data/ logs/ (irreversible)"
        exit 1
        ;;
esac
