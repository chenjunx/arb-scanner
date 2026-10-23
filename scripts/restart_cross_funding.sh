#!/usr/bin/env bash
# 启动 cross-funding 策略；已有进程在跑就先杀掉再重启。
#
# 流程：
#   1. cargo build --release —— 先编译，编译失败直接退出，不动正在跑的进程
#   2. 找已有的 cross-funding 进程(pid 文件 + 按命令行匹配，手动起的也能找到)，
#      先 SIGTERM，等 STOP_TIMEOUT 秒还没退就 SIGKILL
#   3. nohup 后台启动，stdout/stderr 追加到 LOG_FILE，pid 写进 PID_FILE
#
# 注意：cross-funding 没有优雅退出，杀进程时如果正好有一行刚下完一条腿，
# 会留下单腿敞口。重启后启动检查会跳过已有持仓的行，但不会自动补腿，
# 重启前最好看一眼日志/仓位(scripts/check_positions.sh)。
#
# 默认实盘(自动带 --live)，--paper 切模拟盘；其余参数原样透传给 cross-funding。
#
# 用法：
#   ./scripts/restart_cross_funding.sh                 # 实盘
#   ./scripts/restart_cross_funding.sh --paper         # 模拟盘
#   ./scripts/restart_cross_funding.sh --margin-mode isolated
#   CSV=my_pairs.csv RUST_LOG=debug ./scripts/restart_cross_funding.sh
#   ./scripts/restart_cross_funding.sh --stop          # 只停不启

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CSV="${CSV:-cross_funding_pairs.csv}"
CONFIG="${CONFIG:-config.toml}"
LOG_FILE="${LOG_FILE:-logs/cross_funding.log}"
PID_FILE="${PID_FILE:-logs/cross_funding.pid}"
STOP_TIMEOUT="${STOP_TIMEOUT:-15}"
export RUST_LOG="${RUST_LOG:-info}"

BIN="$ROOT_DIR/target/release/arb-scanner"
# 匹配 `arb-scanner cross-funding`，不误伤 cross-funding-kstar / cross-funding-review
PROC_PATTERN='arb-scanner(\.exe)? cross-funding( |$)'

STOP_ONLY=0
PAPER=0
ARGS=()
for arg in "$@"; do
    case "$arg" in
        --stop) STOP_ONLY=1 ;;
        --paper) PAPER=1 ;;
        --live) ;;  # 默认就是实盘
        *) ARGS+=("$arg") ;;
    esac
done
if (( PAPER )); then
    MODE="模拟盘"
else
    MODE="实盘"
    ARGS+=("--live")
fi

find_pids() {
    {
        if [[ -f "$PID_FILE" ]]; then
            local pid
            pid="$(cat "$PID_FILE")"
            if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
                echo "$pid"
            fi
        fi
        pgrep -f "$PROC_PATTERN" 2>/dev/null || true
    } | sort -u
}

stop_existing() {
    local pids
    pids="$(find_pids)"
    if [[ -z "$pids" ]]; then
        echo "没有在跑的 cross-funding 进程"
        rm -f "$PID_FILE"
        return
    fi

    echo "停止已有 cross-funding 进程: $(echo $pids)"
    # shellcheck disable=SC2086
    kill -TERM $pids 2>/dev/null || true

    local waited=0
    while (( waited < STOP_TIMEOUT )); do
        local alive=()
        for pid in $pids; do
            if kill -0 "$pid" 2>/dev/null; then alive+=("$pid"); fi
        done
        if (( ${#alive[@]} == 0 )); then
            echo "已停止"
            rm -f "$PID_FILE"
            return
        fi
        sleep 1
        waited=$((waited + 1))
    done

    echo "等待 ${STOP_TIMEOUT}s 仍未退出，SIGKILL: $(echo $pids)"
    # shellcheck disable=SC2086
    kill -KILL $pids 2>/dev/null || true
    sleep 1
    rm -f "$PID_FILE"
}

if (( STOP_ONLY )); then
    stop_existing
    exit 0
fi

if [[ ! -f "$CSV" ]]; then
    echo "CSV 不存在: $CSV" >&2
    exit 1
fi
if [[ ! -f "$CONFIG" ]]; then
    echo "配置文件不存在: $CONFIG" >&2
    exit 1
fi

echo "编译 release..."
cargo build --release

stop_existing

KILL_SWITCH="$(sed -nE 's/^[[:space:]]*kill_switch_file[[:space:]]*=[[:space:]]*"([^"]*)".*/\1/p' "$CONFIG" | head -n1)"
KILL_SWITCH="${KILL_SWITCH:-cross_funding.stop}"
if [[ -e "$KILL_SWITCH" ]]; then
    echo "警告: kill switch 文件 $KILL_SWITCH 存在，启动后所有行会暂停决策和触发" >&2
fi

mkdir -p "$(dirname "$LOG_FILE")" "$(dirname "$PID_FILE")"

{
    echo
    echo "===== $(date '+%Y-%m-%d %H:%M:%S') 启动 cross-funding ($MODE) args: --csv $CSV --config $CONFIG ${ARGS[*]-} ====="
} >> "$LOG_FILE"

nohup "$BIN" cross-funding --csv "$CSV" --config "$CONFIG" ${ARGS[@]+"${ARGS[@]}"} >> "$LOG_FILE" 2>&1 &
NEW_PID=$!
echo "$NEW_PID" > "$PID_FILE"

# 启动即失败(参数错、缺配置段等)的情况在这里就能发现
sleep 3
if ! kill -0 "$NEW_PID" 2>/dev/null; then
    echo "cross-funding 启动后立即退出，最近日志:" >&2
    tail -n 30 "$LOG_FILE" >&2
    rm -f "$PID_FILE"
    exit 1
fi

echo "cross-funding 已启动 ($MODE) pid=$NEW_PID"
echo "日志: tail -f $LOG_FILE"
