#!/usr/bin/env bash
# 查 Redis 里的全量仓位状态 (arb_scanner:positions)：
#   1. 仓位明细 — 每个 venue|symbol 的 net_qty/avg_price/pending_qty/realized_pnl/total_fees
#   2. 按 base 资产跨 venue 聚合 — net_qty 之和(用来判断是否已对冲)、realized_pnl 之和
#
# 注意：这里只是直接读 Redis 里的记账快照，不接实时行情，所以没有
# market_value/unrealized_pnl —— 这两个数字只有跑起来的 arb-scanner 进程
# (接了行情源的那个)才能算，参见 PortfolioManager/PortfolioSection。
#
# 用法:
#   ./check_positions.sh                      # 默认连 redis://127.0.0.1:6379
#   ./check_positions.sh --all                # 连 flat(net_qty=0) 的历史记录也列出来
#   REDIS_URL=redis://192.168.1.1:6379 ./check_positions.sh

set -euo pipefail

REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_DB="${REDIS_DB:-0}"
SHOW_ALL=0

for arg in "$@"; do
    case "$arg" in
        --all) SHOW_ALL=1 ;;
        *)
            echo "unknown argument: $arg" >&2
            echo "usage: $0 [--all]" >&2
            exit 1
            ;;
    esac
done

# 支持 REDIS_URL 格式 redis://host:port/db
if [[ -n "${REDIS_URL:-}" ]]; then
    REDIS_HOST="$(echo "$REDIS_URL" | sed -E 's|redis://([^:/]+).*|\1|')"
    REDIS_PORT="$(echo "$REDIS_URL" | sed -E 's|redis://[^:]+:([0-9]+).*|\1|; t; s|.*|6379|')"
    REDIS_DB="$(echo "$REDIS_URL" | sed -E 's|.*/([0-9]+)$|\1|; t; s|.*|0|')"
fi

CLI="redis-cli -h $REDIS_HOST -p $REDIS_PORT -n $REDIS_DB"

ms_to_datetime() {
    local ms="$1"
    if [[ -z "$ms" || "$ms" == "0" ]]; then echo "(未设置)"; return; fi
    local sec=$((ms / 1000))
    if date --version >/dev/null 2>&1; then
        date -u -d "@$sec" '+%Y-%m-%d %H:%M:%S UTC' 2>/dev/null || date -u -r "$sec" '+%Y-%m-%d %H:%M:%S UTC'
    else
        date -u -r "$sec" '+%Y-%m-%d %H:%M:%S UTC'
    fi
}

if ! command -v jq >/dev/null 2>&1; then
    echo "[错误] 本脚本需要 jq 来解析仓位 JSON，请先安装 jq" >&2
    exit 1
fi

echo "Redis: $REDIS_HOST:$REDIS_PORT db=$REDIS_DB"
echo "========================================================"

pos_fields="$($CLI HGETALL arb_scanner:positions)"

if [[ -z "$pos_fields" ]]; then
    echo "(空，无持仓记录)"
    exit 0
fi

readarray -t lines <<< "$pos_fields"

echo
echo "【仓位明细】$( [[ $SHOW_ALL -eq 1 ]] && echo "(含 net_qty=0 的历史记录)" || echo "(仅当前非零持仓，--all 看全部)" )"
echo "--------------------------------------------------------"

# 用临时文件按 base 资产聚合 net_qty/realized_pnl，避免子进程/管道里的变量
# 在 bash 里不能跨 subshell 累加。
agg_tmp="$(mktemp)"
trap 'rm -f "$agg_tmp"' EXIT

i=0
open_count=0
while [[ $i -lt ${#lines[@]} ]]; do
    field="${lines[$i]}"
    value="${lines[$((i+1))]}"
    i=$((i+2))

    venue="${field%%|*}"
    symbol="${field#*|}"
    base="${symbol%%/*}"

    net_qty=$(echo "$value" | jq -r '.net_qty // "0"')
    avg_price=$(echo "$value" | jq -r '.avg_price // "N/A"')
    pending_qty=$(echo "$value" | jq -r '.pending_qty // "0"')
    realized_pnl=$(echo "$value" | jq -r '.realized_pnl // "0"')
    updated_ms=$(echo "$value" | jq -r '.updated_at_ms // 0')
    updated=$(ms_to_datetime "$updated_ms")

    # 累加进按资产聚合的表：base_asset net_qty realized_pnl
    echo "$base $net_qty $realized_pnl" >> "$agg_tmp"

    is_flat=$(echo "$net_qty" | awk '{print ($1 == 0)}')
    if [[ "$is_flat" == "1" && $SHOW_ALL -eq 0 ]]; then
        continue
    fi
    [[ "$is_flat" != "1" ]] && open_count=$((open_count + 1))

    fees=$(echo "$value" | jq -r '.total_fees // {} | to_entries | map("\(.key)=\(.value)") | join(",")')
    [[ -z "$fees" ]] && fees="(无)"

    echo "  [$venue $symbol]"
    printf "    net_qty=%-16s avg_price=%-14s pending_qty=%-10s realized_pnl=%-14s\n" \
        "$net_qty" "$avg_price" "$pending_qty" "$realized_pnl"
    printf "    total_fees=%-30s updated=%s\n" "$fees" "$updated"
done

if [[ $SHOW_ALL -eq 0 && $open_count -eq 0 ]]; then
    echo "  (当前无持仓)"
fi

echo
echo "【按资产聚合】(net_qty 跨 venue 求和，接近 0 视为已对冲)"
echo "--------------------------------------------------------"

awk '
{
    net[$1] += $2
    pnl[$1] += $3
}
END {
    for (asset in net) {
        printf "  %-10s net_qty=%-16s realized_pnl=%s\n", asset, net[asset], pnl[asset]
    }
}
' "$agg_tmp" | sort

pos_count=$($CLI HLEN arb_scanner:positions 2>/dev/null || echo 0)
echo
echo "========================================================"
echo "汇总: positions 记录总数=$pos_count  当前非零持仓=$open_count"
