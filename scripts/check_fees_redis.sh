#!/usr/bin/env bash
# 查 Redis 里的费率/手续费情况，涵盖：
#   1. arb_scanner:funding_cursor  — 资金费轮询游标（上次处理到的时间 + tranId）
#   2. arb_scanner:positions       — 仓位里的累计手续费 total_fees + 已实现盈亏
#
# 用法:
#   ./check_fees_redis.sh                     # 默认连 redis://127.0.0.1:6379
#   REDIS_URL=redis://192.168.1.1:6379 ./check_fees_redis.sh

set -euo pipefail

REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_DB="${REDIS_DB:-0}"

# 支持 REDIS_URL 格式 redis://host:port/db
if [[ -n "${REDIS_URL:-}" ]]; then
    # 解析 redis://[host][:port][/db]
    REDIS_HOST="$(echo "$REDIS_URL" | sed -E 's|redis://([^:/]+).*|\1|')"
    REDIS_PORT="$(echo "$REDIS_URL" | sed -E 's|redis://[^:]+:([0-9]+).*|\1|; t; s|.*|6379|')"
    REDIS_DB="$(echo "$REDIS_URL" | sed -E 's|.*/([0-9]+)$|\1|; t; s|.*|0|')"
fi

CLI="redis-cli -h $REDIS_HOST -p $REDIS_PORT -n $REDIS_DB"

# ---- 工具函数 ----

ms_to_datetime() {
    local ms="$1"
    if [[ -z "$ms" || "$ms" == "0" ]]; then echo "(未设置)"; return; fi
    local sec=$((ms / 1000))
    # Windows git bash / Linux 兼容
    if date --version >/dev/null 2>&1; then
        date -u -d "@$sec" '+%Y-%m-%d %H:%M:%S UTC' 2>/dev/null || date -u -r "$sec" '+%Y-%m-%d %H:%M:%S UTC'
    else
        date -u -r "$sec" '+%Y-%m-%d %H:%M:%S UTC'
    fi
}

require_jq() {
    if ! command -v jq >/dev/null 2>&1; then
        echo "[警告] 未找到 jq，JSON 将以原始格式输出" >&2
        return 1
    fi
    return 0
}

has_jq=0
require_jq && has_jq=1

echo "Redis: $REDIS_HOST:$REDIS_PORT db=$REDIS_DB"
echo "========================================================"

# ======== 1. funding_cursor ========
echo
echo "【资金费游标 arb_scanner:funding_cursor】"
echo "  (last_time_ms=上次轮询截止时间, last_tran_id=已处理最大交易号)"
echo "--------------------------------------------------------"

cursor_fields=$($CLI HGETALL arb_scanner:funding_cursor)

if [[ -z "$cursor_fields" ]]; then
    echo "  (空，资金费游标尚未写入)"
else
    # redis HGETALL 输出: field\nvalue\nfield\nvalue...
    readarray -t lines <<< "$cursor_fields"
    i=0
    while [[ $i -lt ${#lines[@]} ]]; do
        field="${lines[$i]}"
        value="${lines[$((i+1))]}"
        i=$((i+2))

        if [[ $has_jq -eq 1 ]]; then
            last_time_ms=$(echo "$value" | jq -r '.last_time_ms // 0')
            last_tran_id=$(echo "$value" | jq -r '.last_tran_id // 0')
            datetime=$(ms_to_datetime "$last_time_ms")
            printf "  %-30s  last_time=%s  last_tran_id=%s\n" \
                "$field" "$datetime" "$last_tran_id"
        else
            echo "  $field => $value"
        fi
    done
fi

# ======== 2. positions — total_fees & realized_pnl ========
echo
echo "【仓位费用汇总 arb_scanner:positions】"
echo "  (total_fees=累计手续费/币种, realized_pnl=已实现盈亏 USDT)"
echo "--------------------------------------------------------"

pos_fields=$($CLI HGETALL arb_scanner:positions)

if [[ -z "$pos_fields" ]]; then
    echo "  (空，无持仓记录)"
else
    readarray -t lines <<< "$pos_fields"
    i=0
    total_fees_usdt="0"
    total_rpnl="0"

    while [[ $i -lt ${#lines[@]} ]]; do
        field="${lines[$i]}"
        value="${lines[$((i+1))]}"
        i=$((i+2))

        if [[ $has_jq -eq 1 ]]; then
            net_qty=$(echo "$value" | jq -r '.net_qty // "0"')
            avg_price=$(echo "$value" | jq -r '.avg_price // "—"')
            realized_pnl=$(echo "$value" | jq -r '.realized_pnl // "0"')
            updated_ms=$(echo "$value" | jq -r '.updated_at_ms // 0')
            updated=$(ms_to_datetime "$updated_ms")

            echo "  [$field]"
            printf "    net_qty=%-14s avg_price=%-14s realized_pnl=%-14s updated=%s\n" \
                "$net_qty" "$avg_price" "$realized_pnl" "$updated"

            # total_fees 按币种列出
            fees=$(echo "$value" | jq -r '.total_fees // {} | to_entries[] | "    fee[\(.key)]=\(.value)"' 2>/dev/null)
            if [[ -n "$fees" ]]; then
                echo "$fees"
            else
                echo "    total_fees=(无)"
            fi
            echo
        else
            echo "  $field =>"
            echo "    $value"
            echo
        fi
    done
fi

# ======== 3. 快速核对：funding_cursor 条数 vs positions 条数 ========
cursor_count=$($CLI HLEN arb_scanner:funding_cursor 2>/dev/null || echo 0)
pos_count=$($CLI HLEN arb_scanner:positions 2>/dev/null || echo 0)
order_count=$($CLI HLEN arb_scanner:orders 2>/dev/null || echo 0)
adj_count=$($CLI LLEN arb_scanner:positions_adjustments 2>/dev/null || echo 0)

echo "========================================================"
echo "汇总:"
printf "  positions       = %s 条\n" "$pos_count"
printf "  funding_cursor  = %s 条\n" "$cursor_count"
printf "  orders          = %s 条\n" "$order_count"
printf "  adjustments日志 = %s 条\n" "$adj_count"
