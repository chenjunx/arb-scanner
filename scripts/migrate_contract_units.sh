#!/usr/bin/env bash
# 一次性迁移：把 Redis 仓位(arb_scanner:positions)里 gate_futures/okx_futures
# 以前按**张**记下的数量换算成币数(合约 base 单位)。
#
# 背景：这两个 venue 以前把交易所回报的张数原样入账，但价格是每币价格，所以
# net_qty、市值、未实现盈亏、以及 realized_pnl 里由成交算出来的那部分都差了
# 一个面值倍数(如 Gate FUTUON_USDT 1 张 = 0.01 币，-49 张实际是 -0.49 币)。
# 新代码在下单网关里做张/币换算后，旧记录要换算一次：
#   net_qty / pending_qty  × 面值
#   avg_price              不变(本来就是每币价格)
#   total_fees             不变(本来就是 USDT)
#   realized_pnl           = 成交盈亏 × 面值 + 调整
#     调整 = 审计日志 arb_scanner:positions_adjustments 里这个 venue+symbol 的
#     amount 之和(资金费/手续费冲减/划转差额/人工修正，本来就是 USDT，不换算)，
#     成交盈亏 = 原 realized_pnl - 调整。
# 面值实时查交易所公开接口：Gate quanto_multiplier，OKX ctVal × ctMult。
#
# 写入前把原始 JSON 存进 arb_scanner:positions_backup_contract_units(同一个
# field)，这个 hash 同时当"已迁移"标记：已经有备份的记录再跑会跳过，不会二次
# 放大。回滚某条：
#   redis-cli HSET arb_scanner:positions "<field>" "$(redis-cli --raw HGET arb_scanner:positions_backup_contract_units "<field>")"
#
# 注意：
#   - 执行前先停掉 cross-funding 进程，迁移完再用新代码启动；
#   - 某条仓位如果用 set-position 重置过(realized_pnl 被清 0，但审计日志还在)，
#     重置前的调整也会被算进"调整"，这条的 realized_pnl 需要人工核对。
#
# 依赖: redis-cli jq bc curl(访问交易所走 curl 自己认的 HTTPS_PROXY 环境变量)
#
# 用法:
#   ./migrate_contract_units.sh             # 只读预览
#   ./migrate_contract_units.sh --confirm   # 真正写入
#   REDIS_URL=redis://192.168.1.1:6379 ./migrate_contract_units.sh

set -euo pipefail

REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_DB="${REDIS_DB:-0}"
CONFIRM=0

for arg in "$@"; do
    case "$arg" in
        --confirm) CONFIRM=1 ;;
        *)
            echo "unknown argument: $arg" >&2
            echo "usage: $0 [--confirm]" >&2
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

CLI="redis-cli --raw -h $REDIS_HOST -p $REDIS_PORT -n $REDIS_DB"
POSITIONS_KEY="arb_scanner:positions"
ADJUSTMENTS_KEY="arb_scanner:positions_adjustments"
BACKUP_KEY="arb_scanner:positions_backup_contract_units"

for cmd in redis-cli jq bc curl; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "[错误] 本脚本需要 $cmd，请先安装" >&2
        exit 1
    fi
done

export BC_LINE_LENGTH=0

# 十进制精确计算，结果去掉多余的 0：calc "(-149.7 - 0.3) * 0.01 + 0.3" → -1.2。
# scale=20：bc 乘法结果的小数位取 max(20, 两个操作数的小数位)，不会超过
# 输入里最多的那个(rust_decimal 最多 28 位)，写回去 Rust 能原样解析。
calc() {
    local out
    out="$(echo "scale=20; $1" | bc)"
    if [[ "$out" == *.* ]]; then
        out="$(echo "$out" | sed -E 's/0+$//; s/\.$//')"
    fi
    out="$(echo "$out" | sed -E 's/^(-?)\./\10./')"
    [[ "$out" == "-0" || -z "$out" ]] && out="0"
    echo "$out"
}

# 1 张 = 多少币。参数: venue base quote
contract_size() {
    local venue="$1" base="$2" quote="$3" body
    case "$venue" in
        gate_futures)
            body="$(curl -sS --fail "https://api.gateio.ws/api/v4/futures/usdt/contracts/${base}_${quote}")"
            echo "$body" | jq -r '.quanto_multiplier // empty'
            ;;
        okx_futures)
            body="$(curl -sS --fail "https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId=${base}-${quote}-SWAP")"
            local ct_ccy ct_val ct_mult
            ct_ccy="$(echo "$body" | jq -r '.data[0].ctValCcy // empty')"
            ct_val="$(echo "$body" | jq -r '.data[0].ctVal // empty')"
            ct_mult="$(echo "$body" | jq -r '.data[0].ctMult // empty')"
            if [[ "${ct_ccy^^}" != "${base^^}" || -z "$ct_val" || -z "$ct_mult" ]]; then
                echo "[错误] okx ${base}-${quote}-SWAP 合约信息异常: $body" >&2
                return 1
            fi
            calc "$ct_val * $ct_mult"
            ;;
    esac
}

echo "Redis: $REDIS_HOST:$REDIS_PORT db=$REDIS_DB"
if [[ "$CONFIRM" == 1 ]]; then
    echo "模式: --confirm，会写入 Redis"
else
    echo "模式: 只读预览(加 --confirm 才写入)"
fi
echo "========================================================"

ADJUSTMENTS_FILE="$(mktemp)"
trap 'rm -f "$ADJUSTMENTS_FILE"' EXIT
$CLI LRANGE "$ADJUSTMENTS_KEY" 0 -1 >"$ADJUSTMENTS_FILE"

FIELDS="$($CLI HKEYS "$POSITIONS_KEY" | grep -E '^(gate_futures|okx_futures)\|' | sort || true)"
if [[ -z "$FIELDS" ]]; then
    echo "没有 gate_futures/okx_futures 的仓位记录，不需要迁移。"
    exit 0
fi

pending=0
while IFS= read -r field <&3; do
    [[ -z "$field" ]] && continue

    if [[ "$($CLI HEXISTS "$BACKUP_KEY" "$field")" == "1" ]]; then
        echo "$field: 已迁移过(备份已存在)，跳过"
        continue
    fi

    json="$($CLI HGET "$POSITIONS_KEY" "$field")"
    venue="$(echo "$json" | jq -r '.venue')"
    base="$(echo "$json" | jq -r '.symbol.base')"
    quote="$(echo "$json" | jq -r '.symbol.quote')"
    net_qty="$(echo "$json" | jq -r '.net_qty // "0"')"
    pending_qty="$(echo "$json" | jq -r '.pending_qty // "0"')"
    realized="$(echo "$json" | jq -r '.realized_pnl // "0"')"
    avg_price="$(echo "$json" | jq -r '.avg_price // "None"')"

    size="$(contract_size "$venue" "$base" "$quote")"
    if [[ -z "$size" || "$(calc "$size > 0")" != "1" ]]; then
        echo "[错误] $field 查不到有效面值(得到 '$size')，中止" >&2
        exit 1
    fi

    adj_sum="0"
    while IFS= read -r amount; do
        [[ -z "$amount" ]] && continue
        adj_sum="$(calc "$adj_sum + $amount")"
    done < <(jq -r --arg v "$venue" --arg b "$base" --arg q "$quote" \
        'select(.venue == $v and .symbol.base == $b and .symbol.quote == $q) | .amount' "$ADJUSTMENTS_FILE")

    trade_pnl="$(calc "$realized - $adj_sum")"
    new_net_qty="$(calc "$net_qty * $size")"
    new_pending_qty="$(calc "$pending_qty * $size")"
    new_trade_pnl="$(calc "$trade_pnl * $size")"
    new_realized="$(calc "$new_trade_pnl + $adj_sum")"

    echo "$field: 1 张 = $size 币"
    echo "  net_qty       $net_qty 张 -> $new_net_qty 币   (avg_price $avg_price 不变)"
    [[ "$pending_qty" != "0" ]] && echo "  pending_qty   $pending_qty -> $new_pending_qty"
    echo "  realized_pnl  $realized -> $new_realized   (调整 $adj_sum 不换算；成交盈亏 $trade_pnl -> $new_trade_pnl)"

    if [[ "$CONFIRM" == 1 ]]; then
        now_ms="$(($(date +%s) * 1000))"
        new_json="$(echo "$json" | jq -c \
            --arg n "$new_net_qty" --arg p "$new_pending_qty" --arg r "$new_realized" --argjson ts "$now_ms" \
            '.net_qty = $n | .pending_qty = $p | .realized_pnl = $r | .updated_at_ms = $ts')"
        # 读和写之间有进程改过这条记录就不写，避免覆盖掉新成交
        if [[ "$($CLI HGET "$POSITIONS_KEY" "$field")" != "$json" ]]; then
            echo "[错误] $field 在预览之后被改动过(cross-funding 没停？)，中止" >&2
            exit 1
        fi
        if [[ "$($CLI HSETNX "$BACKUP_KEY" "$field" "$json")" != "1" ]]; then
            echo "[错误] $field 备份已存在，可能被并发迁移过，中止" >&2
            exit 1
        fi
        $CLI HSET "$POSITIONS_KEY" "$field" "$new_json" >/dev/null
        echo "  已写入: $($CLI HGET "$POSITIONS_KEY" "$field")"
    else
        pending=$((pending + 1))
    fi
done 3<<<"$FIELDS"

echo "========================================================"
if [[ "$CONFIRM" == 1 ]]; then
    echo "完成。原始记录备份在 $BACKUP_KEY。"
elif [[ "$pending" -gt 0 ]]; then
    echo "以上 $pending 条尚未写入。把 net_qty 和交易所界面显示的持仓核对一致后，停掉 cross-funding，加 --confirm 重新执行。"
fi
