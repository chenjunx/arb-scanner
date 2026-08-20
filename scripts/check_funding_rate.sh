#!/usr/bin/env bash
# 查 Binance 合约的历史资金费率 (公开接口，不需要 API key/签名)。
# 用来核对 accounting::FundingFeeTracker 记的 realized_pnl 调整是不是真的对：
# 拿到的 fundingRate 乘以持仓名义价值，应该约等于 income 接口里那笔 FUNDING_FEE。
#
# 用法:
#   ./check_funding_rate.sh APEUSDT MANAUSDT
#   ./check_funding_rate.sh --start "2026-08-19 00:00:00" --end "2026-08-20 06:00:00" APEUSDT
#   BASE_URL=https://testnet.binancefuture.com ./check_funding_rate.sh APEUSDT   # 查 testnet
#
# 依赖: curl；有 jq 会格式化输出，没有就打印原始 JSON。

set -euo pipefail

BASE_URL="${BASE_URL:-https://fapi.binance.com}"
START=""
END=""
SYMBOLS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --start)
            START="$2"
            shift 2
            ;;
        --end)
            END="$2"
            shift 2
            ;;
        *)
            SYMBOLS+=("$1")
            shift
            ;;
    esac
done

if [[ ${#SYMBOLS[@]} -eq 0 ]]; then
    echo "usage: $0 [--start '2026-08-19 00:00:00'] [--end '2026-08-20 06:00:00'] SYMBOL [SYMBOL...]" >&2
    exit 1
fi

# 默认查最近 48 小时，够覆盖最近几次结算点 (每 8 小时一次)。
to_ms() { date -u -d "$1" +%s%3N; }
START_MS="$([[ -n "$START" ]] && to_ms "$START" || date -u -d '48 hours ago' +%s%3N)"
END_MS="$(([[ -n "$END" ]] && to_ms "$END") || date -u +%s%3N)"

echo "查询窗口: $(date -u -d @$((START_MS/1000)) '+%Y-%m-%d %H:%M:%S UTC') ~ $(date -u -d @$((END_MS/1000)) '+%Y-%m-%d %H:%M:%S UTC')"
echo

for symbol in "${SYMBOLS[@]}"; do
    echo "== $symbol =="
    url="${BASE_URL}/fapi/v1/fundingRate?symbol=${symbol}&startTime=${START_MS}&endTime=${END_MS}&limit=100"
    resp="$(curl -sS "$url")"
    if command -v jq >/dev/null 2>&1; then
        echo "$resp" | jq -r '.[] | "\((.fundingTime|tonumber)/1000 | strftime("%Y-%m-%d %H:%M:%S UTC")) rate=\(.fundingRate) mark_price=\(.markPrice)"' \
            2>/dev/null || echo "$resp" | jq .
    else
        echo "$resp"
    fi
    echo
done
