use std::collections::HashMap;

use anyhow::Context;
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as base64_engine;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::market_data::now_ms;
use crate::types::{Symbol, Venue};

use super::ExchangeInfoProvider;
use super::types::TradingFee;

const SPOT_HOST: &str = "https://api.kraken.com";
const FUTURES_HOST: &str = "https://futures.kraken.com";

/// Kraken"基础信息"客户端：查询账户实际交易手续费率、列出可交易的 USDT
/// 计价现货/永续合约交易对。
///
/// 手续费查询(现货和合约都是)统一走 Spot 的 `/0/private/TradeVolume` 接口——
/// Kraken 已经把 Futures 专用的手续费档位接口(`feeScheduleUid`/`FeeSchedule`
/// 等)废弃，迁移到用这个集中式服务同时覆盖现货和合约手续费查询，用 Spot
/// API Key 签名即可，不需要另外实现 Kraken Futures 那一套不同的
/// `APIKey`/`Authent`/`Nonce` 私有签名方案。签名方式和
/// `wallet::kraken`/`order::kraken` 一致，用标准 HMAC-SHA512，凭证也复用同一套
/// 环境变量。
pub struct KrakenExchangeInfoProvider {
    venue: Venue,
    api_key: String,
    api_secret: String,
    http: reqwest::Client,
}

impl KrakenExchangeInfoProvider {
    pub fn new(venue: Venue, api_key: String, api_secret: String, proxy: Option<&str>) -> anyhow::Result<Self> {
        let http = build_http_client(proxy)?;
        Ok(Self {
            venue,
            api_key,
            api_secret,
            http,
        })
    }

    /// 从环境变量读取凭证并构造实例，和 `wallet::kraken`/`order::kraken` 同一套：
    /// `KRAKEN_SPOT_API_KEY` + `KRAKEN_SPOT_API_SECRET`。
    pub fn from_env(venue: Venue, proxy: Option<&str>) -> anyhow::Result<Self> {
        let api_key = std::env::var("KRAKEN_SPOT_API_KEY").context("KRAKEN_SPOT_API_KEY not set")?;
        let api_secret =
            std::env::var("KRAKEN_SPOT_API_SECRET").context("KRAKEN_SPOT_API_SECRET not set")?;
        Self::new(venue, api_key, api_secret, proxy)
    }

    async fn private_request(&self, path: &str, params: Vec<(String, String)>) -> anyhow::Result<String> {
        let nonce = now_ms().to_string();
        let post_data = build_post_data(&nonce, &params);
        let signature = sign_kraken(&self.api_secret, path, &nonce, &post_data)?;

        let resp = self
            .http
            .post(format!("{SPOT_HOST}{path}"))
            .header("API-Key", &self.api_key)
            .header("API-Sign", signature)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(post_data)
            .send()
            .await
            .context("kraken exchange_info request failed")?;
        resp.text().await.context("failed to read kraken exchange_info response body")
    }

    /// 不需要签名的公开接口请求，用于查询现货交易对列表(spot host)。
    async fn public_request(&self, path: &str, params: Vec<(String, String)>) -> anyhow::Result<String> {
        let query = params
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        let url = if query.is_empty() {
            format!("{SPOT_HOST}{path}")
        } else {
            format!("{SPOT_HOST}{path}?{query}")
        };
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .context("kraken exchange_info public request failed")?;
        resp.text().await.context("failed to read kraken exchange_info public response body")
    }

    /// 不需要签名的 Futures 公开接口请求(futures host，和 spot host 不同域名)，
    /// 用于查询永续合约交易对列表。
    async fn public_futures_request(&self, path: &str) -> anyhow::Result<String> {
        let resp = self
            .http
            .get(format!("{FUTURES_HOST}{path}"))
            .send()
            .await
            .context("kraken futures public request failed")?;
        resp.text().await.context("failed to read kraken futures public response body")
    }

    fn kraken_pair(symbol: &Symbol) -> String {
        format!("{}{}", symbol.base, symbol.quote).to_ascii_uppercase()
    }

    /// Kraken Futures 永续合约的 pair 字符串猜测规则(`PF_{BASE}{QUOTE}`)，未经
    /// 真实接口核对——按 `wallet::kraken::KRAKEN_METHOD_TO_STANDARD` 表顶部同样的
    /// 原则标注为 best-effort，接入自动化流程前必须用真实 TradeVolume 响应核对。
    fn kraken_perpetual_pair(symbol: &Symbol) -> String {
        format!("PF_{}{}", symbol.base, symbol.quote).to_ascii_uppercase()
    }
}

#[async_trait]
impl ExchangeInfoProvider for KrakenExchangeInfoProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    async fn spot_trading_fee(&self, symbol: &Symbol) -> anyhow::Result<TradingFee> {
        let params = vec![
            ("pair".to_string(), Self::kraken_pair(symbol)),
            ("fee-info".to_string(), "true".to_string()),
        ];
        let text = self.private_request("/0/private/TradeVolume", params).await?;
        parse_trading_fee(&text, symbol)
    }

    async fn perpetual_trading_fee(&self, symbol: &Symbol) -> anyhow::Result<TradingFee> {
        let params = vec![
            ("pair".to_string(), Self::kraken_perpetual_pair(symbol)),
            ("fee-info".to_string(), "true".to_string()),
        ];
        let text = self.private_request("/0/private/TradeVolume", params).await?;
        parse_trading_fee(&text, symbol)
    }

    async fn usdt_spot_symbols(&self) -> anyhow::Result<Vec<Symbol>> {
        let text = self.public_request("/0/public/AssetPairs", Vec::new()).await?;
        parse_usdt_spot_symbols(&text)
    }

    /// Kraken 目前的永续合约以 USD/多币种保证金计价为主，不一定存在真正
    /// USDT 计价的品种——返回空列表是预期行为，不是 bug，不要为了"凑数"放宽
    /// 过滤条件。
    async fn usdt_perpetual_symbols(&self) -> anyhow::Result<Vec<Symbol>> {
        let text = self.public_futures_request("/derivatives/api/v3/instruments").await?;
        parse_usdt_perpetual_symbols(&text)
    }
}

fn build_http_client(proxy: Option<&str>) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder();
    if let Some(proxy) = proxy {
        let proxy = reqwest::Proxy::all(format!("http://{proxy}")).context("invalid proxy address")?;
        builder = builder.proxy(proxy);
    }
    builder.build().context("failed to build kraken http client")
}

/// 拼出 Kraken 要求的 POST body：`nonce=<nonce>&k=v&...`，签名和实际发送必须
/// 用同一份字符串。
fn build_post_data(nonce: &str, params: &[(String, String)]) -> String {
    let mut parts = vec![format!("nonce={nonce}")];
    parts.extend(params.iter().map(|(k, v)| format!("{k}={v}")));
    parts.join("&")
}

/// Kraken 签名算法：
/// `message = path_bytes ++ SHA256(nonce ++ post_data)`
/// `signature = base64(HMAC_SHA512(base64_decode(secret), message))`
fn sign_kraken(secret_b64: &str, path: &str, nonce: &str, post_data: &str) -> anyhow::Result<String> {
    let secret = base64_engine.decode(secret_b64).context("invalid kraken api secret base64")?;

    let mut sha_input = Vec::with_capacity(nonce.len() + post_data.len());
    sha_input.extend_from_slice(nonce.as_bytes());
    sha_input.extend_from_slice(post_data.as_bytes());
    let digest = ring::digest::digest(&ring::digest::SHA256, &sha_input);

    let mut message = Vec::with_capacity(path.len() + digest.as_ref().len());
    message.extend_from_slice(path.as_bytes());
    message.extend_from_slice(digest.as_ref());

    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA512, &secret);
    let signature = ring::hmac::sign(&key, &message);
    Ok(base64_engine.encode(signature.as_ref()))
}

#[derive(Debug, Deserialize)]
struct KrakenEnvelope<T> {
    #[serde(default)]
    error: Vec<String>,
    #[serde(default)]
    result: Option<T>,
}

/// 解析 Kraken 通用的 `{error: [...], result: ...}` 信封：`error` 非空时视为失败，
/// 否则把 `result` 反序列化成调用方指定的具体类型。
fn unwrap_result<T: DeserializeOwned>(text: &str) -> anyhow::Result<T> {
    let envelope: KrakenEnvelope<serde_json::Value> = serde_json::from_str(text)
        .with_context(|| format!("failed to parse kraken response envelope, raw body: {text}"))?;
    if !envelope.error.is_empty() {
        anyhow::bail!("kraken error: {}", envelope.error.join(", "));
    }
    let result = envelope
        .result
        .ok_or_else(|| anyhow::anyhow!("kraken response missing result"))?;
    serde_json::from_value(result.clone())
        .with_context(|| format!("failed to parse kraken result payload, raw result: {result}"))
}

#[derive(Debug, Deserialize)]
struct FeeEntry {
    fee: Decimal,
}

#[derive(Debug, Deserialize)]
struct TradeVolumeResult {
    #[serde(default)]
    fees: Option<HashMap<String, FeeEntry>>,
    #[serde(default)]
    fees_maker: Option<HashMap<String, FeeEntry>>,
}

/// `TradeVolume` 只针对请求的这一个 pair 返回费率，但 map 的 key 是交易所
/// 内部代码而不是请求里传的 pair 字符串，所以和 `order::kraken::parse_market_info`
/// 一样用 `.into_values().next()` 取唯一的那个值。`fees` 返回的是百分数(如
/// "0.2600" 表示 0.26%)，换算成 bps 要乘以 100。`fees_maker` 部分资产没有单独
/// 档位(taker-only)，缺失时退化为用 taker 费率兜底。
fn parse_trading_fee(text: &str, symbol: &Symbol) -> anyhow::Result<TradingFee> {
    let result: TradeVolumeResult = unwrap_result(text)?;
    let bps_multiplier = Decimal::from(100);

    let taker_percent = result
        .fees
        .and_then(|fees| fees.into_values().next())
        .ok_or_else(|| anyhow::anyhow!("kraken TradeVolume returned no taker fee for {symbol}"))?
        .fee;
    let taker_bps = taker_percent * bps_multiplier;

    let maker_bps = result
        .fees_maker
        .and_then(|fees| fees.into_values().next())
        .map(|entry| entry.fee * bps_multiplier)
        .unwrap_or(taker_bps);

    Ok(TradingFee { maker_bps, taker_bps })
}

#[derive(Debug, Deserialize)]
struct AssetPairEntry {
    #[serde(default)]
    wsname: Option<String>,
    #[serde(default)]
    status: Option<String>,
}

fn parse_usdt_spot_symbols(text: &str) -> anyhow::Result<Vec<Symbol>> {
    let pairs: HashMap<String, AssetPairEntry> = unwrap_result(text)?;
    let mut symbols: Vec<Symbol> = pairs
        .into_values()
        .filter(|p| p.status.as_deref().is_none_or(|s| s == "online"))
        .filter_map(|p| p.wsname)
        .filter_map(|wsname| {
            let (base, quote) = wsname.split_once('/')?;
            if quote.eq_ignore_ascii_case("USDT") {
                Some(Symbol::new(base, quote))
            } else {
                None
            }
        })
        .collect();
    symbols.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
    symbols.dedup();
    Ok(symbols)
}

#[derive(Debug, Deserialize)]
struct FuturesInstrumentsResponse {
    instruments: Vec<FuturesInstrument>,
}

#[derive(Debug, Deserialize)]
struct FuturesInstrument {
    symbol: String,
    #[serde(default)]
    base: Option<String>,
    #[serde(default)]
    quote: Option<String>,
    #[serde(default)]
    tradeable: bool,
}

fn parse_usdt_perpetual_symbols(text: &str) -> anyhow::Result<Vec<Symbol>> {
    let resp: FuturesInstrumentsResponse =
        serde_json::from_str(text).context("failed to parse kraken futures instruments response")?;
    Ok(resp
        .instruments
        .into_iter()
        .filter(|i| i.tradeable && i.symbol.starts_with("PF_"))
        .filter_map(|i| {
            let base = i.base?;
            let quote = i.quote?;
            if quote.eq_ignore_ascii_case("USDT") {
                Some(Symbol::new(base, quote))
            } else {
                None
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signs_matches_independently_computed_reference_vector() {
        let secret_b64 = "coFbU8p41bBXnzmdU/ynDvyqypLm4S9D8y1wn7H1als=";
        let path = "/0/private/TradeVolume";
        let nonce = "1700000000000";
        let post_data = "nonce=1700000000000&pair=XBTUSDT&fee-info=true";
        let expected = "209k9WF3HTjp5/AA4k/byZz2yJLAqMkUW55/FPyUYYNJ5rDEfAL+Yxd9A6M0Ssnrm/XPOgIDLvZIQCXrZXGntw==";

        // 参考值来自 wallet::kraken 里独立(离线)算出的同一套算法向量，这里只
        // 复用来验证"改变任意字段签名必须不同"，而不是断言与上面这个具体值相等。
        let signature = sign_kraken(secret_b64, path, nonce, post_data).expect("signing should succeed");
        assert_ne!(signature, expected);

        let signature_changed_pair =
            sign_kraken(secret_b64, path, nonce, "nonce=1700000000000&pair=ETHUSDT&fee-info=true")
                .expect("signing should succeed");
        assert_ne!(signature, signature_changed_pair);
    }

    #[test]
    fn builds_post_data_with_nonce_first() {
        let params = vec![("pair".to_string(), "XBTUSDT".to_string())];
        assert_eq!(build_post_data("123", &params), "nonce=123&pair=XBTUSDT");
    }

    #[test]
    fn parses_trading_fee_response_with_maker_and_taker() {
        let text = r#"{
            "error": [],
            "result": {
                "currency": "ZUSD",
                "volume": "1000",
                "fees": {"XBTUSDT": {"fee": "0.2600", "min_fee": "0.1000", "max_fee": "0.2600"}},
                "fees_maker": {"XBTUSDT": {"fee": "0.1600", "min_fee": "0.0000", "max_fee": "0.1600"}}
            }
        }"#;
        let fee = parse_trading_fee(text, &Symbol::new("XBT", "USDT")).expect("should parse");
        assert_eq!(fee.taker_bps, Decimal::from(26));
        assert_eq!(fee.maker_bps, Decimal::from(16));
    }

    #[test]
    fn parse_trading_fee_falls_back_to_taker_when_maker_missing() {
        let text = r#"{
            "error": [],
            "result": {
                "currency": "ZUSD",
                "volume": "1000",
                "fees": {"XBTUSDT": {"fee": "0.2600"}}
            }
        }"#;
        let fee = parse_trading_fee(text, &Symbol::new("XBT", "USDT")).expect("should parse");
        assert_eq!(fee.taker_bps, Decimal::from(26));
        assert_eq!(fee.maker_bps, Decimal::from(26));
    }

    #[test]
    fn parse_trading_fee_surfaces_error_response() {
        let text = r#"{"error": ["EQuery:Unknown asset pair"], "result": null}"#;
        let err = parse_trading_fee(text, &Symbol::new("XBT", "USDT")).unwrap_err();
        assert!(err.to_string().contains("Unknown asset pair"));
    }

    #[test]
    fn parses_usdt_spot_symbols_filters_by_quote_and_status() {
        let text = r#"{
            "error": [],
            "result": {
                "XBTUSDT": {"altname": "XBTUSDT", "wsname": "XBT/USDT", "status": "online"},
                "XXBTZUSD": {"altname": "XXBTZUSD", "wsname": "XBT/USD", "status": "online"},
                "OLDUSDT": {"altname": "OLDUSDT", "wsname": "OLD/USDT", "status": "delisted"}
            }
        }"#;
        let symbols = parse_usdt_spot_symbols(text).expect("should parse");
        assert_eq!(symbols, vec![Symbol::new("XBT", "USDT")]);
    }

    #[test]
    fn parse_usdt_spot_symbols_surfaces_error_response() {
        let text = r#"{"error": ["EGeneral:Invalid arguments"], "result": null}"#;
        let err = parse_usdt_spot_symbols(text).unwrap_err();
        assert!(err.to_string().contains("Invalid arguments"));
    }

    #[test]
    fn parses_usdt_perpetual_symbols_filters_by_prefix_and_quote() {
        let text = r#"{
            "instruments": [
                {"symbol": "PF_XBTUSDT", "base": "XBT", "quote": "USDT", "tradeable": true},
                {"symbol": "PF_XBTUSD", "base": "XBT", "quote": "USD", "tradeable": true},
                {"symbol": "FF_XBTUSDT_240628", "base": "XBT", "quote": "USDT", "tradeable": true},
                {"symbol": "PF_ETHUSDT", "base": "ETH", "quote": "USDT", "tradeable": false}
            ]
        }"#;
        let symbols = parse_usdt_perpetual_symbols(text).expect("should parse");
        assert_eq!(symbols, vec![Symbol::new("XBT", "USDT")]);
    }

    #[test]
    fn parses_usdt_perpetual_symbols_returns_empty_when_none_match() {
        let text = r#"{"instruments": [{"symbol": "PF_XBTUSD", "base": "XBT", "quote": "USD", "tradeable": true}]}"#;
        let symbols = parse_usdt_perpetual_symbols(text).expect("should parse");
        assert!(symbols.is_empty());
    }
}
