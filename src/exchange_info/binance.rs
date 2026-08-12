use anyhow::Context;
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as base64_engine;
use ring::signature::Ed25519KeyPair;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::market_data::now_ms;
use crate::types::{Symbol, Venue};

use super::ExchangeInfoProvider;
use super::types::TradingFee;

const SPOT_MAINNET_HOST: &str = "https://api.binance.com";
const SPOT_TESTNET_HOST: &str = "https://testnet.binance.vision";
const FUTURES_MAINNET_HOST: &str = "https://fapi.binance.com";
const FUTURES_TESTNET_HOST: &str = "https://testnet.binancefuture.com";
const RECV_WINDOW_MS: u64 = 5_000;

/// 币安"基础信息"(现货 + USDT-M 永续合约)客户端：查询账户实际手续费率、
/// 列出可交易的 USDT 计价交易对。签名方式和 `order::binance`/`wallet::binance`
/// 一致，用 Ed25519，凭证也复用同一套环境变量(同一个 API Key 上勾选现货+合约
/// 权限即可，两个市场共用一把 key)。
pub struct BinanceExchangeInfoProvider {
    venue: Venue,
    api_key: String,
    key_pair: Ed25519KeyPair,
    spot_host: &'static str,
    futures_host: &'static str,
    http: reqwest::Client,
}

impl BinanceExchangeInfoProvider {
    pub fn new(venue: Venue, api_key: String, private_key_pem: &str, testnet: bool, proxy: Option<&str>) -> anyhow::Result<Self> {
        let key_pair = load_ed25519_key(private_key_pem)?;
        let http = build_http_client(proxy)?;
        Ok(Self {
            venue,
            api_key,
            key_pair,
            spot_host: if testnet { SPOT_TESTNET_HOST } else { SPOT_MAINNET_HOST },
            futures_host: if testnet { FUTURES_TESTNET_HOST } else { FUTURES_MAINNET_HOST },
            http,
        })
    }

    /// 从环境变量读取凭证并构造实例:`BINANCE_API_KEY` +
    /// `BINANCE_API_SECRET`(完整 PEM 文本)，和 `order::binance`/`wallet::binance` 同一套。
    pub fn from_env(venue: Venue, testnet: bool, proxy: Option<&str>) -> anyhow::Result<Self> {
        let api_key = std::env::var("BINANCE_API_KEY").context("BINANCE_API_KEY not set")?;
        let private_key_pem = std::env::var("BINANCE_API_SECRET").context("BINANCE_API_SECRET not set")?;
        Self::new(venue, api_key, &private_key_pem, testnet, proxy)
    }

    /// 对参数做签名并发起一次已签名请求(query string 里带 timestamp/recvWindow/signature)。
    async fn signed_request(
        &self,
        host: &str,
        path: &str,
        mut params: Vec<(String, String)>,
    ) -> anyhow::Result<String> {
        params.push(("timestamp".to_string(), now_ms().to_string()));
        params.push(("recvWindow".to_string(), RECV_WINDOW_MS.to_string()));
        let query = build_query_string(&params);
        let signature = sign_ed25519(&self.key_pair, &query);
        let url = format!("{host}{path}?{query}&signature={signature}");

        let resp = self
            .http
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .context("binance exchange_info request failed")?;
        resp.text().await.context("failed to read binance exchange_info response body")
    }

    /// 不需要签名的公开接口请求，用于查询交易对列表。
    async fn public_request(&self, host: &str, path: &str, params: Vec<(String, String)>) -> anyhow::Result<String> {
        let query = build_query_string(&params);
        let url = if query.is_empty() {
            format!("{host}{path}")
        } else {
            format!("{host}{path}?{query}")
        };
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context("binance exchange_info public request failed")?;
        resp.text().await.context("failed to read binance exchange_info public response body")
    }

    fn binance_symbol(symbol: &Symbol) -> String {
        format!("{}{}", symbol.base, symbol.quote).to_ascii_uppercase()
    }
}

#[async_trait]
impl ExchangeInfoProvider for BinanceExchangeInfoProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    async fn spot_trading_fee(&self, symbol: &Symbol) -> anyhow::Result<TradingFee> {
        let params = vec![("symbol".to_string(), Self::binance_symbol(symbol))];
        let text = self.signed_request(self.spot_host, "/sapi/v1/asset/tradeFee", params).await?;
        parse_spot_trading_fee(&text, symbol)
    }

    async fn perpetual_trading_fee(&self, symbol: &Symbol) -> anyhow::Result<TradingFee> {
        let params = vec![("symbol".to_string(), Self::binance_symbol(symbol))];
        let text = self
            .signed_request(self.futures_host, "/fapi/v1/commissionRate", params)
            .await?;
        parse_futures_trading_fee(&text)
    }

    async fn usdt_spot_symbols(&self) -> anyhow::Result<Vec<Symbol>> {
        let text = self.public_request(self.spot_host, "/api/v3/exchangeInfo", Vec::new()).await?;
        parse_usdt_spot_symbols(&text)
    }

    async fn usdt_perpetual_symbols(&self) -> anyhow::Result<Vec<Symbol>> {
        let text = self
            .public_request(self.futures_host, "/fapi/v1/exchangeInfo", Vec::new())
            .await?;
        parse_usdt_perpetual_symbols(&text)
    }
}

fn build_http_client(proxy: Option<&str>) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder();
    if let Some(proxy) = proxy {
        let proxy = reqwest::Proxy::all(format!("http://{proxy}")).context("invalid proxy address")?;
        builder = builder.proxy(proxy);
    }
    builder.build().context("failed to build binance http client")
}

/// 按插入顺序拼接 `k=v&k=v...`，签名必须覆盖和实际发送完全一致的 query string。
fn build_query_string(params: &[(String, String)]) -> String {
    params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// 解析 PKCS8 PEM 文本，交给 `Ed25519KeyPair::from_pkcs8_maybe_unchecked` 加载私钥。
/// 用 `_maybe_unchecked` 而不是 `from_pkcs8`：兼容 `openssl genpkey` 产出的不带
/// 内嵌公钥的 PKCS8 v1 格式，详见 `order::binance` 里的同名函数说明。
fn load_ed25519_key(pem: &str) -> anyhow::Result<Ed25519KeyPair> {
    let der = parse_pem_pkcs8(pem)?;
    Ed25519KeyPair::from_pkcs8_maybe_unchecked(&der)
        .map_err(|err| anyhow::anyhow!("invalid ed25519 pkcs8 key: {err}"))
}

fn parse_pem_pkcs8(pem: &str) -> anyhow::Result<Vec<u8>> {
    let body: String = pem
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with("-----"))
        .collect();
    base64_engine.decode(body).context("failed to base64-decode PEM body")
}

/// 对 payload 做 Ed25519 签名，返回 base64 编码结果。
fn sign_ed25519(key_pair: &Ed25519KeyPair, payload: &str) -> String {
    let signature = key_pair.sign(payload.as_bytes());
    base64_engine.encode(signature.as_ref())
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    code: i64,
    msg: String,
}

#[derive(Debug, Deserialize)]
struct TradeFeeEntry {
    #[serde(rename = "makerCommission")]
    maker_commission: Decimal,
    #[serde(rename = "takerCommission")]
    taker_commission: Decimal,
}

/// 币安现货 `/sapi/v1/asset/tradeFee` 返回的手续费是小数比例(如 "0.001000"
/// 表示 0.1%)，换算成 bps 要乘以 10000。
fn parse_spot_trading_fee(text: &str, symbol: &Symbol) -> anyhow::Result<TradingFee> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance error {}: {}", err.code, err.msg);
    }
    let entries: Vec<TradeFeeEntry> = serde_json::from_str(text)
        .with_context(|| format!("failed to parse binance tradeFee response, raw body: {text}"))?;
    let entry = entries
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("binance tradeFee returned no entry for {symbol}"))?;

    let bps_multiplier = Decimal::from(10_000);
    Ok(TradingFee {
        maker_bps: entry.maker_commission * bps_multiplier,
        taker_bps: entry.taker_commission * bps_multiplier,
    })
}

#[derive(Debug, Deserialize)]
struct CommissionRateResponse {
    #[serde(rename = "makerCommissionRate")]
    maker_commission_rate: Decimal,
    #[serde(rename = "takerCommissionRate")]
    taker_commission_rate: Decimal,
}

/// 币安 U 本位合约 `/fapi/v1/commissionRate` 返回的也是小数比例，换算规则
/// 和现货一致。
fn parse_futures_trading_fee(text: &str) -> anyhow::Result<TradingFee> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance futures error {}: {}", err.code, err.msg);
    }
    let resp: CommissionRateResponse = serde_json::from_str(text)
        .with_context(|| format!("failed to parse binance commissionRate response, raw body: {text}"))?;

    let bps_multiplier = Decimal::from(10_000);
    Ok(TradingFee {
        maker_bps: resp.maker_commission_rate * bps_multiplier,
        taker_bps: resp.taker_commission_rate * bps_multiplier,
    })
}

#[derive(Debug, Deserialize)]
struct SpotExchangeInfoResponse {
    symbols: Vec<SpotSymbolInfo>,
}

#[derive(Debug, Deserialize)]
struct SpotSymbolInfo {
    status: String,
    #[serde(rename = "baseAsset")]
    base_asset: String,
    #[serde(rename = "quoteAsset")]
    quote_asset: String,
}

fn parse_usdt_spot_symbols(text: &str) -> anyhow::Result<Vec<Symbol>> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance error {}: {}", err.code, err.msg);
    }
    let resp: SpotExchangeInfoResponse =
        serde_json::from_str(text).context("failed to parse binance spot exchangeInfo response")?;
    Ok(resp
        .symbols
        .into_iter()
        .filter(|s| s.status == "TRADING" && s.quote_asset.eq_ignore_ascii_case("USDT"))
        .map(|s| Symbol::new(s.base_asset, s.quote_asset))
        .collect())
}

#[derive(Debug, Deserialize)]
struct FuturesExchangeInfoResponse {
    symbols: Vec<FuturesSymbolInfo>,
}

#[derive(Debug, Deserialize)]
struct FuturesSymbolInfo {
    status: String,
    #[serde(rename = "contractType")]
    contract_type: String,
    #[serde(rename = "baseAsset")]
    base_asset: String,
    #[serde(rename = "quoteAsset")]
    quote_asset: String,
}

fn parse_usdt_perpetual_symbols(text: &str) -> anyhow::Result<Vec<Symbol>> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance futures error {}: {}", err.code, err.msg);
    }
    let resp: FuturesExchangeInfoResponse =
        serde_json::from_str(text).context("failed to parse binance futures exchangeInfo response")?;
    Ok(resp
        .symbols
        .into_iter()
        .filter(|s| s.status == "TRADING" && s.contract_type == "PERPETUAL" && s.quote_asset.eq_ignore_ascii_case("USDT"))
        .map(|s| Symbol::new(s.base_asset, s.quote_asset))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::rand::SystemRandom;
    use ring::signature::{KeyPair, UnparsedPublicKey};

    fn generate_test_pem() -> (String, Ed25519KeyPair) {
        let rng = SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).expect("generate pkcs8");
        let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).expect("load pkcs8");
        let body = base64_engine.encode(pkcs8.as_ref());
        let pem = format!("-----BEGIN PRIVATE KEY-----\n{body}\n-----END PRIVATE KEY-----\n");
        (pem, key_pair)
    }

    #[test]
    fn loads_ed25519_key_from_pem_roundtrip() {
        let (pem, _) = generate_test_pem();
        let loaded = load_ed25519_key(&pem).expect("should load key from PEM");
        assert_eq!(loaded.public_key().as_ref().len(), 32);
    }

    #[test]
    fn signs_payload_verifiable_by_public_key() {
        let (pem, key_pair) = generate_test_pem();
        let loaded = load_ed25519_key(&pem).expect("should load key from PEM");

        let payload = "symbol=BTCUSDT&timestamp=1700000000000";
        let signature_b64 = sign_ed25519(&loaded, payload);
        let signature = base64_engine.decode(signature_b64).expect("valid base64 signature");

        let public_key = UnparsedPublicKey::new(&ring::signature::ED25519, key_pair.public_key().as_ref());
        public_key
            .verify(payload.as_bytes(), &signature)
            .expect("signature should verify against the same keypair's public key");
    }

    #[test]
    fn builds_query_string_in_insertion_order() {
        let params = vec![
            ("symbol".to_string(), "BTCUSDT".to_string()),
            ("timestamp".to_string(), "123".to_string()),
        ];
        assert_eq!(build_query_string(&params), "symbol=BTCUSDT&timestamp=123");
    }

    #[test]
    fn parses_spot_trading_fee_response() {
        let text = r#"[{"symbol":"BTCUSDT","makerCommission":"0.001000","takerCommission":"0.001000"}]"#;
        let fee = parse_spot_trading_fee(text, &Symbol::new("BTC", "USDT")).expect("should parse");
        assert_eq!(fee.maker_bps, Decimal::from(10));
        assert_eq!(fee.taker_bps, Decimal::from(10));
    }

    #[test]
    fn parse_spot_trading_fee_surfaces_error_response() {
        let text = r#"{"code":-2014,"msg":"API-key format invalid."}"#;
        let err = parse_spot_trading_fee(text, &Symbol::new("BTC", "USDT")).unwrap_err();
        assert!(err.to_string().contains("-2014"));
    }

    #[test]
    fn parses_futures_trading_fee_response() {
        let text = r#"{"symbol":"BTCUSDT","makerCommissionRate":"0.0000200","takerCommissionRate":"0.0004000"}"#;
        let fee = parse_futures_trading_fee(text).expect("should parse");
        assert_eq!(fee.maker_bps, "0.2".parse().unwrap());
        assert_eq!(fee.taker_bps, Decimal::from(4));
    }

    #[test]
    fn parse_futures_trading_fee_surfaces_error_response() {
        let text = r#"{"code":-1121,"msg":"Invalid symbol."}"#;
        let err = parse_futures_trading_fee(text).unwrap_err();
        assert!(err.to_string().contains("-1121"));
    }

    #[test]
    fn parses_usdt_spot_symbols_filters_by_quote_and_status() {
        let text = r#"{
            "symbols": [
                {"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT"},
                {"symbol":"ETHBTC","status":"TRADING","baseAsset":"ETH","quoteAsset":"BTC"},
                {"symbol":"OLDUSDT","status":"BREAK","baseAsset":"OLD","quoteAsset":"USDT"}
            ]
        }"#;
        let symbols = parse_usdt_spot_symbols(text).expect("should parse");
        assert_eq!(symbols, vec![Symbol::new("BTC", "USDT")]);
    }

    #[test]
    fn parses_usdt_perpetual_symbols_filters_by_contract_type_quote_and_status() {
        let text = r#"{
            "symbols": [
                {"symbol":"BTCUSDT","status":"TRADING","contractType":"PERPETUAL","baseAsset":"BTC","quoteAsset":"USDT"},
                {"symbol":"BTCUSD_PERP","status":"TRADING","contractType":"PERPETUAL","baseAsset":"BTC","quoteAsset":"USD"},
                {"symbol":"BTCUSDT_240628","status":"TRADING","contractType":"CURRENT_QUARTER","baseAsset":"BTC","quoteAsset":"USDT"},
                {"symbol":"OLDUSDT","status":"SETTLING","contractType":"PERPETUAL","baseAsset":"OLD","quoteAsset":"USDT"}
            ]
        }"#;
        let symbols = parse_usdt_perpetual_symbols(text).expect("should parse");
        assert_eq!(symbols, vec![Symbol::new("BTC", "USDT")]);
    }

    #[test]
    fn parse_usdt_spot_symbols_surfaces_error_response() {
        let text = r#"{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}"#;
        let err = parse_usdt_spot_symbols(text).unwrap_err();
        assert!(err.to_string().contains("-2015"));
    }
}
