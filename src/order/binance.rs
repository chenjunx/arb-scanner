use anyhow::Context;
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as base64_engine;
use ring::signature::Ed25519KeyPair;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::market_data::now_ms;
use crate::types::{Symbol, Venue};

use super::OrderProvider;
use super::types::{MarketInfo, MarketOrderRequest, OrderAmount, OrderResult, OrderSide, OrderStatus};

const MAINNET_HOST: &str = "https://api.binance.com";
const TESTNET_HOST: &str = "https://testnet.binance.vision";
const RECV_WINDOW_MS: u64 = 5_000;

/// 币安下单(执行层)客户端：查询交易对精度限制、提交市价单。签名方式和
/// `wallet::binance::BinanceWalletProvider` 一致，用 Ed25519，凭证也复用同一套
/// 环境变量(交易和提币通常用同一个 API Key，只是权限勾选不同)。
pub struct BinanceOrderProvider {
    venue: Venue,
    api_key: String,
    key_pair: Ed25519KeyPair,
    host: &'static str,
    http: reqwest::Client,
}

impl BinanceOrderProvider {
    pub fn new(venue: Venue, api_key: String, private_key_pem: &str, testnet: bool, proxy: Option<&str>) -> anyhow::Result<Self> {
        let key_pair = load_ed25519_key(private_key_pem)?;
        let http = build_http_client(proxy)?;
        Ok(Self {
            venue,
            api_key,
            key_pair,
            host: if testnet { TESTNET_HOST } else { MAINNET_HOST },
            http,
        })
    }

    /// 从环境变量读取凭证并构造实例，和 `wallet::binance::BinanceWalletProvider::from_env`
    /// 读取同一套变量：`BINANCE_API_KEY` + `BINANCE_API_SECRET`。
    pub fn from_env(venue: Venue, testnet: bool, proxy: Option<&str>) -> anyhow::Result<Self> {
        let api_key = std::env::var("BINANCE_API_KEY")
            .context("BINANCE_API_KEY not set")?;
        let private_key_pem = std::env::var("BINANCE_API_SECRET")
            .context("BINANCE_API_SECRET not set")?;
        Self::new(venue, api_key, &private_key_pem, testnet, proxy)
    }

    /// 对参数做签名并发起一次已签名请求(query string 里带 timestamp/recvWindow/signature)。
    async fn signed_request(
        &self,
        method: reqwest::Method,
        path: &str,
        mut params: Vec<(String, String)>,
    ) -> anyhow::Result<String> {
        params.push(("timestamp".to_string(), now_ms().to_string()));
        params.push(("recvWindow".to_string(), RECV_WINDOW_MS.to_string()));
        let query = build_query_string(&params);
        let signature = sign_ed25519(&self.key_pair, &query);
        let url = format!("{}{}?{}&signature={}", self.host, path, query, signature);

        let resp = self
            .http
            .request(method, &url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .context("binance order request failed")?;
        let text = resp.text().await.context("failed to read binance order response body")?;
        Ok(text)
    }

    /// 不需要签名的公开接口请求，用于查询交易对精度限制。
    async fn public_request(&self, path: &str, params: Vec<(String, String)>) -> anyhow::Result<String> {
        let query = build_query_string(&params);
        let url = format!("{}{}?{}", self.host, path, query);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context("binance public request failed")?;
        resp.text().await.context("failed to read binance public response body")
    }

    fn binance_symbol(symbol: &Symbol) -> String {
        format!("{}{}", symbol.base, symbol.quote).to_ascii_uppercase()
    }
}

#[async_trait]
impl OrderProvider for BinanceOrderProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    async fn market_info(&self, symbol: &Symbol) -> anyhow::Result<MarketInfo> {
        let params = vec![("symbol".to_string(), Self::binance_symbol(symbol))];
        let text = self.public_request("/api/v3/exchangeInfo", params).await?;
        parse_market_info(&text, symbol)
    }

    async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
        let mut params = vec![
            ("symbol".to_string(), Self::binance_symbol(&req.symbol)),
            ("side".to_string(), map_side(req.side).to_string()),
            ("type".to_string(), "MARKET".to_string()),
        ];
        match req.amount {
            OrderAmount::Base(quantity) => params.push(("quantity".to_string(), quantity.to_string())),
            OrderAmount::Quote(quote_amount) => params.push(("quoteOrderQty".to_string(), quote_amount.to_string())),
        }
        if let Some(client_order_id) = &req.client_order_id {
            params.push(("newClientOrderId".to_string(), client_order_id.clone()));
        }
        let text = self.signed_request(reqwest::Method::POST, "/api/v3/order", params).await?;
        parse_order_response(&text)
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

/// 解析 PKCS8 PEM 文本(过滤 BEGIN/END 行,拼接剩余 base64 并解码),交给
/// `Ed25519KeyPair::from_pkcs8` 加载私钥。
fn load_ed25519_key(pem: &str) -> anyhow::Result<Ed25519KeyPair> {
    let der = parse_pem_pkcs8(pem)?;
    Ed25519KeyPair::from_pkcs8(&der).map_err(|err| anyhow::anyhow!("invalid ed25519 pkcs8 key: {err}"))
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

fn map_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn map_status(status: &str) -> OrderStatus {
    match status {
        "FILLED" => OrderStatus::Filled,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" | "EXPIRED_IN_MATCH" => OrderStatus::Expired,
        _ => OrderStatus::New,
    }
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    code: i64,
    msg: String,
}

#[derive(Debug, Deserialize)]
struct ExchangeInfoResponse {
    symbols: Vec<SymbolInfo>,
}

#[derive(Debug, Deserialize)]
struct SymbolInfo {
    filters: Vec<serde_json::Value>,
}

fn parse_market_info(text: &str, symbol: &Symbol) -> anyhow::Result<MarketInfo> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance error {}: {}", err.code, err.msg);
    }
    let resp: ExchangeInfoResponse = serde_json::from_str(text).context("failed to parse binance exchangeInfo response")?;
    let info = resp
        .symbols
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("binance exchangeInfo returned no symbol for {symbol}"))?;

    let lot_size = info
        .filters
        .iter()
        .find(|f| f.get("filterType").and_then(|v| v.as_str()) == Some("LOT_SIZE"))
        .ok_or_else(|| anyhow::anyhow!("binance exchangeInfo missing LOT_SIZE filter for {symbol}"))?;

    let step_size: Decimal = lot_size
        .get("stepSize")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("binance LOT_SIZE filter missing stepSize for {symbol}"))?
        .parse()
        .context("failed to parse binance stepSize")?;
    let min_qty: Decimal = lot_size
        .get("minQty")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("binance LOT_SIZE filter missing minQty for {symbol}"))?
        .parse()
        .context("failed to parse binance minQty")?;

    Ok(MarketInfo {
        symbol: symbol.clone(),
        qty_step: step_size,
        min_qty,
    })
}

#[derive(Debug, Deserialize)]
struct OrderResponse {
    #[serde(rename = "orderId")]
    order_id: i64,
    status: String,
    #[serde(rename = "executedQty")]
    executed_qty: Decimal,
    #[serde(rename = "cummulativeQuoteQty")]
    cummulative_quote_qty: Decimal,
}

fn parse_order_response(text: &str) -> anyhow::Result<OrderResult> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance error {}: {}", err.code, err.msg);
    }
    let resp: OrderResponse = serde_json::from_str(text).context("failed to parse binance order response")?;
    let avg_price = (resp.executed_qty > Decimal::ZERO).then(|| resp.cummulative_quote_qty / resp.executed_qty);

    Ok(OrderResult {
        order_id: resp.order_id.to_string(),
        status: map_status(&resp.status),
        filled_qty: resp.executed_qty,
        avg_price,
    })
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

        let payload = "symbol=BTCUSDT&side=BUY&type=MARKET&quantity=0.01&timestamp=1700000000000";
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
            ("side".to_string(), "BUY".to_string()),
        ];
        assert_eq!(build_query_string(&params), "symbol=BTCUSDT&side=BUY");
    }

    #[test]
    fn maps_side_to_binance_string() {
        assert_eq!(map_side(OrderSide::Buy), "BUY");
        assert_eq!(map_side(OrderSide::Sell), "SELL");
    }

    #[test]
    fn maps_status_strings() {
        assert_eq!(map_status("FILLED"), OrderStatus::Filled);
        assert_eq!(map_status("PARTIALLY_FILLED"), OrderStatus::PartiallyFilled);
        assert_eq!(map_status("NEW"), OrderStatus::New);
        assert_eq!(map_status("REJECTED"), OrderStatus::Rejected);
        assert_eq!(map_status("EXPIRED"), OrderStatus::Expired);
    }

    #[test]
    fn parses_market_info_from_exchange_info() {
        let text = r#"{
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        {"filterType": "LOT_SIZE", "minQty": "0.00001000", "maxQty": "9000.00000000", "stepSize": "0.00001000"}
                    ]
                }
            ]
        }"#;
        let symbol = Symbol::new("BTC", "USDT");
        let info = parse_market_info(text, &symbol).expect("should parse");
        assert_eq!(info.qty_step, "0.00001000".parse().unwrap());
        assert_eq!(info.min_qty, "0.00001000".parse().unwrap());
    }

    #[test]
    fn parse_market_info_surfaces_error_response() {
        let text = r#"{"code":-1121,"msg":"Invalid symbol."}"#;
        let symbol = Symbol::new("BTC", "USDT");
        let err = parse_market_info(text, &symbol).unwrap_err();
        assert!(err.to_string().contains("-1121"));
    }

    #[test]
    fn parses_order_response_with_avg_price() {
        let text = r#"{
            "symbol": "BTCUSDT",
            "orderId": 28,
            "clientOrderId": "abc",
            "transactTime": 1507725176595,
            "price": "0.00000000",
            "origQty": "10.00000000",
            "executedQty": "10.00000000",
            "cummulativeQuoteQty": "1000.00000000",
            "status": "FILLED",
            "timeInForce": "GTC",
            "type": "MARKET",
            "side": "SELL"
        }"#;
        let result = parse_order_response(text).expect("should parse");
        assert_eq!(result.order_id, "28");
        assert_eq!(result.status, OrderStatus::Filled);
        assert_eq!(result.filled_qty, "10.00000000".parse().unwrap());
        assert_eq!(result.avg_price, Some("100".parse().unwrap()));
    }

    #[test]
    fn parse_order_response_surfaces_error_response() {
        let text = r#"{"code":-2010,"msg":"Account has insufficient balance for requested action."}"#;
        let err = parse_order_response(text).unwrap_err();
        assert!(err.to_string().contains("insufficient balance"));
    }
}
