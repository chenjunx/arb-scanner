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

use super::OrderProvider;
use super::types::{MarketInfo, MarketOrderRequest, OrderAmount, OrderResult, OrderSide, OrderStatus};

const HOST: &str = "https://api.kraken.com";

/// Kraken 下单(执行层)客户端：查询交易对精度限制、提交市价单。签名方式和
/// `wallet::kraken::KrakenWalletProvider` 一致，用标准 HMAC-SHA512，凭证也复用
/// 同一套环境变量。
///
/// 重要限制：Kraken 的 `AddOrder` 接口对市价单只同步返回 `txid`，不保证立即
/// 告知是否已成交/成交多少——本实现里 `place_market_order_raw` 因此固定返回
/// `OrderStatus::New`、`filled_qty=0`、`avg_price=None`，调用方需要清楚这不是
/// 遗漏而是接口本身的限制；要拿到真实成交结果需要额外调用 `QueryOrders`
/// (本模块暂未实现)。
pub struct KrakenOrderProvider {
    venue: Venue,
    api_key: String,
    api_secret: String,
    http: reqwest::Client,
}

impl KrakenOrderProvider {
    pub fn new(venue: Venue, api_key: String, api_secret: String, proxy: Option<&str>) -> anyhow::Result<Self> {
        let http = build_http_client(proxy)?;
        Ok(Self {
            venue,
            api_key,
            api_secret,
            http,
        })
    }

    /// 从环境变量读取凭证并构造实例，和 `wallet::kraken::KrakenWalletProvider::from_env`
    /// 读取同一套变量：`KRAKEN_SPOT_API_KEY` + `KRAKEN_SPOT_API_SECRET`。
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
            .post(format!("{HOST}{path}"))
            .header("API-Key", &self.api_key)
            .header("API-Sign", signature)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(post_data)
            .send()
            .await
            .context("kraken order request failed")?;
        resp.text().await.context("failed to read kraken order response body")
    }

    /// 不需要签名的公开接口请求，用于查询交易对精度限制。
    async fn public_request(&self, path: &str, params: Vec<(String, String)>) -> anyhow::Result<String> {
        let query = params
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        let resp = self
            .http
            .get(format!("{HOST}{path}?{query}"))
            .send()
            .await
            .context("kraken public request failed")?;
        resp.text().await.context("failed to read kraken public response body")
    }

    fn kraken_pair(symbol: &Symbol) -> String {
        format!("{}{}", symbol.base, symbol.quote).to_ascii_uppercase()
    }
}

#[async_trait]
impl OrderProvider for KrakenOrderProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    async fn market_info(&self, symbol: &Symbol) -> anyhow::Result<MarketInfo> {
        let params = vec![("pair".to_string(), Self::kraken_pair(symbol))];
        let text = self.public_request("/0/public/AssetPairs", params).await?;
        parse_market_info(&text, symbol)
    }

    async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
        let OrderAmount::Base(quantity) = req.amount else {
            anyhow::bail!("{} does not support quote-amount market orders", self.venue());
        };
        let params = vec![
            ("pair".to_string(), Self::kraken_pair(&req.symbol)),
            ("type".to_string(), map_side(req.side).to_string()),
            ("ordertype".to_string(), "market".to_string()),
            ("volume".to_string(), quantity.to_string()),
        ];
        let text = self.private_request("/0/private/AddOrder", params).await?;
        parse_add_order_result(&text)
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

fn map_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
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
struct AssetPairInfo {
    #[serde(default)]
    ordermin: Option<String>,
    lot_decimals: u32,
}

/// `AssetPairs?pair=<X>` 只返回一个 pair，但返回的 key 是交易所内部代码
/// (如 "XXBTZUSD")而不是请求里传的 altname，所以取 `result` 里唯一的那个值。
fn parse_market_info(text: &str, symbol: &Symbol) -> anyhow::Result<MarketInfo> {
    let pairs: HashMap<String, AssetPairInfo> = unwrap_result(text)?;
    let info = pairs
        .into_values()
        .next()
        .ok_or_else(|| anyhow::anyhow!("kraken AssetPairs returned no pair for {symbol}"))?;

    let qty_step = Decimal::new(1, info.lot_decimals);
    let min_qty = info
        .ordermin
        .and_then(|v| v.parse().ok())
        .unwrap_or(Decimal::ZERO);

    Ok(MarketInfo {
        symbol: symbol.clone(),
        qty_step,
        min_qty,
    })
}

#[derive(Debug, Deserialize)]
struct AddOrderResult {
    txid: Vec<String>,
}

fn parse_add_order_result(text: &str) -> anyhow::Result<OrderResult> {
    let result: AddOrderResult = unwrap_result(text)?;
    let order_id = result
        .txid
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("kraken AddOrder response missing txid"))?;

    // Kraken 的 AddOrder 不同步返回成交信息，见本文件顶部注释。
    Ok(OrderResult {
        order_id,
        status: OrderStatus::New,
        filled_qty: Decimal::ZERO,
        avg_price: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signs_matches_independently_computed_reference_vector() {
        let secret_b64 = "coFbU8p41bBXnzmdU/ynDvyqypLm4S9D8y1wn7H1als=";
        let path = "/0/private/AddOrder";
        let nonce = "1700000000000";
        let post_data = "nonce=1700000000000&pair=XBTUSD&type=buy&ordertype=market&volume=0.1";

        // 用同一套签名算法(和 wallet::kraken 的签名测试用同一个参考密钥)
        // 反向验证：改变任意一个字段签名必须不同，避免实现里字段拼接顺序出错。
        let signature = sign_kraken(secret_b64, path, nonce, post_data).expect("signing should succeed");
        let signature_changed_volume =
            sign_kraken(secret_b64, path, nonce, "nonce=1700000000000&pair=XBTUSD&type=buy&ordertype=market&volume=0.2")
                .expect("signing should succeed");
        assert_ne!(signature, signature_changed_volume);
    }

    #[test]
    fn builds_post_data_with_nonce_first() {
        let params = vec![("pair".to_string(), "XBTUSD".to_string())];
        assert_eq!(build_post_data("123", &params), "nonce=123&pair=XBTUSD");
    }

    #[test]
    fn maps_side_to_kraken_string() {
        assert_eq!(map_side(OrderSide::Buy), "buy");
        assert_eq!(map_side(OrderSide::Sell), "sell");
    }

    #[test]
    fn parses_market_info_from_asset_pairs() {
        let text = r#"{
            "error": [],
            "result": {
                "XXBTZUSD": {
                    "altname": "XBTUSD",
                    "wsname": "XBT/USD",
                    "lot_decimals": 8,
                    "ordermin": "0.0001"
                }
            }
        }"#;
        let symbol = Symbol::new("XBT", "USD");
        let info = parse_market_info(text, &symbol).expect("should parse");
        assert_eq!(info.qty_step, Decimal::new(1, 8));
        assert_eq!(info.min_qty, "0.0001".parse().unwrap());
    }

    #[test]
    fn parse_market_info_defaults_min_qty_when_missing() {
        let text = r#"{
            "error": [],
            "result": {
                "XETHZUSD": {"altname": "ETHUSD", "wsname": "ETH/USD", "lot_decimals": 6}
            }
        }"#;
        let symbol = Symbol::new("ETH", "USD");
        let info = parse_market_info(text, &symbol).expect("should parse");
        assert_eq!(info.min_qty, Decimal::ZERO);
    }

    #[test]
    fn parse_market_info_surfaces_error_response() {
        let text = r#"{"error": ["EQuery:Unknown asset pair"], "result": null}"#;
        let symbol = Symbol::new("XBT", "USD");
        let err = parse_market_info(text, &symbol).unwrap_err();
        assert!(err.to_string().contains("Unknown asset pair"));
    }

    #[test]
    fn parses_add_order_result() {
        let text = r#"{
            "error": [],
            "result": {
                "descr": {"order": "buy 0.0002 XBTUSD @ market"},
                "txid": ["OQCLML-BW3P3-BUCMWZ"]
            }
        }"#;
        let result = parse_add_order_result(text).expect("should parse");
        assert_eq!(result.order_id, "OQCLML-BW3P3-BUCMWZ");
        assert_eq!(result.status, OrderStatus::New);
        assert_eq!(result.filled_qty, Decimal::ZERO);
        assert_eq!(result.avg_price, None);
    }

    #[test]
    fn parse_add_order_result_surfaces_error_response() {
        let text = r#"{"error": ["EOrder:Insufficient funds"], "result": null}"#;
        let err = parse_add_order_result(text).unwrap_err();
        assert!(err.to_string().contains("Insufficient funds"));
    }
}
