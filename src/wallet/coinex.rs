use anyhow::Context;
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::market_data::now_ms;
use crate::types::Venue;

use super::WalletProvider;
use super::types::{AssetInfo, ChainInfo, DepositAddress, WithdrawRequest, WithdrawResult};

const HOST: &str = "https://api.coinex.com";
const API_PREFIX: &str = "/v2";

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// CoinEx 原生链代码(`chain` 字段，如 `"CSC"`) -> 标准链名(与币安 `network`
/// 代码对齐，详见 [`super::types::ChainInfo::network`])的精确映射表。目前
/// 是空的：CoinEx 的 `chain` 代码和主流命名可能不一致(如 CET 的
/// `"CSC"` 指 CoinEx 自己的智能链，不是币安 `"BSC"` 对应的 BNB 智能链，
/// 两者是完全不同的链，不能互相当成同一个网络)，没有真实接口响应核对过的
/// 条目不能凭猜测往表里加，否则可能把资金转去错误的网络——按
/// `wallet::kraken::KRAKEN_METHOD_TO_STANDARD` 同样的规则，新增前必须用真实
/// `asset_info` 输出核对拼写。不在表里的链代码原样透传(转大写)。
const COINEX_CHAIN_TO_STANDARD: &[(&str, &str)] = &[];

fn chain_to_standard(native: &str) -> String {
    COINEX_CHAIN_TO_STANDARD
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(native))
        .map(|(_, standard)| standard.to_string())
        .unwrap_or_else(|| native.to_ascii_uppercase())
}

/// CoinEx 钱包(转账层)客户端：读取收款地址/链信息、发起提币。签名沿用
/// `exchange_info::coinex` 完全相同的 v2 HMAC-SHA256 方案，和 Kraken 系列的
/// 既有约定一样两个文件各自独立一份实现，不共享签名/请求辅助函数。
pub struct CoinexWalletProvider {
    venue: Venue,
    access_id: String,
    secret_key: String,
    http: reqwest::Client,
}

impl CoinexWalletProvider {
    pub fn new(venue: Venue, access_id: String, secret_key: String, proxy: Option<&str>) -> anyhow::Result<Self> {
        let http = build_http_client(proxy)?;
        Ok(Self { venue, access_id, secret_key, http })
    }

    /// 从环境变量读取凭证并构造实例：`COINEX_ACCESS_ID` + `COINEX_SECRET_KEY`。
    pub fn from_env(venue: Venue, proxy: Option<&str>) -> anyhow::Result<Self> {
        let access_id = std::env::var("COINEX_ACCESS_ID").context("COINEX_ACCESS_ID not set")?;
        let secret_key = std::env::var("COINEX_SECRET_KEY").context("COINEX_SECRET_KEY not set")?;
        Self::new(venue, access_id, secret_key, proxy)
    }

    async fn private_get(&self, path: &str, query: &[(String, String)]) -> anyhow::Result<String> {
        let request_path = build_request_path(path, query);
        self.signed_request(reqwest::Method::GET, &request_path, "").await
    }

    async fn private_post(&self, path: &str, body: &str) -> anyhow::Result<String> {
        let request_path = build_request_path(path, &[]);
        self.signed_request(reqwest::Method::POST, &request_path, body).await
    }

    async fn signed_request(&self, method: reqwest::Method, request_path: &str, body: &str) -> anyhow::Result<String> {
        let timestamp = now_ms().to_string();
        let signature = coinex_sign(&self.secret_key, method.as_str(), request_path, body, &timestamp);

        crate::ratelimit::throttle(HOST).await;
        let mut req = self
            .http
            .request(method, format!("{HOST}{request_path}"))
            .header("X-COINEX-KEY", &self.access_id)
            .header("X-COINEX-SIGN", signature)
            .header("X-COINEX-TIMESTAMP", &timestamp)
            .header("Content-Type", "application/json; charset=utf-8");
        if !body.is_empty() {
            req = req.body(body.to_string());
        }
        let resp = req.send().await.context("coinex wallet request failed")?;
        resp.text().await.context("failed to read coinex wallet response body")
    }
}

#[async_trait]
impl WalletProvider for CoinexWalletProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    async fn asset_info(&self, asset: &str) -> anyhow::Result<AssetInfo> {
        let query = vec![("ccy".to_string(), asset.to_string())];
        let text = self.private_get("/assets/deposit-withdraw-config", &query).await?;
        parse_asset_info(&text, asset)
    }

    async fn deposit_address(&self, asset: &str, network: &str) -> anyhow::Result<DepositAddress> {
        // 和 `wallet::kraken::deposit_address` 同样的顾虑：不从标准链名反查
        // CoinEx 原生 `chain` 代码，先查一次这个资产真实的 deposit-withdraw
        // 配置，直接用它自带的原生 `chain` 代码，保证和资产精确匹配。
        let info = self.asset_info(asset).await?;
        let native_chain = info
            .networks
            .iter()
            .find(|n| n.network.eq_ignore_ascii_case(network))
            .map(|n| n.name.clone())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "coinex has no chain for {asset} on network {network}; available networks: {:?}",
                    info.networks.iter().map(|n| &n.network).collect::<Vec<_>>()
                )
            })?;
        let query = vec![("ccy".to_string(), asset.to_string()), ("chain".to_string(), native_chain)];
        let text = self.private_get("/assets/deposit-address", &query).await?;
        parse_deposit_address(&text, asset, network)
    }

    async fn withdraw_raw(&self, req: &WithdrawRequest) -> anyhow::Result<WithdrawResult> {
        // `network` 是标准链名，CoinEx 提币接口要的是它自己的原生 `chain`
        // 代码，同样先查一次 asset_info 换算，不让调用方关心这层翻译。
        let info = self.asset_info(&req.asset).await?;
        let native_chain = info
            .networks
            .iter()
            .find(|n| n.network.eq_ignore_ascii_case(&req.network))
            .map(|n| n.name.clone())
            .ok_or_else(|| anyhow::anyhow!("coinex has no chain for {} on network {}", req.asset, req.network))?;

        let body = serde_json::json!({
            "ccy": req.asset,
            "chain": native_chain,
            "to_address": req.address,
            "amount": req.amount.to_string(),
        })
        .to_string();
        let text = self.private_post("/assets/withdraw", &body).await?;
        parse_withdraw_result(&text)
    }
}

fn build_http_client(proxy: Option<&str>) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder();
    if let Some(proxy) = proxy {
        let proxy = reqwest::Proxy::all(format!("http://{proxy}")).context("invalid proxy address")?;
        builder = builder.proxy(proxy);
    }
    builder.build().context("failed to build coinex http client")
}

/// `path` 相对 `/v2` 的路径(如 `/assets/deposit-address`)，`query` 按传入
/// 顺序拼接——调用方负责保证签名和实际发出请求用的是同一份 query 顺序。
fn build_request_path(path: &str, query: &[(String, String)]) -> String {
    let base = format!("{API_PREFIX}{path}");
    if query.is_empty() {
        return base;
    }
    let query_string = query.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
    format!("{base}?{query_string}")
}

/// CoinEx v2 签名算法：和 `exchange_info::coinex::coinex_sign` 完全相同，见
/// 该函数的文档注释。`body` 对 GET 请求恒为空字符串，POST 请求是实际发送
/// 的请求体原始 JSON 字符串，必须逐字节和签名时用的一致。
fn coinex_sign(secret: &str, method: &str, request_path: &str, body: &str, timestamp: &str) -> String {
    let payload = format!("{method}{request_path}{body}{timestamp}");
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
    let signature = ring::hmac::sign(&key, payload.as_bytes());
    hex_encode(signature.as_ref())
}

#[derive(Debug, Deserialize)]
struct CoinexEnvelope<T> {
    code: i64,
    #[serde(default)]
    data: Option<T>,
    #[serde(default)]
    message: String,
}

/// 解析 CoinEx v2 通用的 `{code, data, message}` 信封：`code != 0` 视为失败。
fn unwrap_data<T: DeserializeOwned>(text: &str) -> anyhow::Result<T> {
    let envelope: CoinexEnvelope<serde_json::Value> = serde_json::from_str(text)
        .with_context(|| format!("failed to parse coinex response envelope, raw body: {text}"))?;
    if envelope.code != 0 {
        anyhow::bail!("coinex error {}: {}", envelope.code, envelope.message);
    }
    let data = envelope.data.ok_or_else(|| anyhow::anyhow!("coinex response missing data"))?;
    serde_json::from_value(data.clone())
        .with_context(|| format!("failed to parse coinex data payload, raw data: {data}"))
}

#[derive(Debug, Deserialize)]
struct ChainConfigEntry {
    chain: String,
    #[serde(default)]
    deposit_enabled: bool,
    #[serde(default)]
    withdraw_enabled: bool,
    #[serde(default)]
    min_withdraw_amount: Option<String>,
    #[serde(default)]
    withdrawal_fee: Option<String>,
    #[serde(default)]
    safe_confirmations: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct DepositWithdrawConfigData {
    #[serde(default)]
    chains: Vec<ChainConfigEntry>,
}

/// `GET /v2/assets/deposit-withdraw-config` 没有提供合约地址信息，
/// `contract_address` 恒为 `None`。
fn parse_asset_info(text: &str, asset: &str) -> anyhow::Result<AssetInfo> {
    let data: DepositWithdrawConfigData = unwrap_data(text)?;
    let networks = data
        .chains
        .into_iter()
        .map(|c| ChainInfo {
            network: chain_to_standard(&c.chain),
            name: c.chain,
            deposit_enabled: c.deposit_enabled,
            withdraw_enabled: c.withdraw_enabled,
            withdraw_fee: c.withdrawal_fee.and_then(|f| f.parse().ok()).unwrap_or(Decimal::ZERO),
            withdraw_min: c.min_withdraw_amount.and_then(|v| v.parse().ok()).unwrap_or(Decimal::ZERO),
            min_confirm: c.safe_confirmations.unwrap_or(0),
            contract_address: None,
        })
        .collect();

    Ok(AssetInfo { asset: asset.to_string(), networks })
}

#[derive(Debug, Deserialize)]
struct DepositAddressData {
    address: String,
    #[serde(default)]
    memo: String,
}

fn parse_deposit_address(text: &str, asset: &str, network: &str) -> anyhow::Result<DepositAddress> {
    let data: DepositAddressData = unwrap_data(text)?;
    let tag = if data.memo.is_empty() { None } else { Some(data.memo) };
    Ok(DepositAddress { asset: asset.to_string(), network: network.to_string(), address: data.address, tag })
}

#[derive(Debug, Deserialize)]
struct WithdrawResponseData {
    withdraw_id: serde_json::Value,
}

fn parse_withdraw_result(text: &str) -> anyhow::Result<WithdrawResult> {
    let data: WithdrawResponseData = unwrap_data(text)?;
    let id = match data.withdraw_id {
        serde_json::Value::String(s) => s,
        other => other.to_string(),
    };
    Ok(WithdrawResult { id })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coinex_sign_changes_with_any_field() {
        let secret = "test-secret";
        let base = coinex_sign(secret, "GET", "/v2/assets/deposit-address", "", "1700000000000");

        assert_ne!(base, coinex_sign(secret, "POST", "/v2/assets/deposit-address", "", "1700000000000"));
        assert_ne!(base, coinex_sign(secret, "GET", "/v2/assets/withdraw", "", "1700000000000"));
        assert_ne!(base, coinex_sign(secret, "GET", "/v2/assets/deposit-address", "{\"a\":1}", "1700000000000"));
        assert_ne!(base, coinex_sign(secret, "GET", "/v2/assets/deposit-address", "", "1700000000001"));
        assert_ne!(base, coinex_sign("other-secret", "GET", "/v2/assets/deposit-address", "", "1700000000000"));
        assert_eq!(base.len(), 64);
    }

    #[test]
    fn chain_to_standard_passes_through_when_table_empty() {
        assert_eq!(chain_to_standard("csc"), "CSC");
    }

    #[test]
    fn parses_asset_info_response() {
        let text = r#"{
            "code": 0,
            "data": {
                "asset": {"ccy": "CET", "deposit_enabled": true, "withdraw_enabled": true},
                "chains": [
                    {"chain": "CSC", "min_deposit_amount": "0.023", "min_withdraw_amount": "0.019",
                     "deposit_enabled": true, "withdraw_enabled": false, "safe_confirmations": 100,
                     "withdrawal_fee": "0.019"}
                ]
            },
            "message": "OK"
        }"#;

        let info = parse_asset_info(text, "CET").expect("should parse");
        assert_eq!(info.networks.len(), 1);
        // 表里没有 "CSC" 的映射条目，原样透传(转大写)。
        assert_eq!(info.networks[0].network, "CSC");
        assert_eq!(info.networks[0].name, "CSC");
        assert!(!info.networks[0].withdraw_enabled);
        assert_eq!(info.networks[0].withdraw_min, "0.019".parse().unwrap());
        assert_eq!(info.networks[0].withdraw_fee, "0.019".parse().unwrap());
        assert_eq!(info.networks[0].min_confirm, 100);
    }

    #[test]
    fn parse_asset_info_surfaces_error_response() {
        let text = r#"{"code": 3008, "data": null, "message": "require auth"}"#;
        let err = parse_asset_info(text, "CET").unwrap_err();
        assert!(err.to_string().contains("require auth"));
    }

    #[test]
    fn parses_deposit_address_response() {
        let text = r#"{"code": 0, "data": {"address": "0x40aa234bcdc528ce411a6020da1a3c07124039d4", "memo": ""}, "message": "OK"}"#;
        let addr = parse_deposit_address(text, "CET", "CSC").expect("should parse");
        assert_eq!(addr.address, "0x40aa234bcdc528ce411a6020da1a3c07124039d4");
        assert_eq!(addr.tag, None);
    }

    #[test]
    fn parses_deposit_address_response_with_memo() {
        let text = r#"{"code": 0, "data": {"address": "raddr", "memo": "12345"}, "message": "OK"}"#;
        let addr = parse_deposit_address(text, "XRP", "XRP").expect("should parse");
        assert_eq!(addr.tag, Some("12345".to_string()));
    }

    #[test]
    fn parses_withdraw_result_with_numeric_id() {
        let text = r#"{"code": 0, "data": {"withdraw_id": 206}, "message": "OK"}"#;
        let result = parse_withdraw_result(text).expect("should parse");
        assert_eq!(result.id, "206");
    }

    #[test]
    fn parse_withdraw_result_surfaces_error_response() {
        let text = r#"{"code": 3410, "data": null, "message": "withdraw disabled"}"#;
        let err = parse_withdraw_result(text).unwrap_err();
        assert!(err.to_string().contains("withdraw disabled"));
    }

    #[test]
    fn builds_request_path_with_and_without_query() {
        assert_eq!(build_request_path("/assets/withdraw", &[]), "/v2/assets/withdraw");
        assert_eq!(
            build_request_path("/assets/deposit-address", &[("ccy".to_string(), "CET".to_string())]),
            "/v2/assets/deposit-address?ccy=CET"
        );
    }
}
