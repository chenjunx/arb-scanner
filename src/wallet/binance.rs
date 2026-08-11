use anyhow::Context;
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as base64_engine;
use ring::signature::Ed25519KeyPair;
use serde::Deserialize;

use crate::market_data::now_ms;
use crate::types::Venue;

use super::WalletProvider;
use super::types::{AssetInfo, ChainInfo, DepositAddress, WithdrawRequest, WithdrawResult};

const MAINNET_HOST: &str = "https://api.binance.com";
const TESTNET_HOST: &str = "https://testnet.binance.vision";
const RECV_WINDOW_MS: u64 = 5_000;

/// 币安钱包(转账层)客户端：读取收款地址/链信息、发起提币。签名用 Ed25519
/// (币安推荐的非对称签名方式,不是 HMAC-SHA256),私钥以 PKCS8 PEM 文本传入。
pub struct BinanceWalletProvider {
    venue: Venue,
    api_key: String,
    key_pair: Ed25519KeyPair,
    host: &'static str,
    http: reqwest::Client,
}

impl BinanceWalletProvider {
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

    /// 从环境变量读取凭证并构造实例:`BINANCE_API_KEY` +
    /// `BINANCE_API_SECRET`(完整 PEM 文本)。
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
            .context("binance wallet request failed")?;
        let text = resp.text().await.context("failed to read binance wallet response body")?;
        Ok(text)
    }
}

#[async_trait]
impl WalletProvider for BinanceWalletProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    async fn asset_info(&self, asset: &str) -> anyhow::Result<AssetInfo> {
        let text = self
            .signed_request(reqwest::Method::GET, "/sapi/v1/capital/config/getall", Vec::new())
            .await?;
        parse_asset_info(&text, asset)
    }

    async fn deposit_address(&self, asset: &str, network: &str) -> anyhow::Result<DepositAddress> {
        let params = vec![
            ("coin".to_string(), asset.to_string()),
            ("network".to_string(), network.to_string()),
        ];
        let text = self
            .signed_request(reqwest::Method::GET, "/sapi/v1/capital/deposit/address", params)
            .await?;
        parse_deposit_address(&text)
    }

    async fn withdraw_raw(&self, req: &WithdrawRequest) -> anyhow::Result<WithdrawResult> {
        let mut params = vec![
            ("coin".to_string(), req.asset.clone()),
            ("network".to_string(), req.network.clone()),
            ("address".to_string(), req.address.clone()),
            ("amount".to_string(), req.amount.to_string()),
        ];
        if let Some(tag) = &req.tag {
            params.push(("addressTag".to_string(), tag.clone()));
        }
        let text = self
            .signed_request(reqwest::Method::POST, "/sapi/v1/capital/withdraw/apply", params)
            .await?;
        parse_withdraw_result(&text)
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

/// 对 payload 做 Ed25519 签名，返回 base64 编码结果(Ed25519/RSA 签名方案用 base64，
/// 和 HMAC-SHA256 方案用 hex 编码不同)。
fn sign_ed25519(key_pair: &Ed25519KeyPair, payload: &str) -> String {
    let signature = key_pair.sign(payload.as_bytes());
    base64_engine.encode(signature.as_ref())
}

#[derive(Debug, Deserialize)]
struct CoinConfig {
    coin: String,
    #[serde(rename = "networkList")]
    network_list: Vec<NetworkConfig>,
}

#[derive(Debug, Deserialize)]
struct NetworkConfig {
    network: String,
    name: String,
    #[serde(rename = "depositEnable")]
    deposit_enable: bool,
    #[serde(rename = "withdrawEnable")]
    withdraw_enable: bool,
    #[serde(rename = "withdrawFee")]
    withdraw_fee: rust_decimal::Decimal,
    #[serde(rename = "withdrawMin")]
    withdraw_min: rust_decimal::Decimal,
    #[serde(rename = "minConfirm")]
    min_confirm: u32,
    #[serde(rename = "contractAddress", default)]
    contract_address: String,
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    code: i64,
    msg: String,
}

fn parse_asset_info(text: &str, asset: &str) -> anyhow::Result<AssetInfo> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance error {}: {}", err.code, err.msg);
    }
    let coins: Vec<CoinConfig> = serde_json::from_str(text).context("failed to parse binance coin config response")?;
    let coin = coins
        .into_iter()
        .find(|c| c.coin.eq_ignore_ascii_case(asset))
        .ok_or_else(|| anyhow::anyhow!("asset {asset} not found in binance coin config"))?;

    let networks = coin
        .network_list
        .into_iter()
        .map(|n| ChainInfo {
            network: n.network,
            name: n.name,
            deposit_enabled: n.deposit_enable,
            withdraw_enabled: n.withdraw_enable,
            withdraw_fee: n.withdraw_fee,
            withdraw_min: n.withdraw_min,
            min_confirm: n.min_confirm,
            contract_address: (!n.contract_address.is_empty()).then_some(n.contract_address),
        })
        .collect();

    Ok(AssetInfo {
        asset: coin.coin,
        networks,
    })
}

#[derive(Debug, Deserialize)]
struct DepositAddressResponse {
    address: String,
    coin: String,
    #[serde(default)]
    tag: String,
}

fn parse_deposit_address(text: &str) -> anyhow::Result<DepositAddress> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance error {}: {}", err.code, err.msg);
    }
    let resp: DepositAddressResponse =
        serde_json::from_str(text).context("failed to parse binance deposit address response")?;
    Ok(DepositAddress {
        asset: resp.coin,
        network: String::new(),
        address: resp.address,
        tag: (!resp.tag.is_empty()).then_some(resp.tag),
    })
}

#[derive(Debug, Deserialize)]
struct WithdrawApplyResponse {
    id: String,
}

fn parse_withdraw_result(text: &str) -> anyhow::Result<WithdrawResult> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance error {}: {}", err.code, err.msg);
    }
    let resp: WithdrawApplyResponse =
        serde_json::from_str(text).context("failed to parse binance withdraw apply response")?;
    Ok(WithdrawResult { id: resp.id })
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

        let payload = "coin=BTC&network=BTC&timestamp=1700000000000";
        let signature_b64 = sign_ed25519(&loaded, payload);
        let signature = base64_engine.decode(signature_b64).expect("valid base64 signature");

        let public_key = UnparsedPublicKey::new(&ring::signature::ED25519, key_pair.public_key().as_ref());
        public_key
            .verify(payload.as_bytes(), &signature)
            .expect("signature should verify against the same keypair's public key");
    }

    #[test]
    fn signature_changes_when_payload_changes() {
        let (pem, _) = generate_test_pem();
        let loaded = load_ed25519_key(&pem).expect("should load key from PEM");
        let sig_a = sign_ed25519(&loaded, "a=1");
        let sig_b = sign_ed25519(&loaded, "a=2");
        assert_ne!(sig_a, sig_b);
    }

    #[test]
    fn builds_query_string_in_insertion_order() {
        let params = vec![
            ("coin".to_string(), "BTC".to_string()),
            ("network".to_string(), "BTC".to_string()),
            ("timestamp".to_string(), "123".to_string()),
        ];
        assert_eq!(build_query_string(&params), "coin=BTC&network=BTC&timestamp=123");
    }

    #[test]
    fn parses_asset_info_response() {
        let text = r#"[
            {
                "coin": "USDT",
                "depositAllEnable": true,
                "withdrawAllEnable": true,
                "name": "TetherUS",
                "networkList": [
                    {
                        "network": "ETH",
                        "coin": "USDT",
                        "name": "Ethereum (ERC20)",
                        "depositEnable": true,
                        "withdrawEnable": true,
                        "withdrawFee": "10.00000000",
                        "withdrawMin": "20.00000000",
                        "minConfirm": 12,
                        "contractAddress": "0xdAC17F958D2ee523a2206206994597C13D831ec"
                    },
                    {
                        "network": "TRX",
                        "coin": "USDT",
                        "name": "Tron (TRC20)",
                        "depositEnable": true,
                        "withdrawEnable": false,
                        "withdrawFee": "1.00000000",
                        "withdrawMin": "5.00000000",
                        "minConfirm": 1,
                        "contractAddress": ""
                    }
                ]
            }
        ]"#;

        let info = parse_asset_info(text, "usdt").expect("should parse");
        assert_eq!(info.asset, "USDT");
        assert_eq!(info.networks.len(), 2);
        assert_eq!(info.networks[0].network, "ETH");
        assert!(info.networks[0].withdraw_enabled);
        assert_eq!(
            info.networks[0].contract_address.as_deref(),
            Some("0xdAC17F958D2ee523a2206206994597C13D831ec")
        );
        assert!(!info.networks[1].withdraw_enabled);
        assert_eq!(info.networks[1].contract_address, None);
    }

    #[test]
    fn parse_asset_info_errors_on_unknown_asset() {
        let text = r#"[{"coin":"BTC","networkList":[]}]"#;
        assert!(parse_asset_info(text, "ETH").is_err());
    }

    #[test]
    fn parse_asset_info_surfaces_error_response() {
        let text = r#"{"code":-2014,"msg":"API-key format invalid."}"#;
        let err = parse_asset_info(text, "BTC").unwrap_err();
        assert!(err.to_string().contains("-2014"));
    }

    #[test]
    fn parses_deposit_address_response() {
        let text = r#"{"address":"0xabc123","coin":"USDT","tag":"","url":"https://etherscan.io"}"#;
        let addr = parse_deposit_address(text).expect("should parse");
        assert_eq!(addr.address, "0xabc123");
        assert_eq!(addr.asset, "USDT");
        assert_eq!(addr.tag, None);
    }

    #[test]
    fn parses_deposit_address_with_tag() {
        let text = r#"{"address":"rN7n7otQ","coin":"XRP","tag":"12345"}"#;
        let addr = parse_deposit_address(text).expect("should parse");
        assert_eq!(addr.tag.as_deref(), Some("12345"));
    }

    #[test]
    fn parses_withdraw_result() {
        let text = r#"{"id":"7213fea8e94b4a5593d507237e5a555b"}"#;
        let result = parse_withdraw_result(text).expect("should parse");
        assert_eq!(result.id, "7213fea8e94b4a5593d507237e5a555b");
    }

    #[test]
    fn parse_withdraw_result_surfaces_error_response() {
        let text = r#"{"code":-6001,"msg":"You are not authorized to withdraw."}"#;
        let err = parse_withdraw_result(text).unwrap_err();
        assert!(err.to_string().contains("You are not authorized"));
    }
}
