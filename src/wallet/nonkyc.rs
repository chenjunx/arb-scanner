use anyhow::Context;
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::market_data::now_ms;
use crate::types::Venue;

use super::WalletProvider;
use super::types::{AssetInfo, ChainInfo, DepositAddress, WithdrawRequest, WithdrawResult};

const HOST: &str = "https://api.nonkyc.io";
const API_PREFIX: &str = "/api/v2";

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Nonkyc 原生链后缀(资产 `ticker` 里 "-" 之后的部分,如 `USDT-BEP20` 里的
/// `"BEP20"`) -> 标准链名(与币安 `network` 代码对齐,详见
/// [`super::types::ChainInfo::network`])的精确映射表。目前是空的:虽然
/// `-BEP20`/`-ERC20`/`-TRC20` 这些后缀看着眼熟,但对应到币安自己的 `network`
/// 代码字符串没有用真实币安账户核对过,写错一个就可能把资金转去错误的网络——
/// 和 `wallet::coinex::COINEX_CHAIN_TO_STANDARD` 同样的规则,没有真实响应核对
/// 过的条目不能凭猜测入表。不在表里的后缀原样透传(转大写)。
const NONKYC_CHAIN_TO_STANDARD: &[(&str, &str)] = &[];

/// `entry_ticker` 是某个 network 条目自己的 ticker(如 `"USDT-BEP20"`,或者
/// 父资产条目本身不带后缀的 `"USDT"`);`parent_ticker` 是资产本身的 ticker,
/// 用来把它从 `entry_ticker` 里剥掉,只保留链后缀参与映射查找。父资产条目本身
/// 没有后缀,直接原样透传父 ticker,同样不猜测它默认在哪条链。
fn chain_to_standard(entry_ticker: &str, parent_ticker: &str) -> String {
    let suffix = entry_ticker.strip_prefix(parent_ticker).and_then(|rest| rest.strip_prefix('-')).unwrap_or(entry_ticker);
    NONKYC_CHAIN_TO_STANDARD
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(suffix))
        .map(|(_, standard)| standard.to_string())
        .unwrap_or_else(|| suffix.to_ascii_uppercase())
}

/// Nonkyc 钱包(转账层)客户端:读取收款地址/链信息、发起提币。签名方案和
/// `exchange_info::coinex`/`wallet::coinex` 用的 CoinEx v2 方案不同——Nonkyc
/// 签的是完整 URL(含协议和域名)+ body + nonce,不是只签 path+query,见
/// [`nonkyc_sign`]。和 CoinEx/Kraken 系列的既有约定一样,两个 nonkyc 文件各自
/// 独立一份签名/请求辅助函数实现,不跨文件共享。
///
/// **提币地址白名单风险**:Nonkyc 官方文档在 `POST /createwithdrawal` 上明确
/// 写着地址 "must be a validated address on your Account (Private)"——提币
/// 目标地址可能需要先在 Nonkyc 网页后台手动加白名单,API 无法直接提到任意
/// 地址。这个限制无法用代码绕过,真正调用 `withdraw()` 前必须先用真实账户
/// 小额验证一次,确认地址白名单要求的严格程度。
pub struct NonkycWalletProvider {
    venue: Venue,
    api_key: String,
    api_secret: String,
    http: reqwest::Client,
}

impl NonkycWalletProvider {
    pub fn new(venue: Venue, api_key: String, api_secret: String, proxy: Option<&str>) -> anyhow::Result<Self> {
        let http = build_http_client(proxy)?;
        Ok(Self { venue, api_key, api_secret, http })
    }

    /// 从环境变量读取凭证并构造实例:`NONKYC_API_KEY` + `NONKYC_API_SECRET`。
    pub fn from_env(venue: Venue, proxy: Option<&str>) -> anyhow::Result<Self> {
        let api_key = std::env::var("NONKYC_API_KEY").context("NONKYC_API_KEY not set")?;
        let api_secret = std::env::var("NONKYC_API_SECRET").context("NONKYC_API_SECRET not set")?;
        Self::new(venue, api_key, api_secret, proxy)
    }

    /// `GET /asset/getlist` 是公开接口,不需要签名——即使被 `WalletProvider`
    /// 这个"私有"语义的 trait 调用,也不代表底层请求本身要认证。
    async fn public_request<T: DeserializeOwned>(&self, path: &str) -> anyhow::Result<T> {
        let url = format!("{HOST}{API_PREFIX}{path}");
        crate::ratelimit::throttle(HOST).await;
        let resp = self.http.get(&url).send().await.context("nonkyc wallet public request failed")?;
        let text = resp.text().await.context("failed to read nonkyc wallet public response body")?;
        parse_data(&text)
    }

    async fn private_get<T: DeserializeOwned>(&self, path: &str) -> anyhow::Result<T> {
        let url = format!("{HOST}{API_PREFIX}{path}");
        let nonce = now_ms().to_string();
        let signature = nonkyc_sign(&self.api_secret, &self.api_key, &url, "", &nonce);

        crate::ratelimit::throttle(HOST).await;
        let resp = self
            .http
            .get(&url)
            .header("X-API-KEY", &self.api_key)
            .header("X-API-NONCE", &nonce)
            .header("X-API-SIGN", signature)
            .send()
            .await
            .context("nonkyc wallet private request failed")?;
        let text = resp.text().await.context("failed to read nonkyc wallet private response body")?;
        parse_data(&text)
    }

    async fn private_post<T: DeserializeOwned>(&self, path: &str, body: &str) -> anyhow::Result<T> {
        let url = format!("{HOST}{API_PREFIX}{path}");
        let nonce = now_ms().to_string();
        let signature = nonkyc_sign(&self.api_secret, &self.api_key, &url, body, &nonce);

        crate::ratelimit::throttle(HOST).await;
        let resp = self
            .http
            .post(&url)
            .header("X-API-KEY", &self.api_key)
            .header("X-API-NONCE", &nonce)
            .header("X-API-SIGN", signature)
            .header("Content-Type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .context("nonkyc wallet private request failed")?;
        let text = resp.text().await.context("failed to read nonkyc wallet private response body")?;
        parse_data(&text)
    }
}

#[async_trait]
impl WalletProvider for NonkycWalletProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    async fn asset_info(&self, asset: &str) -> anyhow::Result<AssetInfo> {
        let assets: Vec<AssetEntry> = self.public_request("/asset/getlist").await?;
        parse_asset_info(&assets, asset)
    }

    async fn deposit_address(&self, asset: &str, network: &str) -> anyhow::Result<DepositAddress> {
        // 和 `wallet::coinex::deposit_address` 同样的顾虑:不从标准链名反查
        // Nonkyc 原生 ticker,先查一次这个资产真实的 asset_info,直接用它自带
        // 的 per-network ticker,保证和资产精确匹配。
        let info = self.asset_info(asset).await?;
        let entry_ticker = info
            .networks
            .iter()
            .find(|n| n.network.eq_ignore_ascii_case(network))
            .map(|n| n.name.clone())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "nonkyc has no chain for {asset} on network {network}; available networks: {:?}",
                    info.networks.iter().map(|n| &n.network).collect::<Vec<_>>()
                )
            })?;
        let data: DepositAddressData = self.private_get(&format!("/getdepositaddress/{entry_ticker}")).await?;
        Ok(parse_deposit_address(data, asset, network))
    }

    async fn withdraw_raw(&self, req: &WithdrawRequest) -> anyhow::Result<WithdrawResult> {
        // `network` 是标准链名,Nonkyc 提币接口要的是它自己的 per-network
        // ticker,同样先查一次 asset_info 换算,不让调用方关心这层翻译。
        let info = self.asset_info(&req.asset).await?;
        let entry_ticker = info
            .networks
            .iter()
            .find(|n| n.network.eq_ignore_ascii_case(&req.network))
            .map(|n| n.name.clone())
            .ok_or_else(|| anyhow::anyhow!("nonkyc has no chain for {} on network {}", req.asset, req.network))?;

        let mut body = serde_json::json!({
            "ticker": entry_ticker,
            "quantity": req.amount.to_string(),
            "address": req.address,
        });
        if let Some(tag) = &req.tag {
            body["paymentid"] = serde_json::Value::String(tag.clone());
        }
        let data: WithdrawalData = self.private_post("/createwithdrawal", &body.to_string()).await?;
        Ok(WithdrawResult { id: data.id })
    }
}

fn build_http_client(proxy: Option<&str>) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder();
    if let Some(proxy) = proxy {
        let proxy = reqwest::Proxy::all(format!("http://{proxy}")).context("invalid proxy address")?;
        builder = builder.proxy(proxy);
    }
    builder.build().context("failed to build nonkyc http client")
}

/// Nonkyc 签名算法(取自其 OpenAPI 描述里自带的 Python 示例代码):
/// GET 请求签 `apiKey + 完整URL(含协议和域名) + nonce`,POST 请求在中间插入
/// 请求体原始 JSON 字符串——`body` 对 GET 请求恒为空字符串。和 CoinEx 只签
/// path+query 不同,这里 `url` 必须是完整 URL。
fn nonkyc_sign(secret: &str, api_key: &str, url: &str, body: &str, nonce: &str) -> String {
    let payload = format!("{api_key}{url}{body}{nonce}");
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
    let signature = ring::hmac::sign(&key, payload.as_bytes());
    hex_encode(signature.as_ref())
}

#[derive(Debug, Deserialize)]
struct NonkycError {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct NonkycErrorEnvelope {
    error: NonkycError,
}

/// Nonkyc 响应解析:成功时响应体直接就是数据本身(不像 CoinEx 包一层
/// `{code,data}` 信封);业务错误返回 `{"error":{code,message,...}}`;网关层
/// 错误(如未认证的 401)返回纯文本,不是 JSON。依次尝试三种情况,都不是的话把
/// 原始 body 内容整个报出来,方便定位。
fn parse_data<T: DeserializeOwned>(text: &str) -> anyhow::Result<T> {
    if let Ok(data) = serde_json::from_str::<T>(text) {
        return Ok(data);
    }
    if let Ok(envelope) = serde_json::from_str::<NonkycErrorEnvelope>(text) {
        anyhow::bail!("nonkyc error {}: {}", envelope.error.code, envelope.error.message);
    }
    anyhow::bail!("nonkyc request failed, raw response: {text}");
}

/// Nonkyc 的多链资产建模:每条链上的变体都是一个独立的资产记录,有自己的
/// `ticker`(如 `"USDT-BEP20"`),通过 `childOf` 精确指回父资产(如
/// `"USDT"`)的 `id`。父资产条目本身 `child_of` 为 `None`。`minimumWithdraw`
/// 字段在部分子资产条目上存在(如 `USDT-BEP20`),但在对应的父资产条目上不
/// 存在(如 `USDT` 本身只有 `maximumWithdraw`),必须按 `Option`/`#[serde(default)]`
/// 处理,不能假设它总是存在。
#[derive(Debug, Deserialize)]
struct AssetEntry {
    id: String,
    ticker: String,
    #[serde(rename = "childOf")]
    child_of: Option<String>,
    #[serde(rename = "depositActive", default)]
    deposit_active: bool,
    #[serde(rename = "withdrawalActive", default)]
    withdrawal_active: bool,
    #[serde(rename = "withdrawFee", default)]
    withdraw_fee: Decimal,
    #[serde(rename = "minimumWithdraw", default)]
    minimum_withdraw: Decimal,
    #[serde(rename = "confirmsRequired", default)]
    confirms_required: u32,
}

/// `GET /asset/getlist` 没有提供合约地址信息,`contract_address` 恒为 `None`。
fn parse_asset_info(assets: &[AssetEntry], asset: &str) -> anyhow::Result<AssetInfo> {
    let parent = assets
        .iter()
        .find(|a| a.ticker.eq_ignore_ascii_case(asset))
        .ok_or_else(|| anyhow::anyhow!("nonkyc has no asset {asset}"))?;

    let mut entries: Vec<&AssetEntry> = vec![parent];
    entries.extend(assets.iter().filter(|a| a.child_of.as_deref() == Some(parent.id.as_str())));

    let networks = entries
        .into_iter()
        .map(|a| ChainInfo {
            network: chain_to_standard(&a.ticker, &parent.ticker),
            name: a.ticker.clone(),
            deposit_enabled: a.deposit_active,
            withdraw_enabled: a.withdrawal_active,
            withdraw_fee: a.withdraw_fee,
            withdraw_min: a.minimum_withdraw,
            min_confirm: a.confirms_required,
            contract_address: None,
        })
        .collect();

    Ok(AssetInfo { asset: asset.to_string(), networks })
}

#[derive(Debug, Deserialize)]
struct DepositAddressData {
    address: String,
    #[serde(default)]
    paymentid: String,
}

fn parse_deposit_address(data: DepositAddressData, asset: &str, network: &str) -> DepositAddress {
    let tag = if data.paymentid.is_empty() { None } else { Some(data.paymentid) };
    DepositAddress { asset: asset.to_string(), network: network.to_string(), address: data.address, tag }
}

#[derive(Debug, Deserialize)]
struct WithdrawalData {
    id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nonkyc_sign_changes_with_any_field() {
        let secret = "test-secret";
        let base = nonkyc_sign(secret, "key", "https://api.nonkyc.io/api/v2/getdepositaddress/USDT", "", "1700000000000");

        assert_ne!(base, nonkyc_sign("other-secret", "key", "https://api.nonkyc.io/api/v2/getdepositaddress/USDT", "", "1700000000000"));
        assert_ne!(base, nonkyc_sign(secret, "other-key", "https://api.nonkyc.io/api/v2/getdepositaddress/USDT", "", "1700000000000"));
        assert_ne!(base, nonkyc_sign(secret, "key", "https://api.nonkyc.io/api/v2/createwithdrawal", "", "1700000000000"));
        assert_ne!(base, nonkyc_sign(secret, "key", "https://api.nonkyc.io/api/v2/getdepositaddress/USDT", "{\"a\":1}", "1700000000000"));
        assert_ne!(base, nonkyc_sign(secret, "key", "https://api.nonkyc.io/api/v2/getdepositaddress/USDT", "", "1700000000001"));
        assert_eq!(base.len(), 64);
    }

    #[test]
    fn chain_to_standard_strips_parent_prefix_and_uppercases() {
        assert_eq!(chain_to_standard("USDT-BEP20", "USDT"), "BEP20");
        assert_eq!(chain_to_standard("USDT", "USDT"), "USDT");
    }

    fn asset(id: &str, ticker: &str, child_of: Option<&str>, deposit: bool, withdraw: bool, fee: &str, min: &str, confirms: u32) -> AssetEntry {
        AssetEntry {
            id: id.to_string(),
            ticker: ticker.to_string(),
            child_of: child_of.map(|s| s.to_string()),
            deposit_active: deposit,
            withdrawal_active: withdraw,
            withdraw_fee: fee.parse().unwrap(),
            minimum_withdraw: min.parse().unwrap(),
            confirms_required: confirms,
        }
    }

    #[test]
    fn parse_asset_info_groups_children_by_child_of() {
        let assets = vec![
            asset("parent-id", "USDT", None, true, true, "0.0008", "0", 10),
            asset("child-1", "USDT-BEP20", Some("parent-id"), true, true, "1.0", "1", 15),
            asset("child-2", "USDT-ERC20", Some("parent-id"), true, false, "5.0", "10", 12),
            asset("other-id", "ETH", None, true, true, "0.001", "0.01", 30),
        ];

        let info = parse_asset_info(&assets, "USDT").expect("should parse");
        assert_eq!(info.asset, "USDT");
        assert_eq!(info.networks.len(), 3);

        let parent = info.networks.iter().find(|n| n.name == "USDT").expect("parent network present");
        assert_eq!(parent.network, "USDT");
        assert_eq!(parent.withdraw_min, Decimal::ZERO);

        let bep20 = info.networks.iter().find(|n| n.name == "USDT-BEP20").expect("bep20 network present");
        assert_eq!(bep20.network, "BEP20");
        assert!(bep20.withdraw_enabled);
        assert_eq!(bep20.withdraw_min, "1".parse().unwrap());
        assert_eq!(bep20.min_confirm, 15);

        let erc20 = info.networks.iter().find(|n| n.name == "USDT-ERC20").expect("erc20 network present");
        assert!(!erc20.withdraw_enabled);
    }

    #[test]
    fn parse_asset_info_missing_asset_errors() {
        let assets = vec![asset("parent-id", "USDT", None, true, true, "0.0008", "0", 10)];
        let err = parse_asset_info(&assets, "BTC").unwrap_err();
        assert!(err.to_string().contains("nonkyc has no asset BTC"));
    }

    #[test]
    fn parses_direct_asset_list_response() {
        let text = r#"[{"id":"1","ticker":"USDT","childOf":null,"depositActive":true,"withdrawalActive":true,"withdrawFee":0.0008,"confirmsRequired":10}]"#;
        let assets: Vec<AssetEntry> = parse_data(text).expect("should parse");
        assert_eq!(assets.len(), 1);
        assert_eq!(assets[0].minimum_withdraw, Decimal::ZERO);
    }

    #[test]
    fn parse_data_surfaces_error_envelope() {
        let text = r#"{"error":{"code":401,"message":"Invalid signature","description":"bad sig"}}"#;
        let err = parse_data::<Vec<AssetEntry>>(text).unwrap_err();
        assert!(err.to_string().contains("Invalid signature"));
    }

    #[test]
    fn parse_data_surfaces_plain_text_error() {
        let err = parse_data::<Vec<AssetEntry>>("Not Authorized").unwrap_err();
        assert!(err.to_string().contains("Not Authorized"));
    }

    #[test]
    fn parses_deposit_address_response() {
        let data = DepositAddressData { address: "0xabc".to_string(), paymentid: String::new() };
        let addr = parse_deposit_address(data, "USDT", "BEP20");
        assert_eq!(addr.address, "0xabc");
        assert_eq!(addr.tag, None);
    }

    #[test]
    fn parses_deposit_address_response_with_paymentid() {
        let data = DepositAddressData { address: "raddr".to_string(), paymentid: "12345".to_string() };
        let addr = parse_deposit_address(data, "XRP", "XRP");
        assert_eq!(addr.tag, Some("12345".to_string()));
    }
}
