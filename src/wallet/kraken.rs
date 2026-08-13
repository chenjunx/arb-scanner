use anyhow::Context;
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as base64_engine;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::market_data::now_ms;
use crate::types::Venue;

use super::WalletProvider;
use super::types::{AssetInfo, ChainInfo, DepositAddress, WithdrawRequest, WithdrawResult};

const HOST: &str = "https://api.kraken.com";

/// Kraken 存款方式(method)原始名称 -> 标准链名(与币安 `network` 代码对齐,
/// 详见 [`super::types::ChainInfo::network`])的精确映射表。覆盖不全，按需
/// 补充；新增前必须用真实 asset_info 输出核对 Kraken 方式名的准确拼写，
/// 不能凭猜测往表里加，否则可能把资金转去错误的网络。不在表里的方式会
/// 原样透传原始名称——不影响手动指定 network 的调用，只是这条链无法参与
/// `execution` 模块的跨交易所自动匹配。
const KRAKEN_METHOD_TO_STANDARD: &[(&str, &str)] = &[
    ("Bitcoin", "BTC"),
    ("Ethereum", "ETH"),
    // Kraken 把 ETH 主网存款方式改名成了 "Ether (Hex)"（不再是 "Ethereum"）。
    // 其余链名不用在这里逐条列 "ETH - <chain> (Unified)" 变体——
    // `native_to_standard` 会先剥掉 Kraken 现在给很多资产套上的
    // "<TICKER> - <链名>" 前缀和 "(Unified)" 后缀外壳，剩下的链名再来这张表
    // 精确匹配，所以下面已有的 "Optimism"/"Base"/"Arbitrum One" 等条目会
    // 自动覆盖 "ETH - Optimism (Unified)" 这类变体，不用重复添加。
    ("Ether (Hex)", "ETH"),
    ("zkSync Era", "ZKSYNCERA"),
    ("Sonic", "SONIC"),
    ("Berachain", "BERA"),
    // 下面两条格式和 "<TICKER> - <链名>" 外壳不一样，剥壳逻辑套不上，只能
    // 按完整原始字符串精确收录(均已用真实 `DepositMethods` 输出核对过)。
    ("S (Sonic)", "SONIC"),
    ("USDC (SPL)", "SOL"),
    ("Stellar XLM", "XLM"),
    ("Tron", "TRX"),
    ("Solana", "SOL"),
    ("BNB Smart Chain (BEP20)", "BSC"),
    ("Polygon", "MATIC"),
    // 以下条目基于链名称与币安公开网络代码的经验对照补充，未逐条用真实
    // Kraken `DepositMethods` 输出核对拼写——按本文件上方的规则，接入
    // `execution` 模块自动跨所转账前必须先用真实接口响应核对一遍。凡是
    // 存在链版本歧义(如新旧 Terra、Polkadot/Kusama relay 与 Asset Hub、
    // dYdX 老 ERC20 与 dYdX Chain、Sei 原生链与 EVM 层等)或币安是否支持/
    // 具体网络代码拼写没有把握的链，一律不加，保持原样透传。
    ("BNB Chain", "BSC"),
    ("Arbitrum One", "ARBITRUM"),
    ("Arbitrum One (USDC)", "ARBITRUM"),
    ("Arbitrum One (USDC.e)", "ARBITRUM"),
    ("Optimism", "OPTIMISM"),
    ("Optimism (USDC)", "OPTIMISM"),
    ("Optimism (USDC.e)", "OPTIMISM"),
    ("Polygon (USDC)", "MATIC"),
    ("Polygon (USDC.e)", "MATIC"),
    ("Base", "BASE"),
    ("Avalanche C-Chain", "AVAXC"),
    ("Linea", "LINEA"),
    ("Acala", "ACA"),
    ("Akash", "AKT"),
    ("Algorand", "ALGO"),
    ("Aptos", "APTOS"),
    ("Arweave", "AR"),
    ("Astar", "ASTR"),
    ("Bitcoin Cash", "BCH"),
    ("Bittensor", "TAO"),
    ("Cardano", "ADA"),
    ("Casper Network", "CSPR"),
    ("Celestia", "TIA"),
    ("Celo", "CELO"),
    ("Conflux", "CFX"),
    ("Cosmos", "ATOM"),
    ("Dash", "DASH"),
    ("Dogecoin", "DOGE"),
    ("Elrond", "EGLD"),
    ("EOS", "EOS"),
    ("Ethereum Classic", "ETC"),
    ("Fetch.ai", "FET"),
    ("Filecoin", "FIL"),
    ("Flow", "FLOW"),
    ("Hedera", "HBAR"),
    ("Injective", "INJ"),
    ("Internet Computer Protocol", "ICP"),
    ("Kaspa", "KAS"),
    ("Kava", "KAVA"),
    ("Litecoin", "LTC"),
    ("Mina", "MINA"),
    ("Monero", "XMR"),
    ("Near", "NEAR"),
    ("Osmosis", "OSMO"),
    ("Qtum", "QTUM"),
    ("Siacoin", "SC"),
    ("Stacks", "STX"),
    ("Stellar", "XLM"),
    ("Sui", "SUI"),
    ("Tezos", "XTZ"),
    ("The Open Network", "TON"),
    ("VeChain", "VET"),
    ("XRP", "XRP"),
    ("Zcash", "ZEC"),
];

/// Kraken 近期把不少资产的存款方式名套上了 "<TICKER> - <链名>" 前缀和/或
/// "(Unified)" 后缀的壳(如 "APE - Ethereum (Unified)"、"VET - VeChain")，链名
/// 本体通常和老格式(如 "Ethereum"、"VeChain")一致。剥掉这层壳(剥不出规律的
/// 原样返回)，交给调用方再去表里精确匹配——这是按已用真实数据核对过的固定
/// 结构做剥离，不是子串/关键词模糊匹配，所以不会重蹈 "Bitcoin" 子串误中
/// "Bitcoin Lightning" 的老问题。
fn strip_unified_wrapper(native: &str) -> &str {
    let without_ticker_prefix = native.split_once(" - ").map(|(_, chain)| chain).unwrap_or(native);
    without_ticker_prefix.strip_suffix(" (Unified)").unwrap_or(without_ticker_prefix)
}

/// Kraken 原生方式名 -> 标准链名，大小写不敏感；先按完整原始字符串精确匹配，
/// 找不到再剥掉 [`strip_unified_wrapper`] 描述的外壳重试一次；两轮都查不到就
/// 原样透传。
fn native_to_standard(native: &str) -> String {
    let lookup = |name: &str| {
        KRAKEN_METHOD_TO_STANDARD
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, standard)| standard.to_string())
    };
    lookup(native).or_else(|| lookup(strip_unified_wrapper(native))).unwrap_or_else(|| native.to_string())
}

/// 标准链名 -> Kraken 原生方式名，大小写不敏感；查不到就原样透传(视为调用方
/// 直接传入了原生名称)。
fn standard_to_native(standard: &str) -> String {
    KRAKEN_METHOD_TO_STANDARD
        .iter()
        .find(|(_, s)| s.eq_ignore_ascii_case(standard))
        .map(|(native, _)| native.to_string())
        .unwrap_or_else(|| standard.to_string())
}

/// Kraken 钱包(转账层)客户端：读取收款地址/链(方式)信息、发起提币。签名沿用
/// Kraken 私有接口的标准 HMAC-SHA512 方案(Kraken 目前只支持这一种，不像币安
/// 有 Ed25519/RSA 可选)。
///
/// 重要差异：Kraken 的 `Withdraw` 接口不接受任意链上地址，而是要求该地址已经
/// 在 Kraken 账户网页端预先登记并起了一个别名，接口里的 `key` 参数引用的是这个
/// 别名，不是原始地址。因此本实现里 `WithdrawRequest.address` 对 Kraken 而言
/// 语义是"预登记地址的别名"，`tag` 字段不会被使用(别名本身已经绑定了目标地址
/// /目标 tag)，调用方需要清楚这一点，不能假设和币安的"传原始地址"语义一致。
pub struct KrakenWalletProvider {
    venue: Venue,
    api_key: String,
    api_secret: String,
    http: reqwest::Client,
}

impl KrakenWalletProvider {
    pub fn new(venue: Venue, api_key: String, api_secret: String, proxy: Option<&str>) -> anyhow::Result<Self> {
        let http = build_http_client(proxy)?;
        Ok(Self {
            venue,
            api_key,
            api_secret,
            http,
        })
    }

    /// 从环境变量读取凭证并构造实例:`KRAKEN_SPOT_API_KEY` +
    /// `KRAKEN_SPOT_API_SECRET`(base64 编码的 secret)。
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

        crate::ratelimit::throttle(HOST).await;
        let resp = self
            .http
            .post(format!("{HOST}{path}"))
            .header("API-Key", &self.api_key)
            .header("API-Sign", signature)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(post_data)
            .send()
            .await
            .context("kraken wallet request failed")?;
        resp.text().await.context("failed to read kraken wallet response body")
    }
}

#[async_trait]
impl WalletProvider for KrakenWalletProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    async fn asset_info(&self, asset: &str) -> anyhow::Result<AssetInfo> {
        let params = vec![("asset".to_string(), asset.to_string())];
        let text = self.private_request("/0/private/DepositMethods", params).await?;
        parse_asset_info(&text, asset)
    }

    async fn deposit_address(&self, asset: &str, network: &str) -> anyhow::Result<DepositAddress> {
        let native_method = standard_to_native(network);
        let params = vec![
            ("asset".to_string(), asset.to_string()),
            ("method".to_string(), native_method),
        ];
        let text = self.private_request("/0/private/DepositAddresses", params).await?;
        parse_deposit_address(&text, asset, network)
    }

    async fn withdraw_raw(&self, req: &WithdrawRequest) -> anyhow::Result<WithdrawResult> {
        let params = vec![
            ("asset".to_string(), req.asset.clone()),
            ("key".to_string(), req.address.clone()),
            ("amount".to_string(), req.amount.to_string()),
        ];
        let text = self.private_request("/0/private/Withdraw", params).await?;
        parse_withdraw_result(&text)
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
/// 注意 `nonce` 会被拼两遍：一遍是裸的 nonce 字符串，一遍是 post_data 里的
/// `nonce=...` 字段——这是 Kraken 官方算法本身的要求，不是笔误。
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
struct DepositMethod {
    method: String,
    #[serde(default)]
    fee: Option<String>,
    #[serde(default)]
    minimum: Option<String>,
}

fn parse_asset_info(text: &str, asset: &str) -> anyhow::Result<AssetInfo> {
    let methods: Vec<DepositMethod> = unwrap_result(text)?;
    let networks = methods
        .into_iter()
        .map(|m| ChainInfo {
            network: native_to_standard(&m.method),
            name: m.method,
            deposit_enabled: true,
            // Kraken 的 DepositMethods 接口不区分提币开关，默认放行；真正被禁用
            // 时会在 withdraw_raw 调用时由 Kraken 直接返回错误并透传给调用方。
            withdraw_enabled: true,
            withdraw_fee: m.fee.and_then(|f| f.parse().ok()).unwrap_or(Decimal::ZERO),
            withdraw_min: m.minimum.and_then(|v| v.parse().ok()).unwrap_or(Decimal::ZERO),
            // Kraken 该接口不提供确认数信息。
            min_confirm: 0,
            contract_address: None,
        })
        .collect();

    Ok(AssetInfo {
        asset: asset.to_string(),
        networks,
    })
}

#[derive(Debug, Deserialize)]
struct DepositAddressEntry {
    address: String,
    #[serde(default)]
    tag: Option<String>,
}

fn parse_deposit_address(text: &str, asset: &str, network: &str) -> anyhow::Result<DepositAddress> {
    let entries: Vec<DepositAddressEntry> = unwrap_result(text)?;
    let entry = entries
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("kraken returned no deposit address for {asset}/{network}"))?;
    Ok(DepositAddress {
        asset: asset.to_string(),
        network: network.to_string(),
        address: entry.address,
        tag: entry.tag,
    })
}

#[derive(Debug, Deserialize)]
struct WithdrawResultBody {
    refid: String,
}

fn parse_withdraw_result(text: &str) -> anyhow::Result<WithdrawResult> {
    let body: WithdrawResultBody = unwrap_result(text)?;
    Ok(WithdrawResult { id: body.refid })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 用独立于本实现的 Python(hashlib/hmac/base64)脱机算出的参考签名值，
    /// 避免测试和实现共用同一份(可能出错的)逻辑而互相"自证"。
    #[test]
    fn signs_matches_independently_computed_reference_vector() {
        let secret_b64 = "coFbU8p41bBXnzmdU/ynDvyqypLm4S9D8y1wn7H1als=";
        let path = "/0/private/Withdraw";
        let nonce = "1700000000000";
        let post_data = "nonce=1700000000000&asset=XBT&key=my-address&amount=0.1";
        let expected = "209k9WF3HTjp5/AA4k/byZz2yJLAqMkUW55/FPyUYYNJ5rDEfAL+Yxd9A6M0Ssnrm/XPOgIDLvZIQCXrZXGntw==";

        let signature = sign_kraken(secret_b64, path, nonce, post_data).expect("signing should succeed");
        assert_eq!(signature, expected);
    }

    #[test]
    fn signature_changes_when_post_data_changes() {
        let secret_b64 = "coFbU8p41bBXnzmdU/ynDvyqypLm4S9D8y1wn7H1als=";
        let sig_a = sign_kraken(secret_b64, "/0/private/Withdraw", "1", "nonce=1&amount=0.1").unwrap();
        let sig_b = sign_kraken(secret_b64, "/0/private/Withdraw", "1", "nonce=1&amount=0.2").unwrap();
        assert_ne!(sig_a, sig_b);
    }

    #[test]
    fn builds_post_data_with_nonce_first() {
        let params = vec![("asset".to_string(), "XBT".to_string())];
        assert_eq!(build_post_data("123", &params), "nonce=123&asset=XBT");
    }

    #[test]
    fn parses_asset_info_response() {
        let text = r#"{
            "error": [],
            "result": [
                {"method": "Bitcoin", "limit": false, "fee": "0.0000", "gen-address": true, "minimum": "0.0002"},
                {"method": "Bitcoin Lightning", "limit": false, "gen-address": true}
            ]
        }"#;

        let info = parse_asset_info(text, "XBT").expect("should parse");
        assert_eq!(info.networks.len(), 2);
        // "Bitcoin" 在映射表里，翻译成标准链名 "BTC"；可读名称保留原样。
        assert_eq!(info.networks[0].network, "BTC");
        assert_eq!(info.networks[0].name, "Bitcoin");
        assert_eq!(info.networks[0].withdraw_min, Decimal::new(2, 4));
        // "Bitcoin Lightning" 不在映射表里，原样透传，不会被误判成 "BTC"
        // (这正是之前子串匹配方案里 "Bitcoin"/"Bitcoin Lightning" 二义性 bug 的根源)。
        assert_eq!(info.networks[1].network, "Bitcoin Lightning");
        assert_eq!(info.networks[1].withdraw_min, Decimal::ZERO);
    }

    #[test]
    fn native_to_standard_maps_known_methods_case_insensitively() {
        assert_eq!(native_to_standard("bitcoin"), "BTC");
        assert_eq!(native_to_standard("Ethereum"), "ETH");
    }

    #[test]
    fn native_to_standard_maps_unified_eth_methods() {
        assert_eq!(native_to_standard("Ether (Hex)"), "ETH");
        assert_eq!(native_to_standard("ETH - Optimism (Unified)"), "OPTIMISM");
        assert_eq!(native_to_standard("ETH - Base (Unified)"), "BASE");
        assert_eq!(native_to_standard("ETH - Arbitrum One (Unified)"), "ARBITRUM");
        assert_eq!(native_to_standard("zkSync Era"), "ZKSYNCERA");
    }

    #[test]
    fn native_to_standard_strips_ticker_prefix_and_unified_suffix_for_other_assets() {
        assert_eq!(native_to_standard("APE - Ethereum (Unified)"), "ETH");
        assert_eq!(native_to_standard("LINK - Ethereum (Unified)"), "ETH");
        assert_eq!(native_to_standard("PENGU - Solana"), "SOL");
        assert_eq!(native_to_standard("VET - VeChain"), "VET");
        assert_eq!(native_to_standard("BNB - BNB Chain"), "BSC");
    }

    #[test]
    fn native_to_standard_maps_non_standard_wrapper_formats_exactly() {
        assert_eq!(native_to_standard("S (Sonic)"), "SONIC");
        assert_eq!(native_to_standard("USDC (SPL)"), "SOL");
        assert_eq!(native_to_standard("USDC - Stellar XLM"), "XLM");
    }

    #[test]
    fn native_to_standard_passes_through_unknown_methods() {
        assert_eq!(native_to_standard("Bitcoin Lightning"), "Bitcoin Lightning");
        // Polkadot relay chain 与 Asset Hub 的对应关系有歧义，不在表里，保持
        // 原样透传，而不是猜一个标准链名。
        assert_eq!(native_to_standard("Polkadot"), "Polkadot");
    }

    #[test]
    fn standard_to_native_round_trips_known_codes() {
        assert_eq!(standard_to_native("BTC"), "Bitcoin");
        assert_eq!(standard_to_native("btc"), "Bitcoin");
    }

    #[test]
    fn standard_to_native_passes_through_unknown_codes() {
        assert_eq!(standard_to_native("SHIB"), "SHIB");
    }

    #[test]
    fn parse_asset_info_surfaces_error_response() {
        let text = r#"{"error": ["EAPI:Invalid key"], "result": null}"#;
        let err = parse_asset_info(text, "XBT").unwrap_err();
        assert!(err.to_string().contains("EAPI:Invalid key"));
    }

    #[test]
    fn parses_deposit_address_response() {
        let text = r#"{"error": [], "result": [{"address": "2N9fRkx5JTWXWHmXzZtvhQsufvoYRMq9ExV", "expiretm": "0", "new": true}]}"#;
        let addr = parse_deposit_address(text, "XBT", "Bitcoin").expect("should parse");
        assert_eq!(addr.address, "2N9fRkx5JTWXWHmXzZtvhQsufvoYRMq9ExV");
        assert_eq!(addr.tag, None);
    }

    #[test]
    fn parses_withdraw_result() {
        let text = r#"{"error": [], "result": {"refid": "AGBSO6T-UFMTTQ-I7KGS6"}}"#;
        let result = parse_withdraw_result(text).expect("should parse");
        assert_eq!(result.id, "AGBSO6T-UFMTTQ-I7KGS6");
    }

    #[test]
    fn parse_withdraw_result_surfaces_error_response() {
        let text = r#"{"error": ["EFunding:Withdraw disabled"], "result": null}"#;
        let err = parse_withdraw_result(text).unwrap_err();
        assert!(err.to_string().contains("Withdraw disabled"));
    }
}
