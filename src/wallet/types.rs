use rust_decimal::Decimal;

/// 某个币种在某条链/网络上的转账相关信息。
#[derive(Debug, Clone, PartialEq)]
pub struct ChainInfo {
    /// 标准链名，以币安的 `network` 代码为准（如 "BSC"、"ETH"、"BTC"）。
    /// 非币安交易所的 `WalletProvider` 实现负责把自己的原生链名翻译成这个
    /// 标准（见各交易所模块内的映射表，如 `wallet::kraken`），调用方可以
    /// 直接按这个字段做精确匹配，不需要再做任何模糊匹配。
    pub network: String,
    /// 网络可读名称。
    pub name: String,
    pub deposit_enabled: bool,
    pub withdraw_enabled: bool,
    pub withdraw_fee: Decimal,
    pub withdraw_min: Decimal,
    pub min_confirm: u32,
    /// 若该币种在这条链上是合约代币，则为合约地址。
    pub contract_address: Option<String>,
}

/// 某个币种支持的全部链/网络信息。
#[derive(Debug, Clone, PartialEq)]
pub struct AssetInfo {
    pub asset: String,
    pub networks: Vec<ChainInfo>,
}

/// 某个币种在某条链上的收款地址。
#[derive(Debug, Clone, PartialEq)]
pub struct DepositAddress {
    pub asset: String,
    pub network: String,
    pub address: String,
    /// XRP/EOS 等链需要的 memo/tag。
    pub tag: Option<String>,
}

/// 提币请求。注意 `address` 在不同交易所语义可能不同——例如 Kraken 要求引用
/// 账户里预先登记好的地址别名，而不是链上原始地址，具体见各交易所实现的说明。
#[derive(Debug, Clone, PartialEq)]
pub struct WithdrawRequest {
    pub asset: String,
    pub network: String,
    pub address: String,
    pub tag: Option<String>,
    pub amount: Decimal,
    /// true 时只做签名前的校验，不真正发起提币请求。
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WithdrawResult {
    pub id: String,
}
