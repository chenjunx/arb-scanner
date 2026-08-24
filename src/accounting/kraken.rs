use std::sync::Arc;

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use log::{debug, warn};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::net::connect_tcp;
use crate::order::kraken::{
    ChannelEnvelope, MAX_BACKOFF, MIN_BACKOFF, WS_HOST, WS_PORT, build_http_client, kraken_private_request, parse_ws_token,
};
use crate::order_manager::stream::StreamHandle;
use crate::topic::{Topic, TopicBus};
use crate::types::Venue;

use super::balance_stream::{BalanceStreamSource, BalanceUpdate};

/// Kraken 私有余额变动流客户端：与 `order::kraken::KrakenPrivateOrderStream`
/// 共用同一套 `GetWebSocketsToken` 取 token / 连接 / 指数退避重连逻辑(那些
/// helper 在 `order::kraken` 里标了 `pub(crate)` 供这里复用)，唯一区别是订阅
/// `balances` channel 并把入金流水转换为 `BalanceUpdate` 发布到 `TopicBus`，
/// 供 `TransferMonitor` 消费做到账确认。
pub struct KrakenBalanceStream {
    venue: Venue,
    api_key: String,
    api_secret: String,
    http: reqwest::Client,
    proxy: Option<String>,
}

impl KrakenBalanceStream {
    pub fn new(venue: Venue, api_key: String, api_secret: String, proxy: Option<&str>) -> anyhow::Result<Self> {
        let http = build_http_client(proxy)?;
        Ok(Self {
            venue,
            api_key,
            api_secret,
            http,
            proxy: proxy.map(str::to_string),
        })
    }

    /// 和 `KrakenOrderProvider::from_env`/`KrakenPrivateOrderStream::from_env` 复用同一套凭证环境变量。
    pub fn from_env(venue: Venue, proxy: Option<&str>) -> anyhow::Result<Self> {
        let api_key = std::env::var("KRAKEN_SPOT_API_KEY").context("KRAKEN_SPOT_API_KEY not set")?;
        let api_secret = std::env::var("KRAKEN_SPOT_API_SECRET").context("KRAKEN_SPOT_API_SECRET not set")?;
        Self::new(venue, api_key, api_secret, proxy)
    }

    async fn fetch_token(&self) -> anyhow::Result<String> {
        let text = kraken_private_request(&self.http, &self.api_key, &self.api_secret, "/0/private/GetWebSocketsToken", vec![]).await?;
        parse_ws_token(&text)
    }

    async fn connect(&self, token: &str) -> anyhow::Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>> {
        let tcp = connect_tcp(WS_HOST, WS_PORT, self.proxy.as_deref()).await?;
        let url = format!("wss://{WS_HOST}/v2");
        let (mut ws, _) = tokio_tungstenite::client_async_tls(url, tcp)
            .await
            .context("kraken balance stream handshake failed")?;

        let subscribe = serde_json::json!({
            "method": "subscribe",
            "params": {
                "channel": "balances",
                "token": token,
            }
        });
        ws.send(Message::Text(subscribe.to_string()))
            .await
            .context("failed to send kraken balances subscribe message")?;
        Ok(ws)
    }
}

impl BalanceStreamSource for KrakenBalanceStream {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    fn spawn(self: Box<Self>, bus: Arc<TopicBus>) -> StreamHandle {
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let join = tokio::spawn(async move {
            let mut backoff = MIN_BACKOFF;
            let mut ready_tx = Some(ready_tx);

            loop {
                let token = match self.fetch_token().await {
                    Ok(token) => token,
                    Err(err) => {
                        warn!(
                            "kraken balance stream: failed to fetch ws token for venue={} err={err:#}, retrying in {:?}",
                            self.venue, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };

                let mut ws = match self.connect(&token).await {
                    Ok(ws) => ws,
                    Err(err) => {
                        warn!(
                            "kraken balance stream connect failed for venue={} err={err:#}, retrying in {:?}",
                            self.venue, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };
                debug!("kraken balance stream connected for venue={}", self.venue);
                backoff = MIN_BACKOFF;
                if let Some(ready_tx) = ready_tx.take() {
                    let _ = ready_tx.send(());
                }

                while let Some(msg) = ws.next().await {
                    let msg = match msg {
                        Ok(msg) => msg,
                        Err(err) => {
                            warn!("kraken balance stream error for venue={} err={err}", self.venue);
                            break;
                        }
                    };
                    let Message::Text(text) = msg else { continue };
                    for update in parse_kraken_balance_update(&text, &self.venue) {
                        bus.publish(Topic::balance_update(self.venue.clone()), update);
                    }
                }

                warn!(
                    "kraken balance stream disconnected for venue={}, reconnecting in {:?}",
                    self.venue, backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        });
        StreamHandle { join, ready: ready_rx }
    }
}

#[derive(Debug, Deserialize)]
struct BalancesEnvelope {
    #[serde(rename = "type", default)]
    msg_type: String,
    #[serde(default)]
    data: Vec<KrakenBalanceData>,
}

#[derive(Debug, Deserialize)]
struct KrakenBalanceData {
    asset: String,
    /// 流水类型：deposit/withdrawal/trade/... 只有 `deposit` 才当作到账处理，
    /// 见 `parse_kraken_balance_update`。
    #[serde(rename = "type", default)]
    entry_type: Option<String>,
    #[serde(default)]
    amount: Decimal,
}

/// 解析一条 WebSocket v2 `balances` channel 消息，只关心 `type: "update"` 且
/// `data[].type == "deposit"` 的入金流水；忽略订阅确认里 `type: "snapshot"`
/// 的初始全量快照，以及 trade/withdrawal 等其它类型的流水条目——和
/// `BinanceBalanceStream` 只转发 `balanceUpdate`(不含交易本身)保持同等语义。
/// 时间戳沿用 `parse_kraken_execution` 的约定：用本地收到消息的时间，不解析
/// 该 channel 用的 ISO8601 字符串。纯函数，不依赖真实 WebSocket 连接，便于
/// 单元测试。
fn parse_kraken_balance_update(text: &str, venue: &Venue) -> Vec<BalanceUpdate> {
    let envelope: ChannelEnvelope = match serde_json::from_str(text) {
        Ok(envelope) => envelope,
        Err(_) => return Vec::new(),
    };
    if envelope.channel.as_deref() != Some("balances") {
        return Vec::new();
    }
    let full: BalancesEnvelope = match serde_json::from_str(text) {
        Ok(full) => full,
        Err(err) => {
            warn!("failed to parse kraken balances message: {err}");
            return Vec::new();
        }
    };
    if full.msg_type != "update" {
        return Vec::new();
    }

    let ts_ms = crate::market_data::now_ms();
    full.data
        .into_iter()
        .filter(|item| item.entry_type.as_deref() == Some("deposit"))
        .map(|item| BalanceUpdate {
            venue: venue.clone(),
            asset: item.asset,
            delta: item.amount,
            ts_ms,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_balances_update_with_deposit_entry() {
        let venue = Venue::new("kraken");
        let text = r#"{
            "channel": "balances",
            "type": "update",
            "data": [
                {
                    "ledger_id": "ADKKFF-WEA5A-CNUBHG",
                    "ref_id": "AGBWUJRU-LAREZ-W3UFAN",
                    "timestamp": "2023-09-22T10:23:42.925034Z",
                    "type": "deposit",
                    "asset": "BTC",
                    "asset_class": "currency",
                    "category": "deposit",
                    "wallet_type": "spot",
                    "wallet_id": "main",
                    "amount": 0.01,
                    "fee": 0.0,
                    "balance": 0.02
                }
            ],
            "sequence": 2
        }"#;
        let updates = parse_kraken_balance_update(text, &venue);
        assert_eq!(updates.len(), 1);
        let update = &updates[0];
        assert_eq!(update.venue, venue);
        assert_eq!(update.asset, "BTC");
        assert_eq!(update.delta, "0.01".parse().unwrap());
    }

    #[test]
    fn ignores_balances_snapshot_message() {
        let venue = Venue::new("kraken");
        let text = r#"{
            "channel": "balances",
            "type": "snapshot",
            "data": [
                {"asset": "BTC", "asset_class": "currency", "balance": 1.2}
            ],
            "sequence": 1
        }"#;
        assert!(parse_kraken_balance_update(text, &venue).is_empty());
    }

    #[test]
    fn filters_out_non_deposit_ledger_entries() {
        let venue = Venue::new("kraken");
        let text = r#"{
            "channel": "balances",
            "type": "update",
            "data": [
                {"asset": "BTC", "type": "trade", "amount": 0.5, "balance": 1.0},
                {"asset": "BTC", "type": "withdrawal", "amount": -0.1, "balance": 0.9},
                {"asset": "ETH", "type": "deposit", "amount": 2.0, "balance": 2.0}
            ],
            "sequence": 3
        }"#;
        let updates = parse_kraken_balance_update(text, &venue);
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].asset, "ETH");
        assert_eq!(updates[0].delta, "2.0".parse().unwrap());
    }

    #[test]
    fn ignores_non_balances_channel_messages() {
        let venue = Venue::new("kraken");
        assert!(parse_kraken_balance_update(r#"{"channel":"heartbeat"}"#, &venue).is_empty());
        assert!(parse_kraken_balance_update(
            r#"{"method":"subscribe","success":true,"result":{"channel":"balances","snapshot":true}}"#,
            &venue
        )
        .is_empty());
    }

    #[test]
    fn ignores_malformed_balances_message() {
        let venue = Venue::new("kraken");
        assert!(parse_kraken_balance_update("not json", &venue).is_empty());
    }
}
