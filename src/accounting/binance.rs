use std::sync::Arc;

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use log::{debug, warn};
use ring::signature::Ed25519KeyPair;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::market_data::now_ms;
use crate::net::connect_tcp;
use crate::order::binance::{
    MAX_BACKOFF, MIN_BACKOFF, UserDataEventEnvelope, WS_API_MAINNET_HOST, WS_API_PATH, WS_API_PORT, WS_API_TESTNET_HOST,
    load_ed25519_key, percent_encode, send_ws_api_request, sign_ed25519,
};
use crate::order_manager::stream::StreamHandle;
use crate::topic::{Topic, TopicBus};
use crate::types::Venue;

use super::balance_stream::{BalanceStreamSource, BalanceUpdate};

/// 币安现货余额变动流：和 `order::binance::BinanceUserDataStream` 共用同一套
/// WS API 连接/鉴权方式(那些 helper 在 `order::binance` 里标了 `pub(crate)`
/// 供这里复用)，仅关心 `balanceUpdate` 事件，将其转换为 `BalanceUpdate` 后
/// 通过 `TopicBus` 发布。划转到账确认由 `TransferMonitor` 消费。
pub struct BinanceBalanceStream {
    venue: Venue,
    api_key: String,
    key_pair: Ed25519KeyPair,
    ws_host: &'static str,
    ws_port: u16,
    proxy: Option<String>,
}

impl BinanceBalanceStream {
    pub fn new(
        venue: Venue,
        api_key: String,
        private_key_pem: &str,
        testnet: bool,
        proxy: Option<&str>,
    ) -> anyhow::Result<Self> {
        let key_pair = load_ed25519_key(private_key_pem)?;
        let ws_host = if testnet { WS_API_TESTNET_HOST } else { WS_API_MAINNET_HOST };
        Ok(Self {
            venue,
            api_key,
            key_pair,
            ws_host,
            ws_port: WS_API_PORT,
            proxy: proxy.map(str::to_string),
        })
    }

    pub fn from_env(venue: Venue, testnet: bool, proxy: Option<&str>) -> anyhow::Result<Self> {
        let api_key = std::env::var("BINANCE_API_KEY").context("BINANCE_API_KEY not set")?;
        let private_key_pem = std::env::var("BINANCE_API_SECRET").context("BINANCE_API_SECRET not set")?;
        Self::new(venue, api_key, &private_key_pem, testnet, proxy)
    }

    async fn connect(&self) -> anyhow::Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>> {
        let tcp = connect_tcp(self.ws_host, self.ws_port, self.proxy.as_deref()).await?;
        let url = format!("wss://{}:{}{}", self.ws_host, self.ws_port, WS_API_PATH);
        let (ws, _) = tokio_tungstenite::client_async_tls(url, tcp)
            .await
            .context("binance balance stream handshake failed")?;
        Ok(ws)
    }

    fn sign_ws_params(&self, params: &[(&str, &str)]) -> String {
        let mut sorted = params.to_vec();
        sorted.sort_by_key(|(k, _)| *k);
        let payload = sorted
            .iter()
            .map(|(k, v)| format!("{k}={}", percent_encode(v)))
            .collect::<Vec<_>>()
            .join("&");
        sign_ed25519(&self.key_pair, &payload)
    }

    async fn session_logon(
        &self,
        ws: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    ) -> anyhow::Result<()> {
        let timestamp = now_ms();
        let timestamp_str = timestamp.to_string();
        let signature = self.sign_ws_params(&[("apiKey", &self.api_key), ("timestamp", &timestamp_str)]);
        let req = serde_json::json!({
            "id": "logon",
            "method": "session.logon",
            "params": {
                "apiKey": self.api_key,
                "signature": signature,
                "timestamp": timestamp,
            }
        });
        send_ws_api_request(ws, "logon", &req).await.context("session.logon failed")
    }

    async fn subscribe_user_data(
        &self,
        ws: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    ) -> anyhow::Result<()> {
        let req = serde_json::json!({"id": "sub", "method": "userDataStream.subscribe"});
        send_ws_api_request(ws, "sub", &req).await.context("userDataStream.subscribe failed")
    }
}

#[derive(Debug, Deserialize)]
struct BalanceUpdatePayload {
    #[serde(rename = "a")]
    asset: String,
    #[serde(rename = "d")]
    delta: Decimal,
    #[serde(rename = "E")]
    event_time_ms: u64,
}

/// `balanceUpdate` 事件解析：取出 WS API 包装层的 `event` 字段后检查类型，
/// 只处理 `balanceUpdate`，其它事件（`executionReport` 等）返回 `None`。
fn parse_balance_update(text: &str, venue: &Venue) -> Option<BalanceUpdate> {
    let raw: serde_json::Value = serde_json::from_str(text).ok()?;
    let event = raw.get("event").cloned().unwrap_or(raw);

    let envelope: UserDataEventEnvelope = serde_json::from_value(event.clone()).ok()?;
    if envelope.event_type != "balanceUpdate" {
        return None;
    }

    let payload: BalanceUpdatePayload = match serde_json::from_value(event) {
        Ok(p) => p,
        Err(err) => {
            warn!("failed to parse binance balanceUpdate: {err}");
            return None;
        }
    };

    Some(BalanceUpdate {
        venue: venue.clone(),
        asset: payload.asset,
        delta: payload.delta,
        ts_ms: payload.event_time_ms,
    })
}

impl BalanceStreamSource for BinanceBalanceStream {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    fn spawn(self: Box<Self>, bus: Arc<TopicBus>) -> StreamHandle {
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let join = tokio::spawn(async move {
            let mut backoff = MIN_BACKOFF;
            let mut ready_tx = Some(ready_tx);

            loop {
                let mut ws = match self.connect().await {
                    Ok(ws) => ws,
                    Err(err) => {
                        warn!(
                            "binance balance stream connect failed for venue={} err={err:#}, retrying in {:?}",
                            self.venue, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };

                if let Err(err) = self.session_logon(&mut ws).await {
                    warn!(
                        "binance balance stream: session.logon failed for venue={} err={err:#}, retrying in {:?}",
                        self.venue, backoff
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }
                if let Err(err) = self.subscribe_user_data(&mut ws).await {
                    warn!(
                        "binance balance stream: userDataStream.subscribe failed for venue={} err={err:#}, retrying in {:?}",
                        self.venue, backoff
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }
                debug!("binance balance stream connected and subscribed for venue={}", self.venue);
                backoff = MIN_BACKOFF;
                if let Some(tx) = ready_tx.take() {
                    let _ = tx.send(());
                }

                loop {
                    let msg = match ws.next().await {
                        Some(Ok(msg)) => msg,
                        Some(Err(err)) => {
                            warn!("binance balance stream error for venue={} err={err}", self.venue);
                            break;
                        }
                        None => break,
                    };
                    match msg {
                        Message::Ping(payload) => {
                            if ws.send(Message::Pong(payload)).await.is_err() {
                                break;
                            }
                        }
                        Message::Text(text) => {
                            let Some(update) = parse_balance_update(&text, &self.venue) else { continue };
                            bus.publish(Topic::balance_update(self.venue.clone()), update);
                        }
                        _ => {}
                    }
                }

                warn!(
                    "binance balance stream disconnected for venue={}, reconnecting in {:?}",
                    self.venue, backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        });
        StreamHandle { join, ready: ready_rx }
    }
}
