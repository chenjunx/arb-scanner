use anyhow::Context;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// 从环境变量 ARB_SCANNER_PROXY 读取代理地址("host:port",如 "127.0.0.1:7890")。
/// 本地开发时在终端/`.env` 里设一次即可让所有出网连接走代理;生产服务器不设置该
/// 变量就天然直连,不需要为部署环境改动任何配置文件,也不用担心把本地代理地址
/// 误提交进 config.toml 带到生产。
pub fn proxy_from_env() -> Option<String> {
    std::env::var("ARB_SCANNER_PROXY")
        .ok()
        .filter(|v| !v.trim().is_empty())
}

/// 建立一条到 target_host:target_port 的 TCP 连接。若提供了 proxy_addr("host:port"),
/// 通过该地址的 HTTP CONNECT 隧道建连;否则直连。所有需要访问真实交易所接口的
/// 数据源/客户端都应通过这个函数建连,这样配置了代理时统一走代理出网,不用每个
/// 交易所各自实现一遍。
pub async fn connect_tcp(
    target_host: &str,
    target_port: u16,
    proxy_addr: Option<&str>,
) -> anyhow::Result<TcpStream> {
    match proxy_addr {
        Some(proxy_addr) => http_connect_tunnel(proxy_addr, target_host, target_port).await,
        None => TcpStream::connect((target_host, target_port))
            .await
            .with_context(|| format!("failed to connect to {target_host}:{target_port}")),
    }
}

async fn http_connect_tunnel(
    proxy_addr: &str,
    target_host: &str,
    target_port: u16,
) -> anyhow::Result<TcpStream> {
    let mut stream = TcpStream::connect(proxy_addr)
        .await
        .with_context(|| format!("failed to connect to proxy {proxy_addr}"))?;

    let target = format!("{target_host}:{target_port}");
    let request = format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .context("failed to send CONNECT request to proxy")?;

    let status_line = read_http_status_line(&mut stream).await?;
    if !status_line.contains(" 200") {
        anyhow::bail!("proxy refused CONNECT to {target}: {status_line}");
    }
    Ok(stream)
}

/// 逐字节读到响应头结束(\r\n\r\n),只取首行状态行。必须逐字节读而不能用
/// BufReader 再丢弃缓冲区——CONNECT 成功后紧跟着的字节属于上层 TLS 握手,
/// 多读一个字节都会导致 TLS ClientHello 少收到数据。
async fn read_http_status_line(stream: &mut TcpStream) -> anyhow::Result<String> {
    let mut header = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = stream
            .read(&mut byte)
            .await
            .context("failed reading proxy CONNECT response")?;
        if n == 0 {
            anyhow::bail!("proxy closed connection during CONNECT handshake");
        }
        header.push(byte[0]);
        if header.len() > 8192 {
            anyhow::bail!("proxy CONNECT response header too large");
        }
        if header.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let text = String::from_utf8_lossy(&header);
    Ok(text.lines().next().unwrap_or_default().to_string())
}
