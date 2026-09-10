//! 可执行入口：只做进程级初始化，命令行解析和子命令实现都在
//! [`arb_scanner::cli`]（放在 lib 里，方便被集成测试直接构造/断言）。

use arb_scanner::{cli, logging};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::init_logging();

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    cli::run().await
}
