/// 初始化日志：读取 RUST_LOG 环境变量控制级别，默认为 info。
pub fn init_logging() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
}
