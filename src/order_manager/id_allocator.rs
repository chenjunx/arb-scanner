use std::sync::atomic::{AtomicU64, Ordering};

/// 分配全局唯一的订单序号。`RedisOrderStore` 以 order_id 作为 Redis Hash 的
/// field(见 `redis_store.rs`)，如果不同进程生成的 order_id 重复，后写入的
/// 会直接覆盖前一笔的历史订单记录。用跨进程共享的分配器（如 Redis INCR）
/// 替代进程内 `AtomicU64`，保证一次性 CLI 命令(`open`)每次重启都能拿到
/// 递增、不重复的序号。
pub trait OrderIdAllocator: Send + Sync {
    /// 返回下一个全局序号；分配失败（如 Redis 连接中断）时返回 `None`，
    /// 调用方退化为本地兜底方案，而不是让下单直接失败。
    fn next(&self) -> Option<u64>;
}

/// 纯内存实现，只在进程内唯一，用于测试。
#[derive(Default)]
pub struct InMemoryOrderIdAllocator {
    seq: AtomicU64,
}

impl InMemoryOrderIdAllocator {
    pub fn new() -> Self {
        Self::default()
    }
}

impl OrderIdAllocator for InMemoryOrderIdAllocator {
    fn next(&self) -> Option<u64> {
        Some(self.seq.fetch_add(1, Ordering::SeqCst) + 1)
    }
}
