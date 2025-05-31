# cursorcache

基于 Redis 分布式缓存和布隆过滤器的游标分页缓存组件，支持高并发场景下的分页数据缓存、击穿保护和缓存穿透优化。

## 特性

- 支持游标分页（next/prev 双向）
- Redis 缓存分页结果，自动 TTL 过期
- 防止缓存击穿（singleflight）
- 可选布隆过滤器，减少无效请求
- 支持高并发安全
- 兼容泛型数据类型
- 支持自定义 TTL 抖动，防止缓存雪崩

## 安装

```shell
go get github.com/zihoulaai/cursorcache
