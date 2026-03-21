# Space Logger OLAP 3D 数据库架构设计

## 1. 目标与约束
- 输入模型：`x:int, y:int, z:int, subject:string, object:string, verb:string, time_ms:long, subjectExtra:string, data:blob`
- 可查询 key：除 `subjectExtra` 与 `data` 之外全部列。
- 读多写少，优先 OLAP 读取性能。
- 写入需要 WAL 保证崩溃恢复。
- 写入结构采用 LSM-Tree（MemTable + immutable segment）。
- 并发：
  - 读：支持并发读。
  - 写：低并发，使用乐观锁（版本号 compare）。
  - 不提供事务隔离。

## 2. 存储分层

### 2.1 WAL 层
- 文件：`<db_dir>/wal.log`
- 每条写入先追加到 WAL，再进入内存表。
- 支持 batch 追加（单次 flush+fsync），减少高频小写入的系统调用成本。
- 记录格式（序列化后长度前缀）：
  - `u32 len + bytes(payload)`
  - payload 为一条完整 Row。
- 每次写入执行 `flush + sync_data`，保证进程崩溃后可恢复。

### 2.2 MemTable（可变内存表）
- 列式缓冲：每列独立向量。
- `subject/object/verb` 维护倒排索引（值 -> row_id 列表），加速等值过滤。
- 达到阈值（`memtable_flush_rows`）后触发 flush，转为 immutable segment。

### 2.3 Segment（LSM 不可变段）
- 文件：`<db_dir>/segments/segment_<id>.bin`
- 按列存储：数值列、字符串列、blob 列均按列序列化。
- 加载后构建内存索引：
  - `subject/object/verb` 倒排索引（等值）
  - `time_ms` 排序索引（范围）
  - `Morton(x,y,z)` 排序索引（范围候选裁剪）
- 当前阶段不做 compaction（可后续扩展）。
 - 当前实现提供手动 `compact()`，可将多个 segment 合并为 1 个。

## 3. 查询模型

### 3.1 查询条件
支持组合过滤：
- `subject/object/verb`：等值过滤
- `x/y/z/time_ms`：`eq/gt/gte/lt/lte` 范围过滤

### 3.2 执行流程
1. 对 `subject/object/verb` 先用倒排索引取 row_id 候选。
2. 对 `x/y/z` 使用 Morton 区间做候选裁剪，再按真实 `x/y/z` 精确过滤。
3. 对 `time_ms` 使用排序索引做区间过滤。
4. 对多条件做交集并回表（列转行）。
5. 合并 memtable 与所有 segments 的结果并返回。

### 3.3 读取性能策略
- 过滤先走索引，再回表。
- 列存减少无关列读取；只有命中 row_id 才访问 `subjectExtra`/`data`。
- 读路径无全局写锁，使用读写锁共享读。

## 4. 并发与一致性
- 全局版本号 `version: u64`。
- `insert_with_version(row, expected_version)`：
  - 若 `expected_version != current_version` 则返回冲突。
  - 成功写入后版本号 +1。
- `insert_batch_with_version(rows, expected_version)`：
  - 同样走乐观锁 compare。
  - 成功后版本号增加 `rows.len()`。
- 读操作不做快照隔离；读到的是调用时刻可见数据。

## 5. 崩溃恢复
- 启动时扫描 `segments/` 加载历史不可变段。
- 回放 `wal.log` 重建 memtable。
- WAL 记录包含递增 `seq`，恢复时只应用 `seq > max(segment.seq)` 的日志，避免重复回放。

## 6. 当前实现范围
- 包含：WAL（含 checkpoint/truncate）、MemTable、Segment 持久化、Morton+time 范围查询、乐观锁、batch 写入、手动 compaction、基础测试。
- 不包含：后台自动 compaction、删除/更新、复杂布尔谓词、TTL。

## 7. 后续扩展建议
1. 增加后台自动 segment compaction，进一步减少查询 fan-out。
2. 增加 compaction 策略（按层级/按大小）与节流控制。
3. 对字符串 key 增加字典编码与位图索引（Roaring Bitmap）。
4. 增加 batch 写入分组与 group commit 策略，降低 p99 写延迟。
