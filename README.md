# space-logger

一个面向 OLAP 读多写少场景的三维特化数据库（Rust 实现）。

## 技术特征

- 列存（Columnar）
  - 每列独立存储，查询时优先按条件列过滤，再回表组装行。
- WAL（Write-Ahead Log）
  - 先写 WAL，再写内存表，崩溃恢复可重放。
  - `flush` 后执行 WAL checkpoint/truncate，控制日志体积。
- LSM-Tree 写路径
  - `MemTable + immutable Segment`。
  - 达到阈值自动 flush 到段文件。
- 范围查询优化
  - `x/y/z` 支持 `> >= < <= = between`。
  - 通过 `Morton Code(x,y,z)` 做候选裁剪，再精确过滤。
  - `time_ms` 走排序索引做范围过滤。
- 乐观锁写入
  - 低并发写场景下用版本号 compare-and-write。
- 删除与压缩
  - `delete` 支持按查询条件删除。
  - `compact` 可手动合并多个段，降低读 fan-out。

## 数据模型

字段如下：

- `x: int`
- `y: int`
- `z: int`
- `subject: string`
- `object: string`
- `verb: string`
- `time_ms: long`
- `subjectExtra: string`（CLI 中字段名 `subject_extra`）
- `data: blob`（CLI 中使用十六进制 `data_hex`）

## 快速开始

### 1. 编译

```bash
cargo build --release
```

### 2. 添加数据

语法：

```bash
space-logger add <x> <y> <z> <subject> <verb> <object> [time_ms]
```

说明：

- `time_ms` 省略时使用当前时间（毫秒）。
- 可选参数：
  - `--subject-extra <text>`
  - `--data-hex <hex>`
- 默认数据库目录是 `./space-logger-data`，可用 `--db` 指定。

示例：

```bash
cargo run -- --db ./data add 1 2 3 alice click doc1
cargo run -- --db ./data add 9 10 11 bob view doc2 1700000001234 --subject-extra note --data-hex 0a0b
```

### 3. 查询数据

语法（可多个条件，条件之间默认 AND）：

```bash
space-logger query [--limit N] <field> <op> <value...> [<field> <op> <value...> ...]
```

结果返回顺序：

- 默认按最新写入优先（从新到旧）。
- `--limit N` 表示最多返回 `N` 条。

支持字段：

- 数值字段：`x y z time_ms`
- 字符串字段：`subject object verb`

支持操作符：

- 数值：`= == > >= < <= between bet`
- 字符串：`= ==`

示例：

```bash
# 与你给的示例一致
cargo run -- --db ./data query x bet 0 100 y = 10

# 限制返回前2条（最新两条）
cargo run -- --db ./data query --limit 2 x between 0 100

# 组合查询
cargo run -- --db ./data query x between 0 100 y = 10 subject = alice verb = click

# 在 shell 里建议给 >= <= > < 加引号，避免被当成重定向
cargo run -- --db ./data query x '>=' 10 time_ms '<=' 1700000005000
```

### 4. 删除数据

语法与 query 一样：

```bash
space-logger delete <field> <op> <value...> [<field> <op> <value...> ...]
```

示例：

```bash
cargo run -- --db ./data delete subject = alice
cargo run -- --db ./data delete x between 0 100 time_ms >= 1700000000000
```

### 5. 手动压缩段文件

```bash
cargo run -- --db ./data compact
```

## 命令行参数

全局参数：

- `--db <path>`：数据库目录（默认 `./space-logger-data`）
- `--memtable-flush-rows <n>`：内存表 flush 阈值（默认 `4096`）

## 开发与验证

### 测试

```bash
cargo test
```

测试覆盖：

- WAL 恢复
- flush + WAL 截断
- LSM flush/segment 查询
- 乐观锁（单条/批量）
- 范围过滤逻辑
- compaction 正确性
- delete 正确性
- 700 行数据对照测试（数据库过滤结果 vs 参考实现）

### 基准测试

```bash
cargo bench --bench olap_bench -- --noplot
```

## 注意事项

- 当前不提供事务隔离与多写并发优化。
- 删除操作当前采用重写段文件策略，适合“读多写少”的 OLAP 场景。
- 如果你要在高写入场景使用，建议优先实现后台自动 compaction 与 group commit。
