# space-logger-mod 下一阶段计划（未完成项）

本文件记录当前“先可见效果”的实现边界，以及后续补齐项。

## 本轮已完成（可直接看到效果）

- 已实现事件落盘器：`space-logger-mod/logs/space-logger-events.jsonl`
- 已接入并记录以下动词：
  - `hurt`
  - `kill`
  - `break`
  - `place`
  - `use`
- 字段已按统一结构输出：
  - `x,y,z,subject,object,verb,time_ms,subjectExtra,dataBase64`
- `object` 已做 `minecraft:` 前缀去除。

## 未完成项与原因

### 1) `add_item` / `remove_item`

- 需要在 `AbstractContainerMenu.clicked` 做“前后快照差分”。
- 难点：
  - 不同 `ContainerInput` / click path（拖拽、shift、swap）语义不同；
  - 需要区分玩家背包槽位 vs 容器槽位；
  - 需要稳定提取“向容器增加/减少”的净变化量。
- 计划注入点：
  - `@Inject(method="clicked", at=@At("HEAD"))` 记录容器槽快照；
  - `@Inject(method="clicked", at=@At("RETURN"))` 对比快照生成 add/remove 事件。

### 2) JNI 直连 Rust 数据库

- 当前为了先出效果，先用 JSONL 落盘。
- 后续替换为 JNI：
  - Java -> `native` 方法（`log(...)`/`query(...)`）；
  - Rust `cdylib` 导出 JNI 符号；
  - Fabric 启动时动态加载 `libspace_logger_native.*`。
- 待补内容：
  - `space-logger-mod/native-logger` crate；
  - Gradle/Cargo 联动构建脚本；
  - 错误码与异常映射。

### 3) Fabric GameTest 自动验证

- 目标：用 testmod 触发全部事件并做可查询断言。
- 待补内容：
  - testmod entrypoint（`fabric-gametest`）；
  - 构造测试玩家与可控场景（实体、方块、容器）；
  - 对每个动词至少一个成功 + 一个失败断言。

## 建议的下一步顺序

1. 先完成 `add_item/remove_item` 差分逻辑（纯 Java，最快可闭环）。  
2. 再切 JNI（替换 JSONL sink，不改 mixin 行为层）。  
3. 最后接入 GameTest 进行自动化回归。  
