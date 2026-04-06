# Agent Execution Rules

- Run Gradle tasks in this repo via `./agent-gradlew` from repository root.
- Do not call `gradle` or `./gradlew` directly for routine build/test runs.
- `./agent-gradlew` pins Java 25 and injects Fabric GameTest JVM flags:
  - `-Dfabric-api.gametest=true`
  - `-Dfabric-api.gametest.report-file=/tmp/space-logger-gametest.xml` (override with `SPACE_LOGGER_GAMETEST_REPORT`)
- `./agent-gradlew` also sets `-Dorg.gradle.java.installations.paths` to the Java 25 home.

# 开发前的检查：避免冲突

**必须检查**：如果25565端口存在占用，说明我正在运行一个Minecraft服务器实例。**不要**在这种情况下运行测试或构建任务，因为它们可能会干扰正在运行的服务器，按照以下步骤继续：

1. 使用 using-git-worktree 创建一个新的工作树（例如 `feat/xxx`）
2. 切换到新工作树，进行开发和测试。必须 test-driven-development，确保每次提交都通过测试，新开发的功能必须有完整的gametest覆盖。
3. 完成开发后，切回主工作树，使用rebase保证线性历史，合并 feat 分支，删除工作树。
