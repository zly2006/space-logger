# Agent Execution Rules

- Run Gradle tasks in this repo via `./agent-gradlew` from repository root.
- Do not call `gradle` or `./gradlew` directly for routine build/test runs.
- `./agent-gradlew` pins Java 25 and injects Fabric GameTest JVM flags:
  - `-Dfabric-api.gametest=true`
  - `-Dfabric-api.gametest.report-file=/tmp/space-logger-gametest.xml` (override with `SPACE_LOGGER_GAMETEST_REPORT`)
- `./agent-gradlew` also sets `-Dorg.gradle.java.installations.paths` to the Java 25 home.
