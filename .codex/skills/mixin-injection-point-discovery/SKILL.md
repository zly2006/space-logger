---
name: mixin-injection-point-discovery
description: Locate robust Mixin injection points from unobfuscated Minecraft jars by reading bytecode signatures with javap and validating method ownership, descriptors, and event semantics. Use when implementing or fixing Fabric mixins, especially in 26.1 versions where mappings may differ.
---

# Mixin Injection Point Discovery

Use this workflow to find safe, reproducible mixin targets before writing injection code.

## Inputs

- Target Minecraft common jar (for this project):
  - `/Users/zhaoliyan/IdeaProjects/space-logger/space-logger-mod/.gradle/loom-cache/minecraftMaven/net/minecraft/minecraft-common-9a7fd27717/26.1-rc-2/minecraft-common-9a7fd27717-26.1-rc-2.jar`
- Feature intent (what behavior to observe/record/change)

## Workflow

1. Confirm target classes exist in jar.

```bash
jar tf "$JAR" | rg 'ClassName1|ClassName2|...'
```

2. Decompile first for human-readable control flow (recommended).

```bash
java -jar ~/Downloads/vineflower-1.10.1-slim.jar "$JAR" ./tmp-mc-src
```

- Use the decompiled source to understand actual branches, return points, and where side effects happen.
- Treat decompilation output as guidance; method descriptors still need bytecode verification.

3. Read exact method signatures from bytecode.

```bash
javap -classpath "$JAR" -p net.minecraft.some.TargetClass | sed -n '1,300p'
```

4. Choose hook method by semantics, not by name similarity alone.
- Prefer methods that encode final server-side outcome.
- For action success/failure, inject at `@At("RETURN")` and inspect return value.
- For pre-state capture + post-state diff, use `HEAD` + `RETURN` pair.

5. Record exact descriptor in comments while coding.
- Example format:
  - `Lnet/minecraft/core/BlockPos;`
  - `Lnet/minecraft/world/InteractionResult;`

6. Verify ownership class.
- Ensure method belongs to the mixin target class (or inherited and still callable from target).
- Avoid injecting into broad base classes unless behavior is truly global.

7. Validate side and threading.
- For game logic persistence, only log on server side.
- Guard with `instanceof ServerPlayer` / `instanceof ServerLevel` where applicable.

8. If method is too broad, narrow context.
- Add runtime guards (container type, block entity presence, action type, non-empty stacks).
- Prefer extra guard code over fragile `ordinal`-based injection.

## Selection Heuristics (Minecraft 26.1 unobfuscated)

- Combat hit amount: `LivingEntity.actuallyHurt(ServerLevel, DamageSource, float)`
- Kill event: `LivingEntity.die(DamageSource)`
- Block break: `ServerPlayerGameMode.destroyBlock(BlockPos)`
- Block use/place flow: `ServerPlayerGameMode.useItemOn(...)` and/or `BlockItem.place(BlockPlaceContext)`
- Container click diffing: `AbstractContainerMenu.clicked(int, int, ContainerInput, Player)`

Always re-check signatures via `javap` before coding; do not assume from older MC versions.

## Command Snippets

```bash
# Optional: decompile full jar for readable logic
java -jar ~/Downloads/vineflower-1.10.1-slim.jar "$JAR" ./tmp-mc-src

# Show methods in target class
javap -classpath "$JAR" -p net.minecraft.server.level.ServerPlayerGameMode

# Find likely classes quickly
jar tf "$JAR" | rg 'ServerPlayerGameMode|LivingEntity|AbstractContainerMenu|BlockItem'

# Find save/nbt helpers
javap -classpath "$JAR" -p net.minecraft.nbt.NbtIo
```

## Failure Patterns to Avoid

- Targeting client-only classes for server persistence logic.
- Injecting at `HEAD` for outcomes that are only known at return time.
- Assuming mapped names from Yarn/Mojmap docs when current jar is unobfuscated snapshot/RC.
- Relying on local variable capture without bytecode confirmation.

## Done Criteria

- Every mixin target class and method signature confirmed from `javap` output.
- Injection point has explicit rationale (`why this method`, `why this at`).
- Server-side guards present where needed.
- Behavior validated with an automated or deterministic test path.
