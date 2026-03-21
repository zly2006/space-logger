package com.github.zly2006.sl.logging;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.UUID;

import net.fabricmc.loader.api.FabricLoader;
import net.minecraft.core.BlockPos;
import net.minecraft.core.registries.BuiltInRegistries;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import net.minecraft.resources.Identifier;
import net.minecraft.util.ProblemReporter;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.player.Player;
import net.minecraft.world.item.ItemStack;
import net.minecraft.world.level.block.state.BlockState;
import net.minecraft.world.level.storage.TagValueOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SpaceLoggerEventSink {
    private static final Logger LOGGER = LoggerFactory.getLogger("space-logger-mod/EventSink");
    private static final Object LOCK = new Object();
    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
    private static Path logPath;

    private SpaceLoggerEventSink() {
    }

    public static void init(Path gameDir) {
        synchronized (LOCK) {
            Path logsDir = gameDir.resolve("logs");
            try {
                Files.createDirectories(logsDir);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to create logs dir: " + logsDir, e);
            }
            logPath = logsDir.resolve("space-logger-events.jsonl");
        }
    }

    public static String normalizeIdentifier(Identifier identifier) {
        if (identifier == null) {
            return "unknown";
        }
        String value = identifier.toString();
        if (value.startsWith("minecraft:")) {
            return value.substring("minecraft:".length());
        }
        return value;
    }

    public static String entityId(Entity entity) {
        return normalizeIdentifier(BuiltInRegistries.ENTITY_TYPE.getKey(entity.getType()));
    }

    public static String blockId(BlockState state) {
        return normalizeIdentifier(BuiltInRegistries.BLOCK.getKey(state.getBlock()));
    }

    public static String itemId(ItemStack stack) {
        return normalizeIdentifier(BuiltInRegistries.ITEM.getKey(stack.getItem()));
    }

    public static byte[] encodeHurtData(UUID targetUuid, float damage) {
        ByteBuffer buffer = ByteBuffer.allocate(16 + 4).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(targetUuid.getMostSignificantBits());
        buffer.putLong(targetUuid.getLeastSignificantBits());
        buffer.putFloat(damage);
        return buffer.array();
    }

    public static byte[] encodeEntityNbt(Entity entity) {
        try {
            TagValueOutput output = TagValueOutput.createWithContext(ProblemReporter.DISCARDING, entity.registryAccess());
            entity.saveWithoutId(output);
            CompoundTag tag = output.buildResult();

            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
                NbtIo.write(tag, dos);
                dos.flush();
                return baos.toByteArray();
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to serialize entity nbt for {}", entity, e);
            return new byte[0];
        }
    }

    public static void log(
        int x,
        int y,
        int z,
        String subject,
        String verb,
        String object,
        String subjectExtra,
        byte[] data
    ) {
        long timeMs = System.currentTimeMillis();
        log(timeMs, x, y, z, subject, verb, object, subjectExtra, data);
    }

    public static void log(
        long timeMs,
        int x,
        int y,
        int z,
        String subject,
        String verb,
        String object,
        String subjectExtra,
        byte[] data
    ) {
        Path path = ensureLogPath();
        String payload = jsonLine(timeMs, x, y, z, safe(subject), safe(verb), safe(object), safe(subjectExtra), data == null ? new byte[0] : data);
        try {
            synchronized (LOCK) {
                Files.writeString(
                    path,
                    payload + System.lineSeparator(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND
                );
            }
        } catch (IOException e) {
            LOGGER.error("Failed to append space logger event to {}", path, e);
        }
    }

    public static String subject(Player player) {
        return player.getScoreboardName();
    }

    public static String subjectExtra(Player player) {
        return player.getUUID().toString();
    }

    public static BlockPos safeBlockPos(BlockPos pos) {
        return pos == null ? BlockPos.ZERO : pos;
    }

    private static Path ensureLogPath() {
        synchronized (LOCK) {
            if (logPath == null) {
                init(FabricLoader.getInstance().getGameDir());
            }
            return logPath;
        }
    }

    private static String safe(String value) {
        return value == null ? "" : value;
    }

    private static String jsonLine(
        long timeMs,
        int x,
        int y,
        int z,
        String subject,
        String verb,
        String object,
        String subjectExtra,
        byte[] data
    ) {
        StringBuilder sb = new StringBuilder(256);
        sb.append('{');
        sb.append("\"x\":").append(x).append(',');
        sb.append("\"y\":").append(y).append(',');
        sb.append("\"z\":").append(z).append(',');
        sb.append("\"subject\":\"").append(escapeJson(subject)).append("\",");
        sb.append("\"object\":\"").append(escapeJson(object)).append("\",");
        sb.append("\"verb\":\"").append(escapeJson(verb)).append("\",");
        sb.append("\"time_ms\":").append(timeMs).append(',');
        sb.append("\"subjectExtra\":\"").append(escapeJson(subjectExtra)).append("\",");
        sb.append("\"dataBase64\":\"").append(BASE64_ENCODER.encodeToString(data)).append('"');
        sb.append('}');
        return sb.toString();
    }

    private static String escapeJson(String value) {
        StringBuilder escaped = new StringBuilder(value.length() + 8);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (c < 0x20) {
                        escaped.append("\\u");
                        String hex = Integer.toHexString(c);
                        for (int pad = hex.length(); pad < 4; pad++) {
                            escaped.append('0');
                        }
                        escaped.append(hex);
                    } else {
                        escaped.append(c);
                    }
                }
            }
        }
        return escaped.toString();
    }
}
