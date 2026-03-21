package com.github.zly2006.sl.jni;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

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

public final class NativeSpaceLoggerBridge {
    private static final Logger LOGGER = LoggerFactory.getLogger("space-logger-mod/NativeBridge");
    private static final Object LOCK = new Object();
    private static final int DEFAULT_FLUSH_ROWS = 4096;

    private static volatile boolean loaded;
    private static volatile boolean initialized;
    private static Path dbDir;

    private NativeSpaceLoggerBridge() {
    }

    public static void init(Path gameDir) {
        synchronized (LOCK) {
            if (!loaded) {
                loadNativeLibrary(gameDir);
                loaded = true;
            }
            if (!initialized) {
                dbDir = gameDir.resolve("space-logger-db");
                nativeInit(dbDir.toString(), DEFAULT_FLUSH_ROWS);
                initialized = true;
            }
        }
    }

    public static void append(
        int x,
        int y,
        int z,
        String subject,
        String verb,
        String object,
        long timeMs,
        String subjectExtra,
        byte[] data
    ) {
        ensureInitialized();
        boolean ok = nativeAppend(
            x,
            y,
            z,
            safe(subject),
            safe(verb),
            safe(object),
            timeMs,
            safe(subjectExtra),
            data == null ? new byte[0] : data
        );
        if (!ok) {
            throw new IllegalStateException("native append returned false");
        }
    }

    public static void appendNow(
        int x,
        int y,
        int z,
        String subject,
        String verb,
        String object,
        String subjectExtra,
        byte[] data
    ) {
        append(x, y, z, subject, verb, object, System.currentTimeMillis(), subjectExtra, data);
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

    public static String subject(Player player) {
        return player.getScoreboardName();
    }

    public static String subjectExtra(Player player) {
        return player.getUUID().toString();
    }

    public static BlockPos safeBlockPos(BlockPos pos) {
        return pos == null ? BlockPos.ZERO : pos;
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

    public static int countAll() {
        ensureInitialized();
        return nativeCountAll();
    }

    public static int countByVerb(String verb) {
        ensureInitialized();
        return nativeCountByVerb(verb);
    }

    public static List<QueryRow> queryRows(
        String subject,
        String object,
        String verb,
        int minX,
        int maxX,
        int minY,
        int maxY,
        int minZ,
        int maxZ,
        long afterTimeMs,
        long beforeTimeMs,
        int limit
    ) {
        ensureInitialized();

        int safeLimit = limit <= 0 ? 20 : limit;
        String[] encodedRows = nativeQuery(
            safe(subject),
            safe(object),
            safe(verb),
            minX,
            maxX,
            minY,
            maxY,
            minZ,
            maxZ,
            afterTimeMs,
            beforeTimeMs,
            safeLimit
        );
        if (encodedRows == null || encodedRows.length == 0) {
            return Collections.emptyList();
        }

        List<QueryRow> rows = new ArrayList<>(encodedRows.length);
        for (String encoded : encodedRows) {
            if (encoded == null || encoded.isBlank()) {
                continue;
            }
            String[] parts = encoded.split("\t", 9);
            if (parts.length != 9) {
                LOGGER.warn("Skipping malformed query row from JNI: {}", encoded);
                continue;
            }

            try {
                rows.add(new QueryRow(
                    Long.parseLong(parts[0]),
                    Integer.parseInt(parts[1]),
                    Integer.parseInt(parts[2]),
                    Integer.parseInt(parts[3]),
                    parts[4],
                    parts[5],
                    parts[6],
                    parts[7],
                    Integer.parseInt(parts[8])
                ));
            } catch (NumberFormatException e) {
                LOGGER.warn("Skipping malformed numeric fields in query row: {}", encoded, e);
            }
        }
        return rows;
    }

    public static void resetForTests() {
        synchronized (LOCK) {
            ensureInitialized();
            nativeReset(dbDir.toString(), DEFAULT_FLUSH_ROWS);
        }
    }

    private static void ensureInitialized() {
        if (!initialized) {
            throw new IllegalStateException("NativeSpaceLoggerBridge is not initialized");
        }
    }

    private static void loadNativeLibrary(Path gameDir) {
        String explicit = System.getProperty("space_logger_native_lib");
        if (explicit != null && !explicit.isBlank()) {
            Path explicitPath = Path.of(explicit).toAbsolutePath();
            System.load(explicitPath.toString());
            LOGGER.info("Loaded native space logger from explicit path: {}", explicitPath);
            return;
        }

        String libName = System.mapLibraryName("space_logger_native");
        List<Path> candidates = new ArrayList<>();
        Path workingDir = Path.of("").toAbsolutePath();
        Path gameParent = gameDir.getParent();

        candidates.add(workingDir.resolve("native-logger/target/release/" + libName));
        candidates.add(workingDir.resolve("native-logger/target/debug/" + libName));
        if (gameParent != null) {
            candidates.add(gameParent.resolve("native-logger/target/release/" + libName));
            candidates.add(gameParent.resolve("native-logger/target/debug/" + libName));
        }

        for (Path candidate : candidates) {
            if (candidate != null && Files.exists(candidate)) {
                System.load(candidate.toAbsolutePath().toString());
                LOGGER.info("Loaded native space logger from {}", candidate);
                return;
            }
        }

        StringBuilder msg = new StringBuilder("Cannot find native space logger library. Tried:\n");
        for (Path candidate : candidates) {
            msg.append(" - ").append(candidate).append('\n');
        }
        msg.append("You can set -Dspace_logger_native_lib=/absolute/path/to/").append(libName);
        throw new IllegalStateException(msg.toString());
    }

    private static String safe(String value) {
        return value == null ? "" : value;
    }

    private static native void nativeInit(String dbDir, int memtableFlushRows);

    private static native boolean nativeAppend(
        int x,
        int y,
        int z,
        String subject,
        String verb,
        String object,
        long timeMs,
        String subjectExtra,
        byte[] data
    );

    private static native int nativeCountAll();

    private static native int nativeCountByVerb(String verb);

    private static native String[] nativeQuery(
        String subject,
        String object,
        String verb,
        int minX,
        int maxX,
        int minY,
        int maxY,
        int minZ,
        int maxZ,
        long afterTimeMs,
        long beforeTimeMs,
        int limit
    );

    private static native void nativeReset(String dbDir, int memtableFlushRows);

    public record QueryRow(
        long timeMs,
        int x,
        int y,
        int z,
        String subject,
        String verb,
        String object,
        String subjectExtra,
        int dataLen
    ) {
    }
}
