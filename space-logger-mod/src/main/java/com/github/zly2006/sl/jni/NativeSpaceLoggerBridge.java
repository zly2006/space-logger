package com.github.zly2006.sl.jni;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import net.minecraft.core.RegistryAccess;
import net.minecraft.core.BlockPos;
import net.minecraft.core.registries.BuiltInRegistries;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import net.minecraft.nbt.NbtOps;
import net.minecraft.nbt.Tag;
import net.minecraft.resources.Identifier;
import net.minecraft.resources.RegistryOps;
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
    private static final byte[] INVENTORY_DATA_MAGIC = new byte[] {'S', 'L', 'I', '1'};
    private static final int INVENTORY_DATA_HEADER_BYTES = 12;
    private static final int QUERY_DATA_HEAD_BYTES = 12;
    private static final Set<UUID> RECENT_PLACE_PLAYERS = ConcurrentHashMap.newKeySet();

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

    public static byte[] encodeItemNbt(ItemStack stack, RegistryAccess registryAccess) {
        if (stack == null || stack.isEmpty()) {
            return new byte[0];
        }

        try {
            RegistryOps<Tag> ops = RegistryOps.create(NbtOps.INSTANCE, registryAccess);
            Tag encoded = ItemStack.CODEC.encodeStart(ops, stack).getOrThrow(IllegalStateException::new);
            if (!(encoded instanceof CompoundTag tag)) {
                return new byte[0];
            }

            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 DataOutputStream dos = new DataOutputStream(baos)) {
                NbtIo.write(tag, dos);
                dos.flush();
                return baos.toByteArray();
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to serialize item nbt for {}", stack, e);
            return new byte[0];
        }
    }

    public static byte[] encodeInventoryDeltaData(ItemStack stack, int quantityDelta, RegistryAccess registryAccess) {
        if (stack == null || stack.isEmpty() || quantityDelta == 0) {
            return new byte[0];
        }

        ItemStack template = stack.copyWithCount(1);
        byte[] itemNbt = encodeItemNbt(template, registryAccess);
        ByteBuffer buffer = ByteBuffer
            .allocate(INVENTORY_DATA_HEADER_BYTES + itemNbt.length)
            .order(ByteOrder.LITTLE_ENDIAN);

        // Layout:
        // [0..3]   magic "SLI1"
        // [4..7]   signed quantity delta (add: positive, remove: negative)
        // [8..11]  item nbt length in bytes
        // [12..]   item nbt payload
        buffer.put(INVENTORY_DATA_MAGIC);
        buffer.putInt(quantityDelta);
        buffer.putInt(itemNbt.length);
        buffer.put(itemNbt);
        return buffer.array();
    }

    public static void markRecentPlace(Player player) {
        if (player == null) {
            return;
        }
        RECENT_PLACE_PLAYERS.add(player.getUUID());
    }

    public static boolean consumeRecentPlace(Player player) {
        if (player == null) {
            return false;
        }
        return RECENT_PLACE_PLAYERS.remove(player.getUUID());
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
        QueryRow[] rows = nativeQuery(
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
        if (rows == null || rows.length == 0) {
            return Collections.emptyList();
        }
        return Arrays.asList(rows);
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

    private static native QueryRow[] nativeQuery(
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
        int dataLen,
        byte[] dataHead
    ) {
        public QueryRow {
            dataHead = dataHead == null ? new byte[0] : dataHead;
        }

        @Override
        public byte[] dataHead() {
            return dataHead.clone();
        }

        public boolean hasInventoryDataHeader() {
            return dataHead.length >= QUERY_DATA_HEAD_BYTES
                && dataHead[0] == INVENTORY_DATA_MAGIC[0]
                && dataHead[1] == INVENTORY_DATA_MAGIC[1]
                && dataHead[2] == INVENTORY_DATA_MAGIC[2]
                && dataHead[3] == INVENTORY_DATA_MAGIC[3];
        }

        public int quantityDelta() {
            if (!hasInventoryDataHeader()) {
                return 0;
            }
            return ByteBuffer.wrap(dataHead, 4, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        }

        public int nbtPayloadLen() {
            if (!hasInventoryDataHeader()) {
                return 0;
            }
            return ByteBuffer.wrap(dataHead, 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        }
    }
}
