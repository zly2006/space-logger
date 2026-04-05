package com.github.zly2006.sl.mixinhelper;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.github.zly2006.sl.SpaceLogger;
import com.github.zly2006.sl.jni.NativeSpaceLoggerBridge;
import net.minecraft.core.BlockPos;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.block.state.BlockState;

public final class RecordMixinHelper {
    private static final int MAX_OPERATION_CONTEXTS = 8192;

    private static long nextOperationId = 1L;
    private static final Map<Long, OperationContext> OPERATION_CONTEXTS = Collections.synchronizedMap(
        new LinkedHashMap<>(256, 0.75F, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, OperationContext> eldest) {
                return size() > MAX_OPERATION_CONTEXTS;
            }
        }
    );
    private static final ThreadLocal<ArrayDeque<OperationStackEntry>> OPERATION_STACK =
        ThreadLocal.withInitial(ArrayDeque::new);

    private RecordMixinHelper() {
    }

    public static void playerStartRecording(ServerPlayer player, OperationCause cause) {
        if (recording() != null) {
            return;
        }
        long operationId = nextOperationId++;
        OperationContext context = new OperationContext(
            operationId,
            NativeSpaceLoggerBridge.subject(player),
            NativeSpaceLoggerBridge.subjectExtra(player),
            cause
        );
        OPERATION_CONTEXTS.put(operationId, context);
        pushRecord(operationId, "player recording/" + player.getScoreboardName() + "/" + cause);
    }

    public static void playerStopRecording(ServerPlayer player) {
        if (recording() == null) {
            return;
        }
        popRecord("player recording/" + player.getScoreboardName() + "/" + recording().cause());
    }

    public static void pushRecord(long operationId, String reason) {
        OPERATION_STACK.get().push(new OperationStackEntry(operationId, OPERATION_CONTEXTS.get(operationId), reason));
    }

    public static void popRecord(String reason) {
        ArrayDeque<OperationStackEntry> stack = OPERATION_STACK.get();
        if (stack.isEmpty()) {
            throw new IllegalStateException("Cannot pop empty operation stack: " + reason);
        }
        OperationStackEntry entry = stack.peek();
        if (!entry.reason().equals(reason)) {
            throw new IllegalStateException("Cannot pop record with different reason: " + reason + " != " + entry.reason());
        }
        stack.pop();
        if (stack.isEmpty()) {
            OPERATION_STACK.remove();
        }
    }

    public static OperationContext recording() {
        OperationStackEntry entry = OPERATION_STACK.get().peek();
        return entry == null ? null : entry.context();
    }

    public static void captureCurrentOperation(Object carrier) {
        if (!(carrier instanceof com.github.zly2006.sl.access.OperationCarrierAccess access)) {
            return;
        }
        OperationContext context = recording();
        if (context != null) {
            access.sl$setOperationId(context.id());
        }
    }

    public static void monitorSetBlock(BlockPos pos, BlockState previousState, BlockState newState) {
        OperationContext context = recording();
        if (context == null || previousState == null || previousState.isAir()) {
            return;
        }
        if (!newState.isAir()) {
            return;
        }

        SpaceLogger.bridge().appendNow(
            pos.getX(),
            pos.getY(),
            pos.getZ(),
            context.subject(),
            NativeSpaceLoggerBridge.VERB_BREAK,
            NativeSpaceLoggerBridge.blockId(previousState),
            context.subjectExtra(),
            new byte[0]
        );
    }

    public enum OperationCause {
        BREAK_BLOCK,
        USE_BLOCK,
        USE_ITEM,
        UNKNOWN
    }

    public record OperationContext(long id, String subject, String subjectExtra, OperationCause cause) {
    }

    private record OperationStackEntry(long id, OperationContext context, String reason) {
    }
}
