package com.github.zly2006.sl.mixinhelper;

import java.util.ArrayDeque;

import com.github.zly2006.sl.SpaceLogger;
import com.github.zly2006.sl.access.OperationCarrierAccess;
import com.github.zly2006.sl.jni.NativeSpaceLoggerBridge;
import net.minecraft.core.BlockPos;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.block.state.BlockState;

public final class RecordMixinHelper {
    private static final ThreadLocal<ArrayDeque<OperationStackEntry>> OPERATION_STACK =
        ThreadLocal.withInitial(ArrayDeque::new);

    private RecordMixinHelper() {
    }

    public static void playerStartRecording(ServerPlayer player, OperationCause cause) {
        if (recording() != null) {
            return;
        }
        OperationContext context = new OperationContext(
            NativeSpaceLoggerBridge.subject(player),
            NativeSpaceLoggerBridge.subjectExtra(player),
            cause
        );
        pushContext(context, "player recording/" + player.getScoreboardName() + "/" + cause);
    }

    public static void playerStopRecording(ServerPlayer player) {
        if (recording() == null) {
            return;
        }
        popRecord("player recording/" + player.getScoreboardName() + "/" + recording().cause());
    }

    public static void pushFromCarrier(Object carrier, String reason) {
        if (!(carrier instanceof OperationCarrierAccess access)) {
            return;
        }
        OperationContext context = access.sl$getOperationContext();
        if (context != null) {
            pushContext(context, reason);
        }
    }

    public static void popRecord(String reason) {
        ArrayDeque<OperationStackEntry> stack = OPERATION_STACK.get();
        if (stack.isEmpty()) {
            // old logic throw new IllegalStateException("Cannot pop empty operation stack: " + reason);
            return;
        }
        OperationStackEntry entry = stack.peek();
        if (!entry.reason().equals(reason)) {
            // old logic throw new IllegalStateException("Cannot pop record with different reason: " + reason + " != " + entry.reason());
            stack.clear();
            return;
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
        if (!(carrier instanceof OperationCarrierAccess access)) {
            return;
        }
        OperationContext context = recording();
        if (context != null) {
            access.sl$setOperationContext(context);
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
        COMMAND,
        USE_BLOCK,
        USE_ITEM,
        UNKNOWN
    }

    private static void pushContext(OperationContext context, String reason) {
        OPERATION_STACK.get().push(new OperationStackEntry(context, reason));
    }

    public record OperationContext(String subject, String subjectExtra, OperationCause cause) {
    }

    private record OperationStackEntry(OperationContext context, String reason) {
    }
}
