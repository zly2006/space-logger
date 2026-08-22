package me.zly2006.sl.mixinhelper;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import me.zly2006.sl.SpaceLogger;
import me.zly2006.sl.access.OperationCarrierAccess;
import me.zly2006.sl.jni.NativeSpaceLoggerBridge;
import net.minecraft.core.BlockPos;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.LivingEntity;
import net.minecraft.world.level.block.state.BlockState;

public final class RecordMixinHelper {
    private static final ThreadLocal<ArrayDeque<OperationStackEntry>> OPERATION_STACK =
        ThreadLocal.withInitial(ArrayDeque::new);
    private static final ThreadLocal<ArrayList<PendingEntityPlacement>> PENDING_ENTITY_PLACEMENTS =
        ThreadLocal.withInitial(ArrayList::new);

    private RecordMixinHelper() {
    }

    public static void playerStartRecording(ServerPlayer player, OperationCause cause) {
        if (recording() != null) {
            return;
        }
        OperationContext context = new OperationContext(
            NativeSpaceLoggerBridge.dimension(player.level()),
            NativeSpaceLoggerBridge.subject(player),
            NativeSpaceLoggerBridge.subjectExtra(player),
            cause
        );
        pushContext(context, "player recording/" + player.getScoreboardName() + "/" + cause);
    }

    public static void clearPendingEntityPlacements() {
        ArrayList<PendingEntityPlacement> placements = PENDING_ENTITY_PLACEMENTS.get();
        placements.clear();
        PENDING_ENTITY_PLACEMENTS.remove();
    }

    public static void recordSpawnedEntity(Entity entity) {
        if (entity == null || entity instanceof ServerPlayer) {
            return;
        }

        OperationContext context = recording();
        if (context == null) {
            return;
        }
        if (context.cause() != OperationCause.USE_BLOCK && context.cause() != OperationCause.USE_ITEM) {
            return;
        }

        PENDING_ENTITY_PLACEMENTS.get().add(
            new PendingEntityPlacement(
                NativeSpaceLoggerBridge.dimension(entity.level()),
                entity.blockPosition(),
                NativeSpaceLoggerBridge.entityId(entity)
            )
        );
    }

    public static List<PendingEntityPlacement> consumePendingEntityPlacements() {
        ArrayList<PendingEntityPlacement> placements = PENDING_ENTITY_PLACEMENTS.get();
        if (placements.isEmpty()) {
            return List.of();
        }
        List<PendingEntityPlacement> copy = List.copyOf(placements);
        placements.clear();
        PENDING_ENTITY_PLACEMENTS.remove();
        return copy;
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
            context.dimension(),
            context.subject(),
            NativeSpaceLoggerBridge.VERB_BREAK,
            NativeSpaceLoggerBridge.blockId(previousState),
            context.subjectExtra(),
            new byte[0]
        );
    }

    public static void logEntityKillIfNeeded(Entity entity) {
        if (entity == null || entity instanceof LivingEntity || entity instanceof ServerPlayer) {
            return;
        }

        OperationContext context = recording();
        if (context == null) {
            return;
        }

        BlockPos pos = entity.blockPosition();
        SpaceLogger.bridge().appendNow(
            pos.getX(),
            pos.getY(),
            pos.getZ(),
            context.dimension(),
            context.subject(),
            NativeSpaceLoggerBridge.VERB_KILL,
            NativeSpaceLoggerBridge.entityId(entity),
            context.subjectExtra(),
            NativeSpaceLoggerBridge.encodeEntityNbt(entity)
        );
    }

    public enum OperationCause {
        BREAK_BLOCK,
        ATTACK_ENTITY,
        COMMAND,
        USE_BLOCK,
        USE_ITEM,
        UNKNOWN
    }

    private static void pushContext(OperationContext context, String reason) {
        OPERATION_STACK.get().push(new OperationStackEntry(context, reason));
    }

    public record OperationContext(String dimension, String subject, String subjectExtra, OperationCause cause) {
    }

    public record PendingEntityPlacement(String dimension, BlockPos pos, String object) {
    }

    private record OperationStackEntry(OperationContext context, String reason) {
    }
}
