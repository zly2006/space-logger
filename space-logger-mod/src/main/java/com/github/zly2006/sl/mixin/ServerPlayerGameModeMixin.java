package com.github.zly2006.sl.mixin;

import com.github.zly2006.sl.SpaceLogger;
import com.github.zly2006.sl.jni.NativeSpaceLoggerBridge;
import com.github.zly2006.sl.mixinhelper.RecordMixinHelper;
import net.minecraft.core.BlockPos;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.InteractionHand;
import net.minecraft.world.InteractionResult;
import net.minecraft.world.item.BlockItem;
import net.minecraft.world.item.ItemStack;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.block.state.BlockState;
import net.minecraft.world.phys.BlockHitResult;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

@Mixin(net.minecraft.server.level.ServerPlayerGameMode.class)
public abstract class ServerPlayerGameModeMixin {
    @Shadow
    protected ServerLevel level;

    @Final
    @Shadow
    protected ServerPlayer player;

    @Inject(
        method = "destroyBlock",
        at = @At(
            value = "INVOKE",
            target = "Lnet/minecraft/world/level/block/Block;playerWillDestroy(Lnet/minecraft/world/level/Level;Lnet/minecraft/core/BlockPos;Lnet/minecraft/world/level/block/state/BlockState;Lnet/minecraft/world/entity/player/Player;)Lnet/minecraft/world/level/block/state/BlockState;"
        )
    )
    private void sl$startRecordingBlockBreak(BlockPos pos, CallbackInfoReturnable<Boolean> cir) {
        RecordMixinHelper.playerStartRecording(this.player, RecordMixinHelper.OperationCause.BREAK_BLOCK);
    }

    @Inject(
        method = "destroyBlock",
        at = @At(
            value = "INVOKE",
            target = "Lnet/minecraft/server/level/ServerPlayer;preventsBlockDrops()Z"
        )
    )
    private void sl$stopRecordingBlockBreak(BlockPos pos, CallbackInfoReturnable<Boolean> cir) {
        RecordMixinHelper.playerStopRecording(this.player);
    }

    @Inject(method = "useItemOn", at = @At("HEAD"))
    private void sl$startRecordingUseBlock(
        ServerPlayer player,
        Level level,
        ItemStack stack,
        InteractionHand hand,
        BlockHitResult hitResult,
        CallbackInfoReturnable<InteractionResult> cir
    ) {
        RecordMixinHelper.playerStartRecording(this.player, RecordMixinHelper.OperationCause.USE_BLOCK);
    }

    @Inject(method = "useItemOn", at = @At("RETURN"))
    private void sl$logUse(
        ServerPlayer player,
        Level level,
        ItemStack stack,
        InteractionHand hand,
        BlockHitResult hitResult,
        CallbackInfoReturnable<InteractionResult> cir
    ) {
        InteractionResult result = cir.getReturnValue();
        if (!result.consumesAction()) {
            return;
        }
        if (NativeSpaceLoggerBridge.consumeRecentPlace(player)) {
            return;
        }
        if (stack.getItem() instanceof BlockItem) {
            return;
        }

        BlockPos pos = hitResult.getBlockPos();
        BlockState state = this.level.getBlockState(pos);
        if (state.isAir()) {
            return;
        }

        SpaceLogger.bridge().appendNow(
            pos.getX(),
            pos.getY(),
            pos.getZ(),
            NativeSpaceLoggerBridge.subject(player),
            NativeSpaceLoggerBridge.VERB_USE,
            NativeSpaceLoggerBridge.blockId(state),
            NativeSpaceLoggerBridge.subjectExtra(player),
            new byte[0]
        );
    }

    @Inject(method = "useItemOn", at = @At("RETURN"))
    private void sl$stopRecordingUseBlock(
        ServerPlayer player,
        Level level,
        ItemStack stack,
        InteractionHand hand,
        BlockHitResult hitResult,
        CallbackInfoReturnable<InteractionResult> cir
    ) {
        RecordMixinHelper.playerStopRecording(this.player);
    }
}
