package com.github.zly2006.sl.mixin;

import com.github.zly2006.sl.logging.SpaceLoggerEventSink;
import net.minecraft.core.BlockPos;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.InteractionHand;
import net.minecraft.world.InteractionResult;
import net.minecraft.world.item.ItemStack;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.block.state.BlockState;
import net.minecraft.world.phys.BlockHitResult;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
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

    @Unique
    private BlockState sl$breakStateBefore;

    @Inject(method = "destroyBlock", at = @At("HEAD"))
    private void sl$captureBeforeBreak(BlockPos pos, CallbackInfoReturnable<Boolean> cir) {
        this.sl$breakStateBefore = this.level.getBlockState(pos);
    }

    @Inject(method = "destroyBlock", at = @At("RETURN"))
    private void sl$logBreak(BlockPos pos, CallbackInfoReturnable<Boolean> cir) {
        if (!cir.getReturnValue()) {
            return;
        }
        if (this.sl$breakStateBefore == null || this.sl$breakStateBefore.isAir()) {
            return;
        }

        SpaceLoggerEventSink.log(
            pos.getX(),
            pos.getY(),
            pos.getZ(),
            SpaceLoggerEventSink.subject(this.player),
            "break",
            SpaceLoggerEventSink.blockId(this.sl$breakStateBefore),
            SpaceLoggerEventSink.subjectExtra(this.player),
            new byte[0]
        );
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

        BlockPos pos = hitResult.getBlockPos();
        BlockState state = this.level.getBlockState(pos);
        if (state.isAir()) {
            return;
        }

        SpaceLoggerEventSink.log(
            pos.getX(),
            pos.getY(),
            pos.getZ(),
            SpaceLoggerEventSink.subject(player),
            "use",
            SpaceLoggerEventSink.blockId(state),
            SpaceLoggerEventSink.subjectExtra(player),
            new byte[0]
        );
    }
}
