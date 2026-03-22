package com.github.zly2006.sl.mixin;

import com.github.zly2006.sl.SpaceLogger;
import com.github.zly2006.sl.jni.NativeSpaceLoggerBridge;
import net.minecraft.core.registries.BuiltInRegistries;
import net.minecraft.core.BlockPos;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.InteractionResult;
import net.minecraft.world.entity.player.Player;
import net.minecraft.world.item.BlockItem;
import net.minecraft.world.item.context.BlockPlaceContext;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

@Mixin(BlockItem.class)
public abstract class BlockItemMixin {
    @Inject(method = "place", at = @At("RETURN"))
    private void sl$logPlace(BlockPlaceContext context, CallbackInfoReturnable<InteractionResult> cir) {
        InteractionResult result = cir.getReturnValue();
        if (!result.consumesAction()) {
            return;
        }

        Player player = context.getPlayer();
        if (!(player instanceof ServerPlayer serverPlayer)) {
            return;
        }

        BlockPos pos = context.getClickedPos();
        BlockItem self = (BlockItem) (Object) this;
        SpaceLogger.bridge().appendNow(
            pos.getX(),
            pos.getY(),
            pos.getZ(),
            NativeSpaceLoggerBridge.subject(serverPlayer),
            "place",
            NativeSpaceLoggerBridge.normalizeIdentifier(BuiltInRegistries.BLOCK.getKey(self.getBlock())),
            NativeSpaceLoggerBridge.subjectExtra(serverPlayer),
            new byte[0]
        );
        NativeSpaceLoggerBridge.markRecentPlace(serverPlayer);
    }
}
