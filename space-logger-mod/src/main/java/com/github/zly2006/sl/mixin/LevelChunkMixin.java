package com.github.zly2006.sl.mixin;

import com.github.zly2006.sl.mixinhelper.RecordMixinHelper;
import net.minecraft.core.BlockPos;
import net.minecraft.world.level.block.state.BlockState;
import net.minecraft.world.level.chunk.LevelChunk;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

@Mixin(LevelChunk.class)
public abstract class LevelChunkMixin {
    // LevelChunk#setBlockState is the stable sink where direct breaks and async side effects both become block changes.
    @Inject(method = "setBlockState", at = @At("RETURN"))
    private void sl$monitorSetBlock(BlockPos pos, BlockState state, int flags, CallbackInfoReturnable<BlockState> cir) {
        BlockState previousState = cir.getReturnValue();
        if (previousState == null) {
            return;
        }
        RecordMixinHelper.monitorSetBlock(pos, previousState, state);
    }
}
