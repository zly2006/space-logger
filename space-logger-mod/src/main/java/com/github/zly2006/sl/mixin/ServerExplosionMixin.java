package com.github.zly2006.sl.mixin;

import com.github.zly2006.sl.access.OperationCarrierAccess;
import com.github.zly2006.sl.mixinhelper.RecordMixinHelper;
import net.minecraft.world.level.ServerExplosion;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

@Mixin(ServerExplosion.class)
public abstract class ServerExplosionMixin implements OperationCarrierAccess {
    @Unique
    private long sl$operationId;

    @Inject(method = "<init>", at = @At("RETURN"))
    private void sl$captureOperationOnInit(CallbackInfo ci) {
        RecordMixinHelper.captureCurrentOperation(this);
    }

    @Inject(method = "explode", at = @At("HEAD"))
    private void sl$pushOperationBeforeExplode(CallbackInfoReturnable<Integer> cir) {
        RecordMixinHelper.pushRecord(this.sl$operationId, "explosion");
    }

    @Inject(method = "explode", at = @At("RETURN"))
    private void sl$popOperationAfterExplode(CallbackInfoReturnable<Integer> cir) {
        RecordMixinHelper.popRecord("explosion");
    }

    @Override
    public long sl$getOperationId() {
        return this.sl$operationId;
    }

    @Override
    public void sl$setOperationId(long operationId) {
        this.sl$operationId = operationId;
    }
}
