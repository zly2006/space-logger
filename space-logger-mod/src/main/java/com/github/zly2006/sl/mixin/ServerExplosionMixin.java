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
    private RecordMixinHelper.OperationContext sl$operationContext;

    @Inject(method = "<init>", at = @At("RETURN"))
    private void sl$captureOperationOnInit(CallbackInfo ci) {
        RecordMixinHelper.captureCurrentOperation(this);
    }

    @Inject(method = "explode", at = @At("HEAD"))
    private void sl$pushOperationBeforeExplode(CallbackInfoReturnable<Integer> cir) {
        RecordMixinHelper.pushFromCarrier(this, "explosion");
    }

    @Inject(method = "explode", at = @At("RETURN"))
    private void sl$popOperationAfterExplode(CallbackInfoReturnable<Integer> cir) {
        RecordMixinHelper.popRecord("explosion");
    }

    @Override
    public RecordMixinHelper.OperationContext sl$getOperationContext() {
        return this.sl$operationContext;
    }

    @Override
    public void sl$setOperationContext(RecordMixinHelper.OperationContext operationContext) {
        this.sl$operationContext = operationContext;
    }
}
