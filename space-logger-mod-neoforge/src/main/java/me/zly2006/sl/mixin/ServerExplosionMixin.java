package me.zly2006.sl.mixin;

import me.zly2006.sl.access.OperationCarrierAccess;
import me.zly2006.sl.mixinhelper.RecordMixinHelper;
import net.minecraft.world.level.Explosion;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

@Mixin(Explosion.class)
public abstract class ServerExplosionMixin implements OperationCarrierAccess {
    @Unique
    private RecordMixinHelper.OperationContext sl$operationContext;

    @Inject(method = "<init>", at = @At("RETURN"))
    private void sl$captureOperationOnInit(CallbackInfo ci) {
        RecordMixinHelper.captureCurrentOperation(this);
    }

    @Inject(method = "explode", at = @At("HEAD"))
    private void sl$pushOperationBeforeExplode(CallbackInfo ci) {
        RecordMixinHelper.pushFromCarrier(this, "explosion");
    }

    @Inject(method = "finalizeExplosion", at = @At("RETURN"))
    private void sl$popOperationAfterExplode(boolean spawnParticles, CallbackInfo ci) {
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
