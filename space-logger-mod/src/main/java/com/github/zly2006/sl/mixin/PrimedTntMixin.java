package com.github.zly2006.sl.mixin;

import com.github.zly2006.sl.access.OperationCarrierAccess;
import com.github.zly2006.sl.mixinhelper.RecordMixinHelper;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.EntityType;
import net.minecraft.world.entity.item.PrimedTnt;
import net.minecraft.world.level.Level;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

@Mixin(PrimedTnt.class)
public abstract class PrimedTntMixin extends Entity implements OperationCarrierAccess {
    @Unique
    private long sl$operationId;

    protected PrimedTntMixin(EntityType<?> entityType, Level level) {
        super(entityType, level);
    }

    @Inject(method = "<init>(Lnet/minecraft/world/entity/EntityType;Lnet/minecraft/world/level/Level;)V", at = @At("RETURN"))
    private void sl$captureOperationId(EntityType<?> entityType, Level level, CallbackInfo ci) {
        if (!level.isClientSide()) {
            RecordMixinHelper.captureCurrentOperation(this);
        }
    }

    @Inject(method = "explode", at = @At("HEAD"))
    private void sl$pushOperationBeforeExplode(CallbackInfo ci) {
        RecordMixinHelper.pushRecord(this.sl$operationId, "tnt explode/" + this.getId());
    }

    @Inject(method = "explode", at = @At("TAIL"))
    private void sl$popOperationAfterExplode(CallbackInfo ci) {
        RecordMixinHelper.popRecord("tnt explode/" + this.getId());
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
