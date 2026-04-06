package me.zly2006.sl.mixin;

import me.zly2006.sl.mixinhelper.RecordMixinHelper;
import net.minecraft.world.entity.Entity;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

@Mixin(Entity.class)
public abstract class EntityMixin {
    @Inject(method = "remove", at = @At("HEAD"))
    private void sl$logNonLivingKill(Entity.RemovalReason reason, CallbackInfo ci) {
        Entity self = (Entity) (Object) this;
        if (reason != Entity.RemovalReason.KILLED || self.isRemoved()) {
            return;
        }
        RecordMixinHelper.logEntityKillIfNeeded(self);
    }
}
