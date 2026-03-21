package com.github.zly2006.sl.mixin;

import com.github.zly2006.sl.logging.SpaceLoggerEventSink;
import net.minecraft.core.BlockPos;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.damagesource.DamageSource;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.LivingEntity;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

@Mixin(LivingEntity.class)
public abstract class LivingEntityMixin {
    @Unique
    private float sl$healthBeforeHurt;

    @Shadow
    public abstract float getHealth();

    @Shadow
    public abstract boolean isRemoved();

    @Shadow
    protected boolean dead;

    @Shadow
    public abstract java.util.UUID getUUID();

    @Shadow
    public abstract BlockPos blockPosition();

    @Shadow
    public abstract LivingEntity getKillCredit();

    @Inject(method = "actuallyHurt", at = @At("HEAD"))
    private void sl$captureHurtHealth(ServerLevel level, DamageSource source, float dmg, CallbackInfo ci) {
        this.sl$healthBeforeHurt = this.getHealth();
    }

    @Inject(method = "actuallyHurt", at = @At("RETURN"))
    private void sl$logHurt(ServerLevel level, DamageSource source, float dmg, CallbackInfo ci) {
        float dealt = Math.max(0.0F, this.sl$healthBeforeHurt - this.getHealth());
        if (dealt <= 0.0F) {
            return;
        }

        Entity attacker = source.getEntity();
        if (!(attacker instanceof ServerPlayer serverPlayer)) {
            return;
        }

        LivingEntity target = (LivingEntity) (Object) this;
        BlockPos pos = target.blockPosition();
        SpaceLoggerEventSink.log(
            pos.getX(),
            pos.getY(),
            pos.getZ(),
            SpaceLoggerEventSink.subject(serverPlayer),
            "hurt",
            SpaceLoggerEventSink.entityId(target),
            SpaceLoggerEventSink.subjectExtra(serverPlayer),
            SpaceLoggerEventSink.encodeHurtData(this.getUUID(), dealt)
        );
    }

    @Inject(method = "die", at = @At("HEAD"))
    private void sl$logKill(DamageSource source, CallbackInfo ci) {
        if (this.isRemoved() || this.dead) {
            return;
        }

        ServerPlayer killer = null;
        LivingEntity killCredit = this.getKillCredit();
        if (killCredit instanceof ServerPlayer creditPlayer) {
            killer = creditPlayer;
        } else if (source.getEntity() instanceof ServerPlayer sourcePlayer) {
            killer = sourcePlayer;
        }

        if (killer == null) {
            return;
        }

        LivingEntity target = (LivingEntity) (Object) this;
        BlockPos pos = target.blockPosition();
        SpaceLoggerEventSink.log(
            pos.getX(),
            pos.getY(),
            pos.getZ(),
            SpaceLoggerEventSink.subject(killer),
            "kill",
            SpaceLoggerEventSink.entityId(target),
            SpaceLoggerEventSink.subjectExtra(killer),
            SpaceLoggerEventSink.encodeEntityNbt(target)
        );
    }
}
