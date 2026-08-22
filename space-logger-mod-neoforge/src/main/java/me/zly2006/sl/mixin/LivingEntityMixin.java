package me.zly2006.sl.mixin;

import me.zly2006.sl.SpaceLogger;
import me.zly2006.sl.jni.NativeSpaceLoggerBridge;
import me.zly2006.sl.mixinhelper.RecordMixinHelper;
import net.minecraft.core.BlockPos;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.damagesource.DamageSource;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.EntityType;
import net.minecraft.world.entity.LivingEntity;
import net.minecraft.world.level.Level;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

@Mixin(LivingEntity.class)
public abstract class LivingEntityMixin extends Entity  {
    @Unique
    private float sl$healthBeforeHurt;

    public LivingEntityMixin(EntityType<?> type, Level level) {
        super(type, level);
    }

    @Shadow
    public abstract float getHealth();

    @Shadow
    protected boolean dead;

    @Shadow
    public abstract LivingEntity getKillCredit();

    @Inject(method = "actuallyHurt", at = @At("HEAD"))
    private void sl$captureHurtHealth(DamageSource source, float dmg, CallbackInfo ci) {
        this.sl$healthBeforeHurt = this.getHealth();
    }

    @Inject(method = "actuallyHurt", at = @At("RETURN"))
    private void sl$logHurt(DamageSource source, float dmg, CallbackInfo ci) {
        float dealt = Math.max(0.0F, this.sl$healthBeforeHurt - this.getHealth());
        if (dealt <= 0.0F) {
            return;
        }

        Entity attacker = source.getEntity();
        String subject;
        String subjectExtra;
        if (attacker instanceof ServerPlayer serverPlayer) {
            subject = NativeSpaceLoggerBridge.subject(serverPlayer);
            subjectExtra = NativeSpaceLoggerBridge.subjectExtra(serverPlayer);
        } else {
            RecordMixinHelper.OperationContext context = RecordMixinHelper.recording();
            if (context == null) {
                return;
            }
            subject = context.subject();
            subjectExtra = context.subjectExtra();
        }

        LivingEntity target = (LivingEntity) (Object) this;
        BlockPos pos = target.blockPosition();
        SpaceLogger.bridge().appendNow(
            pos.getX(),
            pos.getY(),
            pos.getZ(),
            NativeSpaceLoggerBridge.dimension(target.level()),
            subject,
            NativeSpaceLoggerBridge.VERB_HURT,
            NativeSpaceLoggerBridge.entityId(target),
            subjectExtra,
            NativeSpaceLoggerBridge.encodeHurtData(this.getUUID(), dealt)
        );
    }

    @Inject(method = "die", at = @At("HEAD"))
    private void sl$logKill(DamageSource source, CallbackInfo ci) {
        if (this.isRemoved() || this.dead) {
            return;
        }

        String subject;
        String subjectExtra;
        LivingEntity killCredit = this.getKillCredit();
        if (killCredit instanceof ServerPlayer creditPlayer) {
            subject = NativeSpaceLoggerBridge.subject(creditPlayer);
            subjectExtra = NativeSpaceLoggerBridge.subjectExtra(creditPlayer);
        } else if (source.getEntity() instanceof ServerPlayer sourcePlayer) {
            subject = NativeSpaceLoggerBridge.subject(sourcePlayer);
            subjectExtra = NativeSpaceLoggerBridge.subjectExtra(sourcePlayer);
        } else {
            RecordMixinHelper.OperationContext context = RecordMixinHelper.recording();
            if (context == null) {
                return;
            }
            subject = context.subject();
            subjectExtra = context.subjectExtra();
        }

        LivingEntity target = (LivingEntity) (Object) this;
        BlockPos pos = target.blockPosition();
        SpaceLogger.bridge().appendNow(
            pos.getX(),
            pos.getY(),
            pos.getZ(),
            NativeSpaceLoggerBridge.dimension(target.level()),
            subject,
            NativeSpaceLoggerBridge.VERB_KILL,
            NativeSpaceLoggerBridge.entityId(target),
            subjectExtra,
            NativeSpaceLoggerBridge.encodeEntityNbt(target)
        );
    }
}
