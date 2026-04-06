package me.zly2006.sl.mixin;

import me.zly2006.sl.mixinhelper.RecordMixinHelper;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.player.Player;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

@Mixin(Player.class)
public abstract class PlayerMixin {
    @Inject(method = "attack", at = @At("HEAD"))
    private void sl$startRecordingAttack(Entity target, CallbackInfo ci) {
        if ((Object) this instanceof ServerPlayer player) {
            RecordMixinHelper.playerStartRecording(player, RecordMixinHelper.OperationCause.ATTACK_ENTITY);
        }
    }

    @Inject(method = "attack", at = @At("RETURN"))
    private void sl$stopRecordingAttack(Entity target, CallbackInfo ci) {
        if ((Object) this instanceof ServerPlayer player) {
            RecordMixinHelper.playerStopRecording(player);
        }
    }
}
