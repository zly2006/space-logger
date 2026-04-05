package com.github.zly2006.sl.mixin;

import com.github.zly2006.sl.mixinhelper.RecordMixinHelper;
import com.mojang.brigadier.ParseResults;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.server.level.ServerPlayer;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

@Mixin(Commands.class)
public abstract class CommandsMixin {
    @Inject(
        method = "performCommand",
        at = @At(
            value = "INVOKE",
            target = "Lnet/minecraft/commands/Commands;executeCommandInContext(Lnet/minecraft/commands/CommandSourceStack;Ljava/util/function/Consumer;)V",
            shift = At.Shift.BEFORE
        )
    )
    private void sl$startRecordingCommand(ParseResults<CommandSourceStack> parseResults, String command, CallbackInfo ci) {
        if (parseResults.getContext().getSource().getEntity() instanceof ServerPlayer player) {
            RecordMixinHelper.playerStartRecording(player, RecordMixinHelper.OperationCause.COMMAND);
        }
    }

    @Inject(
        method = "performCommand",
        at = @At(
            value = "INVOKE",
            target = "Lnet/minecraft/commands/Commands;executeCommandInContext(Lnet/minecraft/commands/CommandSourceStack;Ljava/util/function/Consumer;)V",
            shift = At.Shift.AFTER
        )
    )
    private void sl$stopRecordingCommand(ParseResults<CommandSourceStack> parseResults, String command, CallbackInfo ci) {
        if (parseResults.getContext().getSource().getEntity() instanceof ServerPlayer player) {
            RecordMixinHelper.playerStopRecording(player);
        }
    }
}
