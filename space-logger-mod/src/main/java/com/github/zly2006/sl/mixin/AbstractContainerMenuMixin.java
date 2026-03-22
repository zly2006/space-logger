package com.github.zly2006.sl.mixin;

import com.github.zly2006.sl.SpaceLogger;
import com.github.zly2006.sl.jni.NativeSpaceLoggerBridge;
import net.minecraft.core.BlockPos;
import net.minecraft.core.NonNullList;
import net.minecraft.core.registries.BuiltInRegistries;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.entity.player.Player;
import net.minecraft.world.inventory.ContainerInput;
import net.minecraft.world.inventory.Slot;
import net.minecraft.world.item.ItemStack;
import net.minecraft.world.level.block.entity.BlockEntity;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Mixin(net.minecraft.world.inventory.AbstractContainerMenu.class)
public abstract class AbstractContainerMenuMixin {
    @Shadow
    @Final
    public NonNullList<Slot> slots;

    @Unique
    private Map<Integer, ItemStack> sl$containerBefore = Collections.emptyMap();

    @Inject(method = "clicked", at = @At("HEAD"))
    private void sl$captureBeforeClick(int slotIndex, int buttonNum, ContainerInput containerInput, Player player, CallbackInfo ci) {
        if (!(player instanceof ServerPlayer)) {
            this.sl$containerBefore = Collections.emptyMap();
            return;
        }
        if ((Object) this == player.inventoryMenu) {
            this.sl$containerBefore = Collections.emptyMap();
            return;
        }
        if (player.containerMenu != (Object) this) {
            this.sl$containerBefore = Collections.emptyMap();
            return;
        }

        this.sl$containerBefore = this.sl$captureContainerSnapshot(player);
    }

    @Inject(method = "clicked", at = @At("RETURN"))
    private void sl$logDiffAfterClick(int slotIndex, int buttonNum, ContainerInput containerInput, Player player, CallbackInfo ci) {
        if (!(player instanceof ServerPlayer serverPlayer)) {
            this.sl$containerBefore = Collections.emptyMap();
            return;
        }
        if (this.sl$containerBefore.isEmpty()) {
            return;
        }

        Map<Integer, ItemStack> before = this.sl$containerBefore;
        this.sl$containerBefore = Collections.emptyMap();

        Map<Integer, ItemStack> after = this.sl$captureContainerSnapshot(player);
        Map<String, ItemStack> beforeSamples = new HashMap<>();
        Map<String, ItemStack> afterSamples = new HashMap<>();
        Map<String, Integer> beforeTotals = this.sl$aggregateByItem(before, beforeSamples);
        Map<String, Integer> afterTotals = this.sl$aggregateByItem(after, afterSamples);

        if (beforeTotals.isEmpty() && afterTotals.isEmpty()) {
            return;
        }

        BlockPos pos = this.sl$resolveContainerPos(player);
        Set<String> allKeys = new HashSet<>(beforeTotals.keySet());
        allKeys.addAll(afterTotals.keySet());

        for (String key : allKeys) {
            int beforeCount = beforeTotals.getOrDefault(key, 0);
            int afterCount = afterTotals.getOrDefault(key, 0);

            if (afterCount > beforeCount) {
                ItemStack sample = afterSamples.get(key);
                if (sample != null) {
                    this.sl$appendContainerEvent(
                        serverPlayer,
                        pos,
                        NativeSpaceLoggerBridge.VERB_ADD_ITEM,
                        sample,
                        afterCount - beforeCount
                    );
                }
            } else if (afterCount < beforeCount) {
                ItemStack sample = beforeSamples.get(key);
                if (sample != null) {
                    this.sl$appendContainerEvent(
                        serverPlayer,
                        pos,
                        NativeSpaceLoggerBridge.VERB_REMOVE_ITEM,
                        sample,
                        afterCount - beforeCount
                    );
                }
            }
        }
    }

    @Unique
    private Map<Integer, ItemStack> sl$captureContainerSnapshot(Player player) {
        Map<Integer, ItemStack> snapshot = new HashMap<>();
        for (int i = 0; i < this.slots.size(); i++) {
            Slot slot = this.slots.get(i);
            if (slot.container == player.getInventory()) {
                continue;
            }
            snapshot.put(i, slot.getItem().copy());
        }
        return snapshot;
    }

    @Unique
    private Map<String, Integer> sl$aggregateByItem(Map<Integer, ItemStack> snapshot, Map<String, ItemStack> samples) {
        Map<String, Integer> totals = new HashMap<>();
        for (ItemStack stack : snapshot.values()) {
            if (stack.isEmpty()) {
                continue;
            }

            String key = this.sl$stackKey(stack);
            samples.putIfAbsent(key, stack.copy());
            totals.merge(key, stack.getCount(), Integer::sum);
        }
        return totals;
    }

    @Unique
    private String sl$stackKey(ItemStack stack) {
        return BuiltInRegistries.ITEM.getKey(stack.getItem()) + "|" + ItemStack.hashItemAndComponents(stack);
    }

    @Unique
    private BlockPos sl$resolveContainerPos(Player player) {
        for (Slot slot : this.slots) {
            if (slot.container == player.getInventory()) {
                continue;
            }
            if (slot.container instanceof BlockEntity blockEntity) {
                return blockEntity.getBlockPos();
            }
        }
        return player.blockPosition();
    }

    @Unique
    private void sl$appendContainerEvent(ServerPlayer player, BlockPos pos, int verb, ItemStack stack, int quantityDelta) {
        if (stack.isEmpty()) {
            return;
        }
        if (quantityDelta == 0) {
            return;
        }

        SpaceLogger.bridge().appendNow(
            pos.getX(),
            pos.getY(),
            pos.getZ(),
            NativeSpaceLoggerBridge.subject(player),
            verb,
            NativeSpaceLoggerBridge.itemId(stack),
            NativeSpaceLoggerBridge.subjectExtra(player),
            NativeSpaceLoggerBridge.encodeInventoryDeltaData(stack, quantityDelta, player.registryAccess())
        );
    }
}
