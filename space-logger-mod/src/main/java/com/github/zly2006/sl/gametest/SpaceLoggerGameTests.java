package com.github.zly2006.sl.gametest;

import com.github.zly2006.sl.jni.NativeSpaceLoggerBridge;
import net.fabricmc.fabric.api.gametest.v1.GameTest;
import net.minecraft.core.BlockPos;
import net.minecraft.core.Direction;
import net.minecraft.gametest.framework.GameTestHelper;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.InteractionHand;
import net.minecraft.world.InteractionResult;
import net.minecraft.world.entity.EntityType;
import net.minecraft.world.item.ItemStack;
import net.minecraft.world.item.Items;
import net.minecraft.world.level.GameType;
import net.minecraft.world.level.block.Blocks;
import net.minecraft.world.phys.BlockHitResult;
import net.minecraft.world.phys.Vec3;

public class SpaceLoggerGameTests {
    @GameTest
    public void recordsHurtAndKill(GameTestHelper helper) {
        int hurtBefore = NativeSpaceLoggerBridge.countByVerb("hurt");
        int killBefore = NativeSpaceLoggerBridge.countByVerb("kill");
        int noMatchBefore = NativeSpaceLoggerBridge.countByVerb("verb_does_not_exist");

        ServerPlayer player = helper.makeMockServerPlayerInLevel();
        player.setGameMode(GameType.SURVIVAL);

        net.minecraft.world.entity.monster.zombie.Zombie zombie = helper.spawn(EntityType.ZOMBIE, new BlockPos(1, 1, 1));
        helper.hurt(zombie, player.damageSources().playerAttack(player), 2.0F);
        helper.hurt(zombie, player.damageSources().playerAttack(player), 100.0F);

        helper.runAfterDelay(2, () -> {
            int hurtCount = NativeSpaceLoggerBridge.countByVerb("hurt");
            int killCount = NativeSpaceLoggerBridge.countByVerb("kill");
            int noMatchCount = NativeSpaceLoggerBridge.countByVerb("verb_does_not_exist");

            helper.assertTrue(hurtCount > hurtBefore, "expected hurt count to increase");
            helper.assertTrue(killCount > killBefore, "expected kill count to increase");
            helper.assertTrue(noMatchCount == noMatchBefore, "expected unknown verb count unchanged");
            helper.succeed();
        });
    }

    @GameTest
    public void recordsBreakPlaceUse(GameTestHelper helper) {
        int placeBefore = NativeSpaceLoggerBridge.countByVerb("place");
        int breakBefore = NativeSpaceLoggerBridge.countByVerb("break");
        int useBefore = NativeSpaceLoggerBridge.countByVerb("use");
        int noMatchBefore = NativeSpaceLoggerBridge.countByVerb("verb_does_not_exist");

        ServerPlayer player = helper.makeMockServerPlayerInLevel();
        player.setGameMode(GameType.SURVIVAL);

        BlockPos placePos = new BlockPos(1, 1, 1);
        BlockPos breakPos = new BlockPos(2, 1, 1);
        BlockPos usePos = new BlockPos(3, 1, 1);

        helper.setBlock(placePos, Blocks.STONE);
        player.setItemInHand(InteractionHand.MAIN_HAND, new ItemStack(Items.STONE));
        BlockPos absPlacePos = helper.absolutePos(placePos);
        BlockHitResult placeHit = new BlockHitResult(
            Vec3.atCenterOf(absPlacePos),
            Direction.UP,
            absPlacePos,
            false
        );
        InteractionResult placeResult = player.gameMode.useItemOn(
            player,
            helper.getLevel(),
            player.getItemInHand(InteractionHand.MAIN_HAND),
            InteractionHand.MAIN_HAND,
            placeHit
        );
        helper.assertTrue(placeResult.consumesAction(), "expected place useItemOn to consume action");

        helper.setBlock(breakPos, Blocks.STONE);
        BlockPos absBreakPos = helper.absolutePos(breakPos);
        player.setPos(absBreakPos.getX() + 0.5, absBreakPos.getY() + 1.0, absBreakPos.getZ() + 0.5);
        player.gameMode.destroyBlock(absBreakPos);

        helper.setBlock(usePos, Blocks.LEVER, Direction.NORTH);
        BlockPos absUsePos = helper.absolutePos(usePos);
        player.setPos(absUsePos.getX() + 0.5, absUsePos.getY() + 1.0, absUsePos.getZ() + 0.5);
        BlockHitResult useHit = new BlockHitResult(
            Vec3.atCenterOf(absUsePos),
            Direction.NORTH,
            absUsePos,
            true
        );
        InteractionResult useResult = player.gameMode.useItemOn(
            player,
            helper.getLevel(),
            player.getItemInHand(InteractionHand.MAIN_HAND),
            InteractionHand.MAIN_HAND,
            useHit
        );
        helper.assertTrue(useResult.consumesAction(), "expected useItemOn to consume action");

        helper.runAfterDelay(2, () -> {
            int placeCount = NativeSpaceLoggerBridge.countByVerb("place");
            int breakCount = NativeSpaceLoggerBridge.countByVerb("break");
            int useCount = NativeSpaceLoggerBridge.countByVerb("use");
            int noMatchCount = NativeSpaceLoggerBridge.countByVerb("verb_does_not_exist");

            helper.assertTrue(placeCount > placeBefore, "expected place count to increase");
            helper.assertTrue(breakCount > breakBefore, "expected break count to increase");
            helper.assertTrue(useCount > useBefore, "expected use count to increase");
            helper.assertTrue(noMatchCount == noMatchBefore, "expected unknown verb count unchanged");
            helper.succeed();
        });
    }
}
