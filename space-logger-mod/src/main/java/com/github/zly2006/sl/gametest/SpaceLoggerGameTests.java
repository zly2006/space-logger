package com.github.zly2006.sl.gametest;

import com.github.zly2006.sl.SpaceLogger;
import com.github.zly2006.sl.jni.NativeSpaceLoggerBridge;
import net.fabricmc.fabric.api.gametest.v1.GameTest;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.core.BlockPos;
import net.minecraft.core.Direction;
import net.minecraft.gametest.framework.GameTestHelper;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.server.permissions.PermissionSet;
import net.minecraft.world.InteractionHand;
import net.minecraft.world.InteractionResult;
import net.minecraft.world.inventory.AbstractContainerMenu;
import net.minecraft.world.inventory.ContainerInput;
import net.minecraft.world.inventory.Slot;
import net.minecraft.world.entity.EntityType;
import net.minecraft.world.entity.player.Player;
import net.minecraft.world.item.ItemStack;
import net.minecraft.world.item.Items;
import net.minecraft.world.level.GameType;
import net.minecraft.world.level.block.Blocks;
import net.minecraft.world.phys.BlockHitResult;
import net.minecraft.world.phys.Vec3;
import net.minecraft.world.entity.monster.zombie.Zombie;

public class SpaceLoggerGameTests {
    private static int mask(int verbId) {
        return NativeSpaceLoggerBridge.verbMaskSingle(verbId);
    }

    @GameTest
    public void recordsHurtAndKill(GameTestHelper helper) {
        int hurtBefore = SpaceLogger.bridge().countByVerb(NativeSpaceLoggerBridge.VERB_HURT);
        int killBefore = SpaceLogger.bridge().countByVerb(NativeSpaceLoggerBridge.VERB_KILL);
        int noMatchBefore = SpaceLogger.bridge().countByVerb(-1);

        ServerPlayer player = helper.makeMockServerPlayerInLevel();
        player.setGameMode(GameType.SURVIVAL);

        net.minecraft.world.entity.monster.zombie.Zombie zombie = helper.spawn(EntityType.ZOMBIE, new BlockPos(1, 1, 1));
        helper.hurt(zombie, player.damageSources().playerAttack(player), 100.0F);

        helper.runAfterDelay(2, () -> {
            int hurtCount = SpaceLogger.bridge().countByVerb(NativeSpaceLoggerBridge.VERB_HURT);
            int killCount = SpaceLogger.bridge().countByVerb(NativeSpaceLoggerBridge.VERB_KILL);
            int noMatchCount = SpaceLogger.bridge().countByVerb(-1);

            helper.assertTrue(hurtCount == hurtBefore + 1, "expected hurt count to increase by exactly 1");
            helper.assertTrue(killCount == killBefore + 1, "expected kill count to increase by exactly 1");
            helper.assertTrue(noMatchCount == noMatchBefore, "expected unknown verb count unchanged");
            helper.succeed();
        });
    }

    @GameTest
    public void recordsBreakPlaceUse(GameTestHelper helper) {
        int noMatchBefore = SpaceLogger.bridge().countByVerb(-1);
        long startTimeMs = System.currentTimeMillis();

        ServerPlayer player = helper.makeMockServerPlayerInLevel();
        player.setGameMode(GameType.SURVIVAL);

        BlockPos placePos = new BlockPos(1, 1, 1);
        BlockPos breakPos = new BlockPos(2, 1, 1);
        BlockPos usePos = new BlockPos(3, 1, 1);
        String subject = NativeSpaceLoggerBridge.subject(player);
        String stoneObject = NativeSpaceLoggerBridge.blockId(Blocks.STONE.defaultBlockState());

        helper.setBlock(placePos, Blocks.STONE);
        player.setItemInHand(InteractionHand.MAIN_HAND, new ItemStack(Items.STONE));
        BlockPos absPlacePos = helper.absolutePos(placePos);
        BlockPos absPlacedBlockPos = absPlacePos.above();
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
        player.setItemInHand(InteractionHand.MAIN_HAND, ItemStack.EMPTY);
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
            int placeCount = countRowsAt(
                subject,
                stoneObject,
                NativeSpaceLoggerBridge.VERB_PLACE,
                absPlacedBlockPos,
                startTimeMs
            );
            int breakCount = countRowsAt(
                subject,
                stoneObject,
                NativeSpaceLoggerBridge.VERB_BREAK,
                absBreakPos,
                startTimeMs
            );
            int noMatchCount = SpaceLogger.bridge().countByVerb(-1);
            int useAtPlacePos = SpaceLogger.bridge().queryRows(
                "",
                "",
                mask(NativeSpaceLoggerBridge.VERB_USE),
                absPlacePos.getX(),
                absPlacePos.getX(),
                absPlacePos.getY(),
                absPlacePos.getY(),
                absPlacePos.getZ(),
                absPlacePos.getZ(),
                startTimeMs,
                Long.MAX_VALUE,
                32
            ).size();
            int useAtUsePos = SpaceLogger.bridge().queryRows(
                "",
                "",
                mask(NativeSpaceLoggerBridge.VERB_USE),
                absUsePos.getX(),
                absUsePos.getX(),
                absUsePos.getY(),
                absUsePos.getY(),
                absUsePos.getZ(),
                absUsePos.getZ(),
                startTimeMs,
                Long.MAX_VALUE,
                32
            ).size();

            helper.assertTrue(
                placeCount == 1,
                "expected exactly one place row at placed block position, actual=" + placeCount
            );
            helper.assertTrue(
                breakCount == 1,
                "expected exactly one break row at broken block position, actual=" + breakCount
            );
            helper.assertTrue(useAtPlacePos == 0, "expected place action to not log use");
            helper.assertTrue(useAtUsePos == 1, "expected exactly one use row on lever position");
            helper.assertTrue(noMatchCount == noMatchBefore, "expected unknown verb count unchanged");
            helper.succeed();
        });
    }

    @GameTest
    public void recordsAddAndRemoveItem(GameTestHelper helper) {
        int noMatchBefore = SpaceLogger.bridge().countByVerb(-1);
        long startTimeMs = System.currentTimeMillis();

        ServerPlayer player = helper.makeMockServerPlayerInLevel();
        player.setGameMode(GameType.SURVIVAL);

        BlockPos chestPos = new BlockPos(1, 1, 1);
        helper.setBlock(chestPos, Blocks.CHEST);
        BlockPos absChestPos = helper.absolutePos(chestPos);

        player.getInventory().setItem(0, new ItemStack(Items.DIRT, 2));
        player.getInventory().setItem(1, ItemStack.EMPTY);
        helper.getBlockEntity(chestPos, net.minecraft.world.level.block.entity.ChestBlockEntity.class)
            .setItem(1, new ItemStack(Items.COBBLESTONE, 3));

        player.setPos(absChestPos.getX() + 0.5D, absChestPos.getY() + 1.0D, absChestPos.getZ() + 0.5D);
        BlockHitResult chestHit = new BlockHitResult(
            Vec3.atCenterOf(absChestPos),
            Direction.NORTH,
            absChestPos,
            true
        );
        InteractionResult openResult = player.gameMode.useItemOn(
            player,
            helper.getLevel(),
            player.getItemInHand(InteractionHand.MAIN_HAND),
            InteractionHand.MAIN_HAND,
            chestHit
        );
        helper.assertTrue(openResult.consumesAction(), "expected chest open to consume action");

        AbstractContainerMenu menu = player.containerMenu;
        helper.assertTrue(menu != player.inventoryMenu, "expected a container menu to be opened");

        int playerHotbar0 = findMenuSlotIndex(menu, player, true, 0);
        int playerHotbar1 = findMenuSlotIndex(menu, player, true, 1);
        int chestSlot0 = findMenuSlotIndex(menu, player, false, 0);
        int chestSlot1 = findMenuSlotIndex(menu, player, false, 1);
        helper.assertTrue(playerHotbar0 >= 0, "failed to locate player hotbar slot 0 in menu");
        helper.assertTrue(playerHotbar1 >= 0, "failed to locate player hotbar slot 1 in menu");
        helper.assertTrue(chestSlot0 >= 0, "failed to locate chest slot 0 in menu");
        helper.assertTrue(chestSlot1 >= 0, "failed to locate chest slot 1 in menu");

        menu.clicked(playerHotbar0, 0, ContainerInput.PICKUP, player);
        menu.clicked(chestSlot0, 0, ContainerInput.PICKUP, player);
        ItemStack addedStack = menu.getSlot(chestSlot0).getItem();
        helper.assertTrue(
            addedStack.getItem() == Items.DIRT && addedStack.getCount() == 2,
            "expected chest slot 0 to contain 2 dirt after add click sequence"
        );

        menu.clicked(chestSlot1, 0, ContainerInput.PICKUP, player);
        menu.clicked(playerHotbar1, 0, ContainerInput.PICKUP, player);
        ItemStack removedFromChest = menu.getSlot(chestSlot1).getItem();
        helper.assertTrue(
            removedFromChest.isEmpty(),
            "expected chest slot 1 to be empty after remove click sequence"
        );

        helper.runAfterDelay(2, () -> {
            var addRows = SpaceLogger.bridge().queryRows(
                "",
                "",
                mask(NativeSpaceLoggerBridge.VERB_ADD_ITEM),
                absChestPos.getX(),
                absChestPos.getX(),
                absChestPos.getY(),
                absChestPos.getY(),
                absChestPos.getZ(),
                absChestPos.getZ(),
                startTimeMs,
                Long.MAX_VALUE,
                32
            );
            var removeRows = SpaceLogger.bridge().queryRows(
                "",
                "",
                mask(NativeSpaceLoggerBridge.VERB_REMOVE_ITEM),
                absChestPos.getX(),
                absChestPos.getX(),
                absChestPos.getY(),
                absChestPos.getY(),
                absChestPos.getZ(),
                absChestPos.getZ(),
                startTimeMs,
                Long.MAX_VALUE,
                32
            );
            int addCount = addRows.size();
            int removeCount = removeRows.size();
            int noMatchCount = SpaceLogger.bridge().countByVerb(-1);
            int addGlobal = SpaceLogger.bridge().countByVerb(NativeSpaceLoggerBridge.VERB_ADD_ITEM);
            int removeGlobal = SpaceLogger.bridge().countByVerb(NativeSpaceLoggerBridge.VERB_REMOVE_ITEM);

            helper.assertTrue(
                addCount == 1,
                "expected add_item count in chest area to be exactly 1, actual=" + addCount + ", global=" + addGlobal
            );
            helper.assertTrue(
                removeCount == 1,
                "expected remove_item count in chest area to be exactly 1, actual=" + removeCount + ", global=" + removeGlobal
            );
            NativeSpaceLoggerBridge.QueryRow addRow = addRows.getFirst();
            NativeSpaceLoggerBridge.QueryRow removeRow = removeRows.getFirst();
            helper.assertTrue(addRow.dataLen() > 12, "expected add_item row dataLen > 12");
            helper.assertTrue(removeRow.dataLen() > 12, "expected remove_item row dataLen > 12");
            helper.assertTrue(addRow.dataHead().length == 12, "expected add_item data head to be 12 bytes");
            helper.assertTrue(removeRow.dataHead().length == 12, "expected remove_item data head to be 12 bytes");
            helper.assertTrue(addRow.hasInventoryDataHeader(), "expected add_item row to include inventory header");
            helper.assertTrue(removeRow.hasInventoryDataHeader(), "expected remove_item row to include inventory header");
            helper.assertTrue(addRow.quantityDelta() == 2, "expected add_item quantity delta = +2");
            helper.assertTrue(removeRow.quantityDelta() == -3, "expected remove_item quantity delta = -3");
            helper.assertTrue(addRow.nbtPayloadLen() > 0, "expected add_item nbt payload len > 0");
            helper.assertTrue(removeRow.nbtPayloadLen() > 0, "expected remove_item nbt payload len > 0");
            helper.assertTrue(noMatchCount == noMatchBefore, "expected unknown verb count unchanged");
            helper.succeed();
        });
    }

    @GameTest
    public void recordsAttachedTorchWhenSupportingBlockBroken(GameTestHelper helper) {
        long startTimeMs = System.currentTimeMillis();
        ServerPlayer player = helper.makeMockServerPlayerInLevel();
        player.setGameMode(GameType.SURVIVAL);

        BlockPos supportPos = new BlockPos(1, 1, 1);
        BlockPos torchPos = supportPos.above();
        BlockPos absSupportPos = helper.absolutePos(supportPos);
        BlockPos absTorchPos = helper.absolutePos(torchPos);
        String subject = NativeSpaceLoggerBridge.subject(player);
        String supportObject = NativeSpaceLoggerBridge.blockId(Blocks.STONE.defaultBlockState());
        String torchObject = NativeSpaceLoggerBridge.blockId(Blocks.TORCH.defaultBlockState());

        helper.setBlock(supportPos, Blocks.STONE);
        helper.setBlock(torchPos, Blocks.TORCH);

        player.setPos(absSupportPos.getX() + 0.5D, absSupportPos.getY() + 1.0D, absSupportPos.getZ() + 0.5D);
        boolean destroyed = player.gameMode.destroyBlock(absSupportPos);
        helper.assertTrue(destroyed, "expected supporting block destroy to succeed");

        helper.runAfterDelay(2, () -> {
            helper.assertTrue(
                helper.getLevel().getBlockState(absSupportPos).isAir(),
                "expected supporting block to be removed"
            );
            helper.assertTrue(
                helper.getLevel().getBlockState(absTorchPos).isAir(),
                "expected attached torch to be removed after support break"
            );

            int supportBreakCount = countRowsAt(
                subject,
                supportObject,
                NativeSpaceLoggerBridge.VERB_BREAK,
                absSupportPos,
                startTimeMs
            );
            int torchBreakCount = countRowsAt(
                subject,
                torchObject,
                NativeSpaceLoggerBridge.VERB_BREAK,
                absTorchPos,
                startTimeMs
            );

            helper.assertTrue(
                supportBreakCount == 1,
                "expected exactly one supporting block break row, actual=" + supportBreakCount
            );
            helper.assertTrue(
                torchBreakCount == 1,
                "expected exactly one attached torch break row, actual=" + torchBreakCount
            );
            helper.succeed();
        });
    }

    @GameTest(maxTicks = 160)
    public void recordsChainTntBreakForIgnitingPlayer(GameTestHelper helper) {
        long startTimeMs = System.currentTimeMillis();
        ServerPlayer player = helper.makeMockServerPlayerInLevel();
        player.setGameMode(GameType.SURVIVAL);

        BlockPos tnt1Pos = new BlockPos(1, 2, 1);
        BlockPos tnt2Pos = new BlockPos(4, 2, 1);
        BlockPos targetStonePos = new BlockPos(7, 2, 1);
        BlockPos absTnt1Pos = helper.absolutePos(tnt1Pos);
        BlockPos absTargetStonePos = helper.absolutePos(targetStonePos);
        String subject = NativeSpaceLoggerBridge.subject(player);
        String stoneObject = NativeSpaceLoggerBridge.blockId(Blocks.STONE.defaultBlockState());

        for (int x = 1; x <= 7; x++) {
            helper.setBlock(new BlockPos(x, 1, 1), Blocks.OBSIDIAN);
        }
        helper.setBlock(tnt1Pos, Blocks.TNT);
        helper.setBlock(tnt2Pos, Blocks.TNT);
        helper.setBlock(targetStonePos, Blocks.STONE);

        player.setItemInHand(InteractionHand.MAIN_HAND, new ItemStack(Items.FLINT_AND_STEEL));
        player.setPos(absTnt1Pos.getX() + 0.5D, absTnt1Pos.getY() + 1.0D, absTnt1Pos.getZ() + 0.5D);
        BlockHitResult igniteHit = new BlockHitResult(
            Vec3.atCenterOf(absTnt1Pos),
            Direction.UP,
            absTnt1Pos,
            false
        );
        InteractionResult igniteResult = player.gameMode.useItemOn(
            player,
            helper.getLevel(),
            player.getItemInHand(InteractionHand.MAIN_HAND),
            InteractionHand.MAIN_HAND,
            igniteHit
        );
        helper.assertTrue(igniteResult.consumesAction(), "expected TNT ignition to consume action");

        player.setPos(absTargetStonePos.getX() + 8.0D, absTargetStonePos.getY() + 2.0D, absTargetStonePos.getZ() + 8.0D);

        helper.runAfterDelay(85, () -> {
            helper.assertTrue(
                helper.getLevel().getBlockState(absTargetStonePos).is(Blocks.STONE),
                "expected target stone to survive TNT1 explosion before TNT2 explodes"
            );
            int earlyBreakCount = countRowsAt(
                subject,
                stoneObject,
                NativeSpaceLoggerBridge.VERB_BREAK,
                absTargetStonePos,
                startTimeMs
            );
            helper.assertTrue(
                earlyBreakCount == 0,
                "expected no stone break row before TNT2 explosion, actual=" + earlyBreakCount
            );
        });

        helper.succeedWhen(() -> {
            helper.assertTrue(
                helper.getLevel().getBlockState(absTargetStonePos).isAir(),
                "expected TNT2 explosion to destroy the target stone"
            );
            int finalBreakCount = countRowsAt(
                subject,
                stoneObject,
                NativeSpaceLoggerBridge.VERB_BREAK,
                absTargetStonePos,
                startTimeMs
            );
            helper.assertTrue(
                finalBreakCount == 1,
                "expected exactly one target stone break row for igniting player, actual=" + finalBreakCount
            );
        });
    }

    @GameTest
    public void recordsFillCommandBreaksBlocksForPlayer(GameTestHelper helper) {
        long startTimeMs = System.currentTimeMillis();
        ServerPlayer player = helper.makeMockServerPlayerInLevel();
        player.setGameMode(GameType.SURVIVAL);

        BlockPos firstPos = new BlockPos(1, 1, 1);
        BlockPos secondPos = new BlockPos(2, 1, 1);
        BlockPos absFirstPos = helper.absolutePos(firstPos);
        BlockPos absSecondPos = helper.absolutePos(secondPos);
        String subject = NativeSpaceLoggerBridge.subject(player);
        String stoneObject = NativeSpaceLoggerBridge.blockId(Blocks.STONE.defaultBlockState());

        helper.setBlock(firstPos, Blocks.STONE);
        helper.setBlock(secondPos, Blocks.STONE);

        CommandSourceStack commandSource = player.createCommandSourceStack()
            .withPermission(PermissionSet.ALL_PERMISSIONS);
        String command = String.format(
            "/fill %d %d %d %d %d %d air destroy",
            absFirstPos.getX(),
            absFirstPos.getY(),
            absFirstPos.getZ(),
            absSecondPos.getX(),
            absSecondPos.getY(),
            absSecondPos.getZ()
        );
        helper.getLevel().getServer().getCommands().performPrefixedCommand(commandSource, command);

        helper.runAfterDelay(2, () -> {
            helper.assertTrue(helper.getLevel().getBlockState(absFirstPos).isAir(), "expected first fill target to be removed");
            helper.assertTrue(helper.getLevel().getBlockState(absSecondPos).isAir(), "expected second fill target to be removed");

            int firstBreakCount = countRowsAt(
                subject,
                stoneObject,
                NativeSpaceLoggerBridge.VERB_BREAK,
                absFirstPos,
                startTimeMs
            );
            int secondBreakCount = countRowsAt(
                subject,
                stoneObject,
                NativeSpaceLoggerBridge.VERB_BREAK,
                absSecondPos,
                startTimeMs
            );

            helper.assertTrue(firstBreakCount == 1, "expected exactly one break row at first fill target, actual=" + firstBreakCount);
            helper.assertTrue(secondBreakCount == 1, "expected exactly one break row at second fill target, actual=" + secondBreakCount);
            helper.succeed();
        });
    }

    @GameTest
    public void flushMergesContinuousRemoveThenAdd(GameTestHelper helper) {
        long startTimeMs = System.currentTimeMillis();
        ServerPlayer player = helper.makeMockServerPlayerInLevel();
        player.setGameMode(GameType.SURVIVAL);

        BlockPos absPos = helper.absolutePos(new BlockPos(6, 1, 6));
        ItemStack sample = new ItemStack(Items.DIRT, 4);
        String subject = NativeSpaceLoggerBridge.subject(player);
        String subjectExtra = NativeSpaceLoggerBridge.subjectExtra(player);
        String object = NativeSpaceLoggerBridge.itemId(sample);

        SpaceLogger.bridge().appendNow(
            absPos.getX(),
            absPos.getY(),
            absPos.getZ(),
            subject,
            NativeSpaceLoggerBridge.VERB_REMOVE_ITEM,
            object,
            subjectExtra,
            NativeSpaceLoggerBridge.encodeInventoryDeltaData(sample, -4, player.registryAccess())
        );
        SpaceLogger.bridge().appendNow(
            absPos.getX(),
            absPos.getY(),
            absPos.getZ(),
            subject,
            NativeSpaceLoggerBridge.VERB_ADD_ITEM,
            object,
            subjectExtra,
            NativeSpaceLoggerBridge.encodeInventoryDeltaData(sample, 4, player.registryAccess())
        );
        SpaceLogger.bridge().flush();

        helper.runAfterDelay(2, () -> {
            int removeCount = SpaceLogger.bridge().queryRows(
                subject,
                object,
                mask(NativeSpaceLoggerBridge.VERB_REMOVE_ITEM),
                absPos.getX(),
                absPos.getX(),
                absPos.getY(),
                absPos.getY(),
                absPos.getZ(),
                absPos.getZ(),
                startTimeMs,
                Long.MAX_VALUE,
                32
            ).size();
            int addCount = SpaceLogger.bridge().queryRows(
                subject,
                object,
                mask(NativeSpaceLoggerBridge.VERB_ADD_ITEM),
                absPos.getX(),
                absPos.getX(),
                absPos.getY(),
                absPos.getY(),
                absPos.getZ(),
                absPos.getZ(),
                startTimeMs,
                Long.MAX_VALUE,
                32
            ).size();

            helper.assertTrue(removeCount == 0, "expected remove_item rows to be merged away on flush");
            helper.assertTrue(addCount == 0, "expected add_item rows to be merged away on flush");
            helper.succeed();
        });
    }

    @GameTest
    public void flushDoesNotMergeAcrossUseBarrier(GameTestHelper helper) {
        long startTimeMs = System.currentTimeMillis();
        ServerPlayer player = helper.makeMockServerPlayerInLevel();
        player.setGameMode(GameType.SURVIVAL);

        BlockPos absPos = helper.absolutePos(new BlockPos(7, 1, 7));
        ItemStack sample = new ItemStack(Items.DIRT, 2);
        String subject = NativeSpaceLoggerBridge.subject(player);
        String subjectExtra = NativeSpaceLoggerBridge.subjectExtra(player);
        String object = NativeSpaceLoggerBridge.itemId(sample);

        SpaceLogger.bridge().appendNow(
            absPos.getX(),
            absPos.getY(),
            absPos.getZ(),
            subject,
            NativeSpaceLoggerBridge.VERB_REMOVE_ITEM,
            object,
            subjectExtra,
            NativeSpaceLoggerBridge.encodeInventoryDeltaData(sample, -2, player.registryAccess())
        );
        SpaceLogger.bridge().appendNow(
            absPos.getX(),
            absPos.getY(),
            absPos.getZ(),
            subject,
            NativeSpaceLoggerBridge.VERB_USE,
            "stone",
            subjectExtra,
            new byte[0]
        );
        SpaceLogger.bridge().appendNow(
            absPos.getX(),
            absPos.getY(),
            absPos.getZ(),
            subject,
            NativeSpaceLoggerBridge.VERB_ADD_ITEM,
            object,
            subjectExtra,
            NativeSpaceLoggerBridge.encodeInventoryDeltaData(sample, 2, player.registryAccess())
        );
        SpaceLogger.bridge().flush();

        helper.runAfterDelay(2, () -> {
            int removeCount = SpaceLogger.bridge().queryRows(
                subject,
                object,
                mask(NativeSpaceLoggerBridge.VERB_REMOVE_ITEM),
                absPos.getX(),
                absPos.getX(),
                absPos.getY(),
                absPos.getY(),
                absPos.getZ(),
                absPos.getZ(),
                startTimeMs,
                Long.MAX_VALUE,
                32
            ).size();
            int addCount = SpaceLogger.bridge().queryRows(
                subject,
                object,
                mask(NativeSpaceLoggerBridge.VERB_ADD_ITEM),
                absPos.getX(),
                absPos.getX(),
                absPos.getY(),
                absPos.getY(),
                absPos.getZ(),
                absPos.getZ(),
                startTimeMs,
                Long.MAX_VALUE,
                32
            ).size();

            helper.assertTrue(removeCount == 1, "expected remove_item row to remain because use should break merge");
            helper.assertTrue(addCount == 1, "expected add_item row to remain because use should break merge");
            helper.succeed();
        });
    }

    private static int findMenuSlotIndex(AbstractContainerMenu menu, Player player, boolean playerInventory, int containerSlot) {
        for (int i = 0; i < menu.slots.size(); i++) {
            Slot slot = menu.slots.get(i);
            boolean isPlayerInventorySlot = slot.container == player.getInventory();
            if (isPlayerInventorySlot != playerInventory) {
                continue;
            }
            if (slot.getContainerSlot() == containerSlot) {
                return i;
            }
        }
        return -1;
    }

    private static int countRowsAt(String subject, String object, int verbId, BlockPos pos, long startTimeMs) {
        return SpaceLogger.bridge().queryRows(
            subject,
            object,
            mask(verbId),
            pos.getX(),
            pos.getX(),
            pos.getY(),
            pos.getY(),
            pos.getZ(),
            pos.getZ(),
            startTimeMs,
            Long.MAX_VALUE,
            32
        ).size();
    }
}
