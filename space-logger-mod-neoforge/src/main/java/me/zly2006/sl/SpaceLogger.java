package me.zly2006.sl;

import java.nio.file.Path;
import me.zly2006.sl.command.SpaceLoggerCommand;
import me.zly2006.sl.jni.NativeSpaceLoggerBridge;
import net.neoforged.bus.api.IEventBus;
import net.neoforged.fml.ModContainer;
import net.neoforged.fml.common.Mod;
import net.neoforged.fml.loading.FMLPaths;
import net.neoforged.neoforge.common.NeoForge;
import net.neoforged.neoforge.event.server.ServerStoppingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Mod(SpaceLogger.MOD_ID)
public final class SpaceLogger {
    public static final String MOD_ID = "space_logger_mod";
    private static final Logger LOGGER = LoggerFactory.getLogger("space-logger-mod/SpaceLogger");
    private static volatile NativeSpaceLoggerBridge bridgeInstance;

    public SpaceLogger(IEventBus modEventBus, ModContainer modContainer) {
        Path gameDir = FMLPaths.GAMEDIR.get();
        NativeSpaceLoggerBridge previous = bridgeInstance;
        if (previous != null) {
            previous.close();
        }
        bridgeInstance = NativeSpaceLoggerBridge.open(gameDir, gameDir.resolve("space-logger-db"), 4096);

        if (System.getProperty("neoforge.enabledGameTestNamespaces") != null) {
            resetForGameTests();
        }

        NeoForge.EVENT_BUS.addListener(this::onServerStopping);
        SpaceLoggerCommand.register(NeoForge.EVENT_BUS);
    }

    public static NativeSpaceLoggerBridge bridge() {
        NativeSpaceLoggerBridge bridge = bridgeInstance;
        if (bridge == null) {
            throw new IllegalStateException("NativeSpaceLoggerBridge is not initialized");
        }
        return bridge;
    }

    private static void resetForGameTests() {
        try {
            bridge().resetForTests();
            LOGGER.info("Reset native space logger db for gametest run");
        } catch (Exception exception) {
            LOGGER.error("Failed to reset native space logger db for gametest run", exception);
        }
    }

    private void onServerStopping(ServerStoppingEvent event) {
        NativeSpaceLoggerBridge bridge = bridgeInstance;
        if (bridge == null || bridge.isClosed()) {
            return;
        }
        try {
            bridge.flush();
        } catch (Exception exception) {
            LOGGER.error("Failed to flush native space logger during server stop", exception);
        }
    }
}
