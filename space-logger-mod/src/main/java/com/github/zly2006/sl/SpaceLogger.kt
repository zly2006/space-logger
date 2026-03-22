package com.github.zly2006.sl

import com.github.zly2006.sl.command.SpaceLoggerCommand
import com.github.zly2006.sl.jni.NativeSpaceLoggerBridge
import net.fabricmc.api.ModInitializer
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents
import net.fabricmc.loader.api.FabricLoader
import org.slf4j.LoggerFactory

class SpaceLogger : ModInitializer {
    companion object {
        private val LOGGER = LoggerFactory.getLogger("space-logger-mod/SpaceLogger")
        @Volatile
        private var bridgeInstance: NativeSpaceLoggerBridge? = null

        @JvmStatic
        fun bridge(): NativeSpaceLoggerBridge {
            return bridgeInstance ?: error("NativeSpaceLoggerBridge is not initialized")
        }
    }

    override fun onInitialize() {
        val gameDir = FabricLoader.getInstance().gameDir
        bridgeInstance?.close()
        bridgeInstance = NativeSpaceLoggerBridge.open(
            gameDir,
            gameDir.resolve("space-logger-db"),
            4096
        )
        if (java.lang.Boolean.getBoolean("fabric-api.gametest")) {
            try {
                bridgeInstance?.resetForTests()
                LOGGER.info("Reset native space logger db for gametest run")
            } catch (e: Exception) {
                LOGGER.error("Failed to reset native space logger db for gametest run", e)
            }
        }
        ServerLifecycleEvents.SERVER_STOPPING.register {
            val bridge = bridgeInstance
            if (bridge == null || bridge.isClosed) {
                return@register
            }
            try {
                bridge.flush()
            } catch (e: Exception) {
                LOGGER.error("Failed to flush native space logger during server stop", e)
            }
        }
        SpaceLoggerCommand.register()
    }
}
