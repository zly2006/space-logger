package com.github.zly2006.sl

import com.github.zly2006.sl.command.SpaceLoggerCommand
import com.github.zly2006.sl.jni.NativeSpaceLoggerBridge
import net.fabricmc.api.ModInitializer
import net.fabricmc.loader.api.FabricLoader

class SpaceLogger : ModInitializer {
    companion object {
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
        SpaceLoggerCommand.register()
    }
}
