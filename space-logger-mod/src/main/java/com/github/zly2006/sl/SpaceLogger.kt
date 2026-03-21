package com.github.zly2006.sl

import com.github.zly2006.sl.command.SpaceLoggerCommand
import com.github.zly2006.sl.jni.NativeSpaceLoggerBridge
import net.fabricmc.api.ModInitializer
import net.fabricmc.loader.api.FabricLoader

class SpaceLogger : ModInitializer {

    override fun onInitialize() {
        NativeSpaceLoggerBridge.init(FabricLoader.getInstance().gameDir)
        SpaceLoggerCommand.register()
    }
}
