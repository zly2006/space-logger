package com.github.zly2006.sl

import net.fabricmc.api.ModInitializer
import net.fabricmc.loader.api.FabricLoader
import com.github.zly2006.sl.logging.SpaceLoggerEventSink

class SpaceLogger : ModInitializer {

    override fun onInitialize() {
        SpaceLoggerEventSink.init(FabricLoader.getInstance().gameDir)
    }
}
