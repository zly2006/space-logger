package me.zly2006.sl;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class NeoForgeMetadataTest {
    @Test
    void declaresModAndMixinConfiguration() throws IOException {
        String metadata;
        try (var input = getClass().getResourceAsStream("/META-INF/neoforge.mods.toml")) {
            assertTrue(input != null, "processed NeoForge metadata should be on the test classpath");
            metadata = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
        assertTrue(metadata.contains("modId=\"space_logger_mod\""));
        assertTrue(metadata.contains("config=\"space-logger-mod-neoforge.mixins.json\""));
    }
}
