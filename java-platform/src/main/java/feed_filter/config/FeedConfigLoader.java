package feed_filter.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Loads feed configurations from YAML files.
 * Default directory: conf/feeds/
 * Can be overridden via FEED_CONFIG_DIR environment variable.
 */
public class FeedConfigLoader {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    private static final String DEFAULT_CONFIG_DIR = "../conf/feeds";
    
    private static final Path CONFIG_DIR = Path.of(
            System.getenv().getOrDefault("FEED_CONFIG_DIR", DEFAULT_CONFIG_DIR)
    );

    /**
     * Load all feed configurations from the config directory.
     * @return Map of exchange name (lowercase) -> FeedConfig
     */
    public static Map<String, FeedConfig> loadAll() {
        Map<String, FeedConfig> configs = new HashMap<>();
        
        File dir = CONFIG_DIR.toFile();
        if (!dir.exists() || !dir.isDirectory()) {
            System.err.println("[FeedConfigLoader] Config directory not found: " + CONFIG_DIR.toAbsolutePath());
            return configs;
        }

        File[] files = dir.listFiles((d, name) -> name.endsWith(".yaml") || name.endsWith(".yml"));
        if (files == null) {
            return configs;
        }

        for (File file : files) {
            try {
                FeedConfig config = YAML_MAPPER.readValue(file, FeedConfig.class);
                String exchangeName = file.getName().replace(".yaml", "").replace(".yml", "");
                configs.put(exchangeName.toLowerCase(), config);
                System.out.println("[FeedConfigLoader] Loaded: " + exchangeName + " -> " + config);
            } catch (IOException e) {
                System.err.println("[FeedConfigLoader] Failed to load " + file.getName() + ": " + e.getMessage());
            }
        }

        return configs;
    }

    /**
     * Load configuration for a specific exchange.
     * @param exchangeName Exchange name (case-insensitive, e.g., "bitget", "BINANCEFUT")
     * @return FeedConfig or null if not found/disabled
     */
    public static FeedConfig load(String exchangeName) {
        String normalizedName = exchangeName.toLowerCase();
        Path file = CONFIG_DIR.resolve(normalizedName + ".yaml");
        
        if (!Files.exists(file)) {
            // Try .yml extension
            file = CONFIG_DIR.resolve(normalizedName + ".yml");
        }
        
        if (!Files.exists(file)) {
            System.out.println("[FeedConfigLoader] No config file for: " + exchangeName + " (looked in " + CONFIG_DIR.toAbsolutePath() + ")");
            return null;
        }

        try {
            FeedConfig config = YAML_MAPPER.readValue(file.toFile(), FeedConfig.class);
            System.out.println("[FeedConfigLoader] Loaded " + exchangeName + ": enabled=" + config.isEnabled() + 
                    ", symbols=" + config.getSymbols().size());
            return config;
        } catch (IOException e) {
            System.err.println("[FeedConfigLoader] Failed to load " + exchangeName + ": " + e.getMessage());
            return null;
        }
    }

    /**
     * Check if feed is enabled for given exchange.
     */
    public static boolean isEnabled(String exchangeName) {
        FeedConfig config = load(exchangeName);
        return config != null && config.isEnabled();
    }
}
