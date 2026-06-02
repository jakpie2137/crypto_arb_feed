package feed_filter.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Loads cross feed configurations from conf/feeds/cross_feeds.yaml.
 * File structure: { feedId: { enabled, appId, symbols: { SYMBOL: { source, stream } }, defaults } }
 */
public class CrossFeedConfigLoader {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    private static final String DEFAULT_CONFIG_DIR = "../conf/feeds";

    private static final Path CONFIG_FILE = Path.of(
            System.getenv().getOrDefault("FEED_CONFIG_DIR", DEFAULT_CONFIG_DIR),
            "cross_feeds.yaml"
    );

    /**
     * Load all cross feed configurations.
     * @return Map of feedId (e.g. "feed_forex") -> CrossFeedConfig
     */
    @SuppressWarnings("unchecked")
    public static Map<String, CrossFeedConfig> loadAll() {
        Map<String, CrossFeedConfig> result = new HashMap<>();
        File file = CONFIG_FILE.toFile();

        if (!file.exists() || !file.isFile()) {
            System.out.println("[CrossFeedConfigLoader] No cross_feeds.yaml at " + CONFIG_FILE.toAbsolutePath());
            return result;
        }

        try {
            Map<String, Object> raw = YAML_MAPPER.readValue(file, Map.class);
            for (Map.Entry<String, Object> entry : raw.entrySet()) {
                String feedId = entry.getKey();
                Object value = entry.getValue();
                if (value instanceof Map) {
                    CrossFeedConfig cfg = YAML_MAPPER.convertValue(value, CrossFeedConfig.class);
                    if (cfg.getAppId() == null || cfg.getAppId().isBlank()) {
                        cfg.setAppId(feedId);
                    }
                    result.put(feedId, cfg);
                    System.out.println("[CrossFeedConfigLoader] Loaded cross feed: " + feedId +
                            " enabled=" + cfg.isEnabled() + " symbols=" + cfg.getSymbols().keySet());
                }
            }
        } catch (IOException e) {
            System.err.println("[CrossFeedConfigLoader] Failed to load: " + e.getMessage());
        }

        return result;
    }
}
