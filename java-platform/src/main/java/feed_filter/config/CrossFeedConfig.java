package feed_filter.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Map;

/**
 * Configuration for a single cross/FX feed (e.g. feed_forex).
 * Loaded from conf/feeds/cross_feeds.yaml
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CrossFeedConfig {

    private boolean enabled = true;
    private String appId;
    private Map<String, SymbolConfig> symbols;
    private CrossFeedDefaults defaults;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public Map<String, SymbolConfig> getSymbols() {
        return symbols != null ? symbols : Map.of();
    }

    public void setSymbols(Map<String, SymbolConfig> symbols) {
        this.symbols = symbols;
    }

    public CrossFeedDefaults getDefaults() {
        return defaults != null ? defaults : new CrossFeedDefaults();
    }

    public void setDefaults(CrossFeedDefaults defaults) {
        this.defaults = defaults;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SymbolConfig {
        private String source;   // BINANCE, KRAKEN, etc.
        private String stream;   // eurusdt@bookTicker

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public String getStream() {
            return stream;
        }

        public void setStream(String stream) {
            this.stream = stream;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CrossFeedDefaults {
        private long silenceReconnectMs = 60_000;
        private long midPriceStaleReconnectMs = 120_000;

        public long getSilenceReconnectMs() {
            return silenceReconnectMs;
        }

        public void setSilenceReconnectMs(long silenceReconnectMs) {
            this.silenceReconnectMs = silenceReconnectMs;
        }

        public long getMidPriceStaleReconnectMs() {
            return midPriceStaleReconnectMs;
        }

        public void setMidPriceStaleReconnectMs(long midPriceStaleReconnectMs) {
            this.midPriceStaleReconnectMs = midPriceStaleReconnectMs;
        }
    }
}
