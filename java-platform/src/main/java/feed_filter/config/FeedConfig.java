package feed_filter.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/**
 * Configuration model for exchange feed.
 * Loaded from conf/feeds/{exchange}.yaml
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FeedConfig {

    private String appId;
    private boolean enabled = true;
    private FeedDefaults defaults;
    private List<String> symbols;

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public FeedDefaults getDefaults() {
        return defaults != null ? defaults : new FeedDefaults();
    }

    public void setDefaults(FeedDefaults defaults) {
        this.defaults = defaults;
    }

    public List<String> getSymbols() {
        return symbols != null ? symbols : List.of();
    }

    public void setSymbols(List<String> symbols) {
        this.symbols = symbols;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FeedDefaults {
        private long orderbookStaleThresholdMs = 10_000;
        private long reconnectBatchStreamThresholdMs = 30_000;
        private long globalSilenceTimeoutMs = 60_000;  // reconnect if no message for this long
        private int depth = 20;
        private int batchSize = 50;
        private long fundingPollIntervalMs = 900_000;

        public long getOrderbookStaleThresholdMs() {
            return orderbookStaleThresholdMs;
        }

        public void setOrderbookStaleThresholdMs(long orderbookStaleThresholdMs) {
            this.orderbookStaleThresholdMs = orderbookStaleThresholdMs;
        }

        public long getReconnectBatchStreamThresholdMs() {
            return reconnectBatchStreamThresholdMs;
        }

        public void setReconnectBatchStreamThresholdMs(long reconnectBatchStreamThresholdMs) {
            this.reconnectBatchStreamThresholdMs = reconnectBatchStreamThresholdMs;
        }

        public long getGlobalSilenceTimeoutMs() {
            return globalSilenceTimeoutMs;
        }

        public void setGlobalSilenceTimeoutMs(long globalSilenceTimeoutMs) {
            this.globalSilenceTimeoutMs = globalSilenceTimeoutMs;
        }

        public int getDepth() {
            return depth;
        }

        public void setDepth(int depth) {
            this.depth = depth;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public long getFundingPollIntervalMs() {
            return fundingPollIntervalMs;
        }

        public void setFundingPollIntervalMs(long fundingPollIntervalMs) {
            this.fundingPollIntervalMs = fundingPollIntervalMs;
        }
    }

    @Override
    public String toString() {
        return "FeedConfig{" +
                "appId='" + appId + '\'' +
                ", enabled=" + enabled +
                ", symbols=" + (symbols != null ? symbols.size() : 0) +
                '}';
    }
}
