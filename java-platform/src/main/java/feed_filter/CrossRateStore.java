package feed_filter;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe store for cross/FX rates.
 * Key: (feedId, symbol) e.g. ("feed_forex", "EURUSDT")
 * Value: bid, ask, mid, timestamp
 */
public class CrossRateStore {

    public static final class Quote {
        public final double bid;
        public final double ask;
        public final double mid;
        public final long tsMs;

        public Quote(double bid, double ask, long tsMs) {
            this.bid = bid;
            this.ask = ask;
            this.mid = (bid > 0 && ask > 0) ? (bid + ask) / 2.0 : 0.0;
            this.tsMs = tsMs;
        }

        public boolean isValid() {
            return bid > 0 && ask > 0 && Double.isFinite(mid);
        }
    }

    private final ConcurrentHashMap<String, Quote> rates = new ConcurrentHashMap<>();

    private static String key(String feedId, String symbol) {
        if (feedId == null || symbol == null) return null;
        return (feedId + ":" + symbol).toUpperCase();
    }

    public void update(String feedId, String symbol, double bid, double ask, long tsMs) {
        String k = key(feedId, symbol);
        if (k != null && bid > 0 && ask > 0) {
            rates.put(k, new Quote(bid, ask, tsMs));
        }
    }

    /**
     * Get mid price for (feedId, symbol). Returns 0 if missing/stale.
     */
    public double getMidPrice(String feedId, String symbol) {
        Quote q = rates.get(key(feedId, symbol));
        return (q != null && q.isValid()) ? q.mid : 0.0;
    }

    /**
     * Get full quote. Returns null if missing.
     */
    public Quote getQuote(String feedId, String symbol) {
        return rates.get(key(feedId, symbol));
    }

    /**
     * Check if rate is available and not too stale.
     * @param maxAgeMs max age in ms (e.g. 60000)
     */
    public boolean isFresh(String feedId, String symbol, long maxAgeMs) {
        Quote q = rates.get(key(feedId, symbol));
        if (q == null || !q.isValid()) return false;
        return (System.currentTimeMillis() - q.tsMs) <= maxAgeMs;
    }
}
